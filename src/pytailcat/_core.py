from __future__ import annotations

import threading
import weakref
from dataclasses import dataclass, field
from typing import Any, Literal, Self, overload

from ._errors import TailcatError
from ._ffi import Operation, config_bytes, encode, native

MAX_UDP_PAYLOAD = 1232


def _port(port: int, *, listen: bool = False) -> int:
    if (
        isinstance(port, bool)
        or not isinstance(port, int)
        or not (0 if listen else 1) <= port <= 65535
    ):
        raise ValueError("port must be an integer in the valid TCP/UDP port range")
    return port


def _endpoint(address: str) -> tuple[str, int]:
    host, port = address.rsplit(":", 1)
    return host.strip("[]"), int(port)


@dataclass(frozen=True)
class KeyPair:
    private_key: str = field(repr=False)
    public_key: str
    preshared_key: str = field(repr=False)

    @classmethod
    def generate(cls) -> KeyPair:
        return cls(**native().json("tc_key_generate"))


def parse_address(address: str) -> dict[str, Any]:
    """Return connection metadata, excluding the address's pre-shared secret."""
    return native().json("tc_address_parse", encode(address))


def resolve_address(
    address: str, *, derp_map_url: str = "", timeout: float | None = None
) -> str:
    with Operation(timeout) as op:
        return native().text(
            "tc_address_resolve", op.handle, encode(address), encode(derp_map_url)
        )


def build_info() -> dict[str, Any]:
    return native().json("tc_build_info")


class _Resource:
    def _init_handle(self, handle: int, parent: _Resource | None = None) -> None:
        self._handle = handle
        self._parent = parent
        self._native = native()
        self._close_lock = threading.Lock()
        self._closed = False
        self._finalizer = weakref.finalize(self, self._native.close, handle)
        # Runtime teardown order is unspecified; explicit context managers are preferred.
        self._finalizer.atexit = False

    @property
    def closed(self) -> bool:
        return self._closed or (self._parent is not None and self._parent.closed)

    def close(self) -> None:
        with self._close_lock:
            if not self._closed:
                try:
                    self._native.close(self._handle)
                finally:
                    self._closed = True
                    self._finalizer.detach()

    def _info(self, op: Operation) -> dict[str, Any]:
        return self._native.json("tc_info", self._handle, op.handle)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class _DataConnection(_Resource):
    local_address: tuple[str, int]
    remote_address: tuple[str, int]

    @classmethod
    def _from_handle(cls, handle: int, parent: _Resource, op: Operation) -> Self:
        self = cls.__new__(cls)
        self._init_handle(handle, parent)
        try:
            info = self._info(op)
            self.local_address = _endpoint(info["local_address"])
            self.remote_address = _endpoint(info["remote_address"])
        except BaseException:
            self.close()
            raise
        return self

    def _recv(self, op: Operation, size: int) -> bytes:
        if isinstance(size, bool) or not isinstance(size, int) or size < 1:
            raise ValueError("size must be a positive integer")
        ffi = self._native.ffi
        buffer = ffi.new("char[]", size)
        count = ffi.new("size_t *")
        try:
            self._native.invoke(
                "tc_conn_read",
                self._handle,
                op.handle,
                buffer,
                size,
                count,
                allow_eof=True,
            )
        except TailcatError as exc:
            exc.partial_data = bytes(ffi.buffer(buffer, count[0]))
            raise
        return bytes(ffi.buffer(buffer, count[0]))

    def _send(self, op: Operation, data: bytes | memoryview) -> int:
        ffi = self._native.ffi
        buffer = ffi.from_buffer(data)
        count = ffi.new("size_t *")
        try:
            self._native.invoke(
                "tc_conn_write", self._handle, op.handle, buffer, len(data), count
            )
        except TailcatError as exc:
            exc.bytes_written = int(count[0])
            raise
        return int(count[0])


class Connection(_DataConnection):
    """A TCP byte stream. Supports one reader and one writer concurrently."""

    def recv(self, size: int = 65536, *, timeout: float | None = None) -> bytes:
        """Read up to size bytes; b'' means TCP EOF."""
        with Operation(timeout) as op:
            return self._recv(op, size)

    def send(self, data: bytes, *, timeout: float | None = None) -> int:
        """Write bytes, returning the count; a short write is possible."""
        with Operation(timeout) as op:
            return self._send(op, bytes(data))

    def _sendall(self, op: Operation, data: bytes) -> None:
        if not data:
            self._send(op, data)
            return
        offset = 0
        view = memoryview(data)
        while offset < len(data):
            try:
                n = self._send(op, view[offset:])
            except TailcatError as exc:
                exc.bytes_written = getattr(exc, "bytes_written", 0) + offset
                raise
            if n == 0:
                raise TailcatError("Connection made no write progress")
            offset += n

    def sendall(self, data: bytes, *, timeout: float | None = None) -> None:
        """Write all bytes using one deadline for the entire operation."""
        with Operation(timeout) as op:
            self._sendall(op, bytes(data))

    def close_write(self) -> None:
        """Send TCP EOF while retaining the ability to receive."""
        with Operation() as op:
            self._native.invoke("tc_conn_close_write", self._handle, op.handle)

    def _readable(self) -> bool:
        if self.closed:
            return True
        out = self._native.ffi.new("int32_t *")
        self._native.invoke("tc_conn_readable", self._handle, out)
        return bool(out[0])


class DatagramConnection(_DataConnection):
    """A connected UDP flow. Each receive/send consumes/produces one datagram."""

    def recv(
        self, size: int = MAX_UDP_PAYLOAD, *, timeout: float | None = None
    ) -> bytes:
        """Receive one packet; an empty result is a valid zero-length datagram."""
        with Operation(timeout) as op:
            return self._recv(op, size)

    def send(self, data: bytes, *, timeout: float | None = None) -> int:
        with Operation(timeout) as op:
            return self._send(op, bytes(data))


class Listener(_Resource):
    local_address: tuple[str, int]
    _network: int

    @classmethod
    def _from_handle(
        cls, handle: int, parent: Server, network: int, op: Operation
    ) -> Listener:
        self = cls.__new__(cls)
        self._init_handle(handle, parent)
        self._network = network
        try:
            self.local_address = _endpoint(self._info(op)["local_address"])
        except BaseException:
            self.close()
            raise
        return self

    @property
    def port(self) -> int:
        return self.local_address[1]

    def _accept(self, op: Operation) -> Connection | DatagramConnection:
        handle = self._native.handle("tc_listener_accept", self._handle, op.handle)
        cls = Connection if self._network == 1 else DatagramConnection
        # Connections survive listener closure, but keep the server alive.
        assert self._parent is not None
        return cls._from_handle(handle, self._parent, op)

    def accept(
        self, *, timeout: float | None = None
    ) -> Connection | DatagramConnection:
        with Operation(timeout) as op:
            return self._accept(op)


class Client(_Resource):
    """A reusable Tailcat peer connection, established lazily on first I/O."""

    def __init__(
        self, address: str, *, key: KeyPair | str | None = None, derp_map_url: str = ""
    ) -> None:
        config = {
            "address": address,
            "key": key.private_key if isinstance(key, KeyPair) else key or "",
            "derp_map_url": derp_map_url,
        }
        self._init_handle(native().handle("tc_client_new", config_bytes(config)))
        with Operation() as op:
            self.public_key: str = self._info(op)["public_key"]

    @overload
    def _dial(self, op: Operation, port: int, network: Literal[1]) -> Connection: ...

    @overload
    def _dial(
        self, op: Operation, port: int, network: Literal[2]
    ) -> DatagramConnection: ...

    def _dial(
        self, op: Operation, port: int, network: int
    ) -> Connection | DatagramConnection:
        handle = self._native.handle(
            "tc_client_dial", self._handle, op.handle, _port(port), network
        )
        cls = Connection if network == 1 else DatagramConnection
        return cls._from_handle(handle, self, op)

    def dial_tcp(self, port: int, *, timeout: float | None = None) -> Connection:
        with Operation(timeout) as op:
            return self._dial(op, port, 1)

    def dial_udp(
        self, port: int, *, timeout: float | None = None
    ) -> DatagramConnection:
        with Operation(timeout) as op:
            return self._dial(op, port, 2)

    def _ping(self, op: Operation, disco: bool) -> dict[str, Any]:
        return self._native.json("tc_client_ping", self._handle, op.handle, int(disco))

    def ping(
        self, *, disco: bool = False, timeout: float | None = None
    ) -> dict[str, Any]:
        with Operation(timeout) as op:
            return self._ping(op, disco)

    def _drain(self, op: Operation) -> None:
        self._native.invoke("tc_drain", self._handle, op.handle)

    def drain(self, *, timeout: float = 5.0) -> None:
        with Operation(timeout) as op:
            self._drain(op)


class Server(_Resource):
    """Accept Tailcat connections on explicitly opened TCP or UDP ports."""

    def __init__(
        self,
        *,
        key: KeyPair | str | None = None,
        preshared_key: str | None = None,
        region: dict[str, Any] | None = None,
        region_id: int = 0,
        derp_map_url: str = "",
        allowed_clients: list[str] | None = None,
        udp_idle_timeout: float = 0,
    ) -> None:
        config = {
            "key": key.private_key if isinstance(key, KeyPair) else key or "",
            "preshared_key": preshared_key
            if preshared_key is not None
            else key.preshared_key
            if isinstance(key, KeyPair)
            else "",
            "region": region,
            "region_id": region_id,
            "derp_map_url": derp_map_url,
            "allowed_clients": allowed_clients or [],
            "udp_idle_timeout": udp_idle_timeout,
        }
        self._init_handle(native().handle("tc_server_new", config_bytes(config)))
        self._address: str | None = None

    @property
    def address(self) -> str:
        """The secret peer address, available after start() or listen_*()."""
        if self._address is None:
            raise RuntimeError(
                "Start the server or create a listener before reading its address"
            )
        return self._address

    def _start(self, op: Operation) -> None:
        self._native.invoke("tc_server_start", self._handle, op.handle)
        self._address = self._info(op)["address"]

    def start(self, *, timeout: float | None = None) -> None:
        with Operation(timeout) as op:
            self._start(op)

    def _listen(self, op: Operation, port: int, network: int) -> Listener:
        handle = self._native.handle(
            "tc_server_listen",
            self._handle,
            op.handle,
            _port(port, listen=True),
            network,
        )
        try:
            self._address = self._info(op)["address"]
        except BaseException:
            self._native.close(handle)
            raise
        return Listener._from_handle(handle, self, network, op)

    def listen_tcp(self, port: int = 0, *, timeout: float | None = None) -> Listener:
        with Operation(timeout) as op:
            return self._listen(op, port, 1)

    def listen_udp(self, port: int = 0, *, timeout: float | None = None) -> Listener:
        with Operation(timeout) as op:
            return self._listen(op, port, 2)

    def _allow_client(self, op: Operation, public_key: str) -> None:
        self._native.invoke(
            "tc_server_allow_client", self._handle, op.handle, encode(public_key)
        )

    def allow_client(self, public_key: str) -> None:
        """Add an allowed key; once a list exists, other clients cannot connect."""
        with Operation() as op:
            self._allow_client(op, public_key)

    _drain = Client._drain
    drain = Client.drain
