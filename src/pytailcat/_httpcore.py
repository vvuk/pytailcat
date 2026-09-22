"""HTTPcore networking over Tailcat streams, including socket-independent TLS."""

from __future__ import annotations

import ssl
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import anyio
import httpcore

from ._async import AsyncClient, _close, _run
from ._core import Client, Connection
from ._errors import ClosedError, TailcatError, TailcatTimeout
from ._ffi import Operation


@contextmanager
def _map_errors(phase: str) -> Iterator[None]:
    try:
        yield
    except TailcatTimeout as exc:
        cls = {
            "connect": httpcore.ConnectTimeout,
            "read": httpcore.ReadTimeout,
            "write": httpcore.WriteTimeout,
        }[phase]
        raise cls(str(exc)) from exc
    except (TailcatError, ssl.SSLError) as exc:
        cls = {
            "connect": httpcore.ConnectError,
            "read": httpcore.ReadError,
            "write": httpcore.WriteError,
        }[phase]
        raise cls(str(exc)) from exc


class _Stream(httpcore.NetworkStream):
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.ssl_object: ssl.SSLObject | None = None
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()

    def _tls_call(self, op: Operation, fn: Any, *args: Any) -> Any:
        # HTTP/1 serializes operations per connection. Each loop uses the same
        # native operation, so a TLS handshake or write has one overall deadline.
        while True:
            try:
                result = fn(*args)
            except ssl.SSLWantReadError:
                self._flush(op)
                data = self.connection._recv(op, 65536)
                if data:
                    self._incoming.write(data)
                else:
                    self._incoming.write_eof()
            except ssl.SSLWantWriteError:
                self._flush(op)
            else:
                self._flush(op)
                return result

    def _flush(self, op: Operation) -> None:
        if self._outgoing.pending:
            self.connection._sendall(op, self._outgoing.read())

    def _read(self, op: Operation, size: int) -> bytes:
        if self.ssl_object is None:
            return self.connection._recv(op, size)
        try:
            return self._tls_call(op, self.ssl_object.read, size)
        except ssl.SSLZeroReturnError:
            return b""

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        with _map_errors("read"), Operation(timeout) as op:
            return self._read(op, max_bytes)

    def _write(self, op: Operation, buffer: bytes) -> None:
        if self.ssl_object is None:
            self.connection._sendall(op, buffer)
        else:
            offset = 0
            while offset < len(buffer):
                offset += self._tls_call(
                    op, self.ssl_object.write, memoryview(buffer)[offset:]
                )

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        with _map_errors("write"), Operation(timeout) as op:
            self._write(op, buffer)

    def _start_tls(
        self, op: Operation, context: ssl.SSLContext, hostname: str | None
    ) -> _Stream:
        if self.ssl_object is not None:
            raise httpcore.ConnectError("TLS is already active")
        self.ssl_object = context.wrap_bio(
            self._incoming, self._outgoing, server_hostname=hostname
        )
        self._tls_call(op, self.ssl_object.do_handshake)
        return self

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> _Stream:
        try:
            with _map_errors("connect"), Operation(timeout) as op:
                return self._start_tls(op, ssl_context, server_hostname)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        self.connection.close()

    def get_extra_info(self, info: str) -> Any:
        if info == "ssl_object":
            return self.ssl_object
        if info == "client_addr":
            return self.connection.local_address
        if info == "server_addr":
            return self.connection.remote_address
        if info == "is_readable":
            if self.ssl_object is not None and self.ssl_object.pending():
                return True
            try:
                return self.connection._readable()
            except ClosedError:
                return True
        return None


class _AsyncStream(httpcore.AsyncNetworkStream):
    def __init__(self, stream: _Stream) -> None:
        self._sync = stream

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        with _map_errors("read"):
            return await _run(
                lambda op: self._sync._read(op, max_bytes), timeout, kind="read"
            )

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        with _map_errors("write"):
            await _run(lambda op: self._sync._write(op, buffer), timeout, kind="write")

    async def aclose(self) -> None:
        await _close(self._sync)

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> _AsyncStream:
        try:
            with _map_errors("connect"):
                await _run(
                    lambda op: self._sync._start_tls(op, ssl_context, server_hostname),
                    timeout,
                    kind="connect",
                )
            return self
        except BaseException:
            await self.aclose()
            raise

    def get_extra_info(self, info: str) -> Any:
        # The native readability probe never waits for data or a read lock.
        return self._sync.get_extra_info(info)


class _Backend(httpcore.NetworkBackend):
    def __init__(self, client: Client, host: str, port: int) -> None:
        self.client = client
        self.host = host
        self.port = port

    def _check(
        self, host: str, port: int, local_address: Any, socket_options: Any
    ) -> None:
        if host != self.host or port != self.port:
            raise httpcore.ConnectError(
                "Request does not match this Tailcat peer's origin"
            )
        if local_address is not None or socket_options:
            raise httpcore.ConnectError(
                "OS socket options are not supported by Tailcat"
            )

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> _Stream:
        self._check(host, port, local_address, socket_options)
        with _map_errors("connect"):
            return _Stream(self.client.dial_tcp(port, timeout=timeout))

    def connect_unix_socket(
        self, path: str, timeout: float | None = None, socket_options: Any = None
    ) -> httpcore.NetworkStream:
        raise httpcore.UnsupportedProtocol(
            "Tailcat transports require an HTTP or HTTPS origin"
        )

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class _AsyncBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, client: AsyncClient, host: str, port: int) -> None:
        self.client = client
        self._check_backend = _Backend(client._sync, host, port)

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> _AsyncStream:
        self._check_backend._check(host, port, local_address, socket_options)
        with _map_errors("connect"):
            connection = await _run(
                lambda op: self.client._sync._dial(op, port, 1),
                timeout,
                dispose=lambda c: c.close(),
                kind="connect",
            )
        return _AsyncStream(_Stream(connection))

    async def connect_unix_socket(
        self, path: str, timeout: float | None = None, socket_options: Any = None
    ) -> httpcore.AsyncNetworkStream:
        raise httpcore.UnsupportedProtocol(
            "Tailcat transports require an HTTP or HTTPS origin"
        )

    async def sleep(self, seconds: float) -> None:
        await anyio.sleep(seconds)
