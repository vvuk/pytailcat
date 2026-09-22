"""HTTPcore networking over Tailcat streams, including socket-independent TLS."""

from __future__ import annotations

import ssl
import threading
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
        self._state_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._receive_lock = threading.Lock()
        self._receive_generation = 0
        self._read_backlog = b""

    @contextmanager
    def _locked(self, op: Operation, lock: Any) -> Iterator[None]:
        # A different request may own this direction. Cancelling this request
        # must not wait indefinitely for that request's native I/O to finish.
        while True:
            op._check()
            if self.connection.closed:
                raise ClosedError("Tailcat connection is closed")
            if lock.acquire(timeout=0.01):
                break
        try:
            op._check()
            yield
        finally:
            lock.release()

    def _tls_call(self, op: Operation, fn: Any, *args: Any) -> Any:
        # SSLObject and both BIOs share a lock, but no blocking I/O holds it.
        # HTTP/2 needs a writer to progress while another request is reading.
        while True:
            op._check()
            result = None
            want_read = want_write = False
            with self._state_lock:
                generation = self._receive_generation
                try:
                    result = fn(*args)
                except ssl.SSLWantReadError:
                    want_read = True
                except ssl.SSLWantWriteError:
                    want_write = True
            self._flush(op)
            if want_read:
                with self._locked(op, self._receive_lock):
                    with self._state_lock:
                        # Another TLS operation may already have fed the BIO.
                        if generation != self._receive_generation:
                            continue
                    data = self.connection._recv(op, 65536)
                    with self._state_lock:
                        if data:
                            self._incoming.write(data)
                        else:
                            self._incoming.write_eof()
                        self._receive_generation += 1
            elif not want_write:
                return result

    def _flush(self, op: Operation) -> None:
        with self._state_lock:
            if not self._outgoing.pending:
                return
        try:
            # Acquire before draining the BIO, preserving TLS record order even
            # when reads generate control records concurrently with writes.
            with self._locked(op, self._send_lock):
                with self._state_lock:
                    data = self._outgoing.read()
                if data:
                    self.connection._sendall(op, data)
        except BaseException:
            # A partially sent record cannot be replayed or safely skipped.
            self.close()
            raise

    def _unread(self, data: bytes) -> None:
        with self._state_lock:
            self._read_backlog = data + self._read_backlog

    def _read(self, op: Operation, size: int) -> bytes:
        with self._state_lock:
            if self._read_backlog:
                data, self._read_backlog = (
                    self._read_backlog[:size],
                    self._read_backlog[size:],
                )
                return data
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
        try:
            self._write_all(op, buffer)
        except BaseException:
            # HTTPcore has already removed these frames from its send queue.
            # Even cleartext partial writes must invalidate the connection.
            self.close()
            raise

    def _write_all(self, op: Operation, buffer: bytes) -> None:
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
            with self._state_lock:
                if self._read_backlog or (
                    self.ssl_object is not None and self.ssl_object.pending()
                ):
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
                lambda op: self._sync._read(op, max_bytes),
                timeout,
                # Cancellation can race with a successful read. HTTP/2 frames
                # may belong to other requests, so never discard those bytes.
                dispose=self._sync._unread,
                kind="read",
            )

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        with _map_errors("write"):
            await _run(
                lambda op: self._sync._write(op, buffer),
                timeout,
                dispose=lambda _: self._sync.close(),
                kind="write",
            )

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
