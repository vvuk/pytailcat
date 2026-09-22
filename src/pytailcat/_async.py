from __future__ import annotations

from collections.abc import Callable
from typing import Any, Self

import anyio
from anyio.lowlevel import RunVar

from ._core import (
    MAX_UDP_PAYLOAD,
    Client,
    Connection,
    DatagramConnection,
    KeyPair,
    Server,
)
from ._errors import TailcatTimeout
from ._ffi import Operation, encode, native

_limiters: RunVar[dict[str, anyio.CapacityLimiter]] = RunVar("pytailcat_io_limiters")


def _io_limiter(kind: str) -> anyio.CapacityLimiter:
    try:
        limiters = _limiters.get()
    except LookupError:
        limiters = {}
        _limiters.set(limiters)
    if kind not in limiters:
        limiters[kind] = anyio.CapacityLimiter(64)
    return limiters[kind]


async def _run[T](
    fn: Callable[[Operation], T],
    timeout: float | None = None,
    *,
    dispose: Callable[[T], None] | None = None,
    kind: str = "control",
) -> T:
    """Cancel native work and join it before relinquishing buffers or results."""
    result: list[T] = []
    failure: list[BaseException] = []
    done = anyio.Event()
    with Operation(timeout) as op:
        limiter = _io_limiter(kind)
        # Acquire before launching a shielded worker, so a saturated queue still
        # respects timeout/cancellation. Separate directions prevent blocked reads
        # or accepts from starving the writes/dials needed to make progress.
        try:
            with anyio.fail_after(timeout):
                await limiter.acquire()
        except TimeoutError as exc:
            raise TailcatTimeout("Timed out waiting for a Tailcat worker") from exc

        async def work() -> None:
            # The worker must start even if cancellation arrives while it is queued.
            with anyio.CancelScope(shield=True):
                try:
                    result.append(
                        await anyio.to_thread.run_sync(
                            fn, op, limiter=anyio.CapacityLimiter(1)
                        )
                    )
                except BaseException as exc:
                    failure.append(exc)
                finally:
                    done.set()

        try:
            async with anyio.create_task_group() as group:
                group.start_soon(work)
                try:
                    await done.wait()
                except BaseException:
                    op.cancel()  # Nonblocking native cancellation bypasses the I/O limiter.
                    with anyio.CancelScope(shield=True):
                        await done.wait()
                        if result and dispose is not None:
                            await anyio.to_thread.run_sync(
                                dispose, result[0], limiter=anyio.CapacityLimiter(1)
                            )
                    raise
        finally:
            limiter.release()
    if failure:
        raise failure[0]
    return result[0]


async def _close(resource: Any) -> None:
    # Shutdown bypasses the occupied I/O limiter and completes under cancellation.
    with anyio.CancelScope(shield=True):
        await anyio.to_thread.run_sync(resource.close, limiter=anyio.CapacityLimiter(1))


class _AsyncResource:
    def __init__(self, resource: Any) -> None:
        self._sync = resource

    @property
    def closed(self) -> bool:
        return self._sync.closed

    async def aclose(self) -> None:
        await _close(self._sync)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()


class AsyncConnection(_AsyncResource):
    @property
    def local_address(self) -> tuple[str, int]:
        return self._sync.local_address

    @property
    def remote_address(self) -> tuple[str, int]:
        return self._sync.remote_address

    async def recv(self, size: int = 65536, *, timeout: float | None = None) -> bytes:
        return await _run(lambda op: self._sync._recv(op, size), timeout, kind="read")

    async def send(self, data: bytes, *, timeout: float | None = None) -> int:
        data = bytes(data)
        return await _run(lambda op: self._sync._send(op, data), timeout, kind="write")

    async def sendall(self, data: bytes, *, timeout: float | None = None) -> None:
        data = bytes(data)
        await _run(lambda op: self._sync._sendall(op, data), timeout, kind="write")

    async def close_write(self) -> None:
        await _run(
            lambda op: native().invoke(
                "tc_conn_close_write", self._sync._handle, op.handle
            )
        )


class AsyncDatagramConnection(_AsyncResource):
    local_address = AsyncConnection.local_address
    remote_address = AsyncConnection.remote_address

    async def recv(
        self, size: int = MAX_UDP_PAYLOAD, *, timeout: float | None = None
    ) -> bytes:
        return await _run(lambda op: self._sync._recv(op, size), timeout, kind="read")

    async def send(self, data: bytes, *, timeout: float | None = None) -> int:
        data = bytes(data)
        return await _run(lambda op: self._sync._send(op, data), timeout, kind="write")


def _connection(
    resource: Connection | DatagramConnection,
) -> AsyncConnection | AsyncDatagramConnection:
    if isinstance(resource, Connection):
        return AsyncConnection(resource)
    return AsyncDatagramConnection(resource)


class AsyncListener(_AsyncResource):
    @property
    def local_address(self) -> tuple[str, int]:
        return self._sync.local_address

    @property
    def port(self) -> int:
        return self._sync.port

    async def accept(
        self, *, timeout: float | None = None
    ) -> AsyncConnection | AsyncDatagramConnection:
        resource = await _run(
            self._sync._accept, timeout, dispose=lambda r: r.close(), kind="accept"
        )
        return _connection(resource)


class AsyncClient(_AsyncResource):
    def __init__(
        self, address: str, *, key: KeyPair | str | None = None, derp_map_url: str = ""
    ) -> None:
        # Construction allocates configuration and identity only, with no networking.
        super().__init__(Client(address, key=key, derp_map_url=derp_map_url))

    @property
    def public_key(self) -> str:
        return self._sync.public_key

    async def dial_tcp(
        self, port: int, *, timeout: float | None = None
    ) -> AsyncConnection:
        resource = await _run(
            lambda op: self._sync._dial(op, port, 1),
            timeout,
            dispose=lambda r: r.close(),
            kind="connect",
        )
        return AsyncConnection(resource)

    async def dial_udp(
        self, port: int, *, timeout: float | None = None
    ) -> AsyncDatagramConnection:
        resource = await _run(
            lambda op: self._sync._dial(op, port, 2),
            timeout,
            dispose=lambda r: r.close(),
            kind="connect",
        )
        return AsyncDatagramConnection(resource)

    async def ping(
        self, *, disco: bool = False, timeout: float | None = None
    ) -> dict[str, Any]:
        return await _run(lambda op: self._sync._ping(op, disco), timeout)

    async def drain(self, *, timeout: float = 5.0) -> None:
        await _run(self._sync._drain, timeout)


class AsyncServer(_AsyncResource):
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
        super().__init__(
            Server(
                key=key,
                preshared_key=preshared_key,
                region=region,
                region_id=region_id,
                derp_map_url=derp_map_url,
                allowed_clients=allowed_clients,
                udp_idle_timeout=udp_idle_timeout,
            )
        )

    @property
    def address(self) -> str:
        return self._sync.address

    async def start(self, *, timeout: float | None = None) -> None:
        await _run(self._sync._start, timeout)

    async def listen_tcp(
        self, port: int = 0, *, timeout: float | None = None
    ) -> AsyncListener:
        resource = await _run(
            lambda op: self._sync._listen(op, port, 1),
            timeout,
            dispose=lambda r: r.close(),
        )
        return AsyncListener(resource)

    async def listen_udp(
        self, port: int = 0, *, timeout: float | None = None
    ) -> AsyncListener:
        resource = await _run(
            lambda op: self._sync._listen(op, port, 2),
            timeout,
            dispose=lambda r: r.close(),
        )
        return AsyncListener(resource)

    async def allow_client(self, public_key: str) -> None:
        await _run(lambda op: self._sync._allow_client(op, public_key))

    drain = AsyncClient.drain


async def resolve_address(
    address: str, *, derp_map_url: str = "", timeout: float | None = None
) -> str:
    return await _run(
        lambda op: native().text(
            "tc_address_resolve", op.handle, encode(address), encode(derp_map_url)
        ),
        timeout,
    )
