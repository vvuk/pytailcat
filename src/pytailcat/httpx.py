"""HTTP/1.1 and HTTP/2 transports for HTTPX (install pytailcat[httpx])."""

from __future__ import annotations

import ssl
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Any

import anyio
import certifi
import httpcore
import httpx

from ._async import AsyncClient
from ._core import Client
from ._httpcore import _AsyncBackend, _Backend

__all__ = ["TailcatTransport", "AsyncTailcatTransport"]


@contextmanager
def _httpx_errors(request: httpx.Request | None = None) -> Iterator[None]:
    try:
        yield
    except Exception as exc:
        # Walk the exception's MRO so concrete timeout/network/protocol categories
        # survive translation. This uses only public HTTPX/HTTPcore classes.
        for cls in type(exc).__mro__:
            if cls.__module__.startswith("httpcore"):
                mapped = getattr(httpx, cls.__name__, None)
                if isinstance(mapped, type) and issubclass(mapped, httpx.RequestError):
                    raise mapped(str(exc), request=request) from exc
        raise


def _origin(url: httpx.URL) -> tuple[str, str, int]:
    if url.scheme not in {"http", "https"} or not url.host:
        raise ValueError("origin must be an absolute http:// or https:// URL")
    return (
        url.scheme,
        url.raw_host.decode("ascii"),
        url.port or (443 if url.scheme == "https" else 80),
    )


def _settings(
    origin: str,
    verify: bool | ssl.SSLContext,
    limits: httpx.Limits | None,
    http1: bool,
    http2: bool,
) -> tuple[tuple[str, str, int], dict[str, Any]]:
    if not http1 and not http2:
        raise ValueError("At least one of http1 or http2 must be enabled")
    if http2:
        try:
            import h2  # noqa: F401
        except ImportError as exc:
            raise ImportError("HTTP/2 requires h2; install pytailcat[httpx]") from exc
    url = httpx.URL(origin)
    key = _origin(url)
    if url.raw_path != b"/" or url.fragment or url.userinfo:
        raise ValueError(
            "origin must contain only a scheme, hostname, and optional port"
        )
    if isinstance(verify, ssl.SSLContext):
        context = verify
    elif verify is True:
        context = ssl.create_default_context(cafile=certifi.where())
    elif verify is False:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    else:
        raise TypeError("verify must be a bool or SSLContext")
    limits = limits if limits is not None else httpx.Limits()
    return key, {
        "ssl_context": context,
        "max_connections": limits.max_connections,
        "max_keepalive_connections": limits.max_keepalive_connections,
        "keepalive_expiry": limits.keepalive_expiry,
        "http1": http1,
        "http2": http2,
    }


def _request(request: httpx.Request, origin: tuple[str, str, int]) -> httpcore.Request:
    try:
        matches = _origin(request.url) == origin
    except ValueError:
        matches = False
    if not matches:
        raise httpx.UnsupportedProtocol(
            "URL does not match this Tailcat transport's configured origin",
            request=request,
        )
    return httpcore.Request(
        method=request.method,
        url=httpcore.URL(
            scheme=request.url.raw_scheme,
            host=request.url.raw_host,
            port=request.url.port,
            target=request.url.raw_path,
        ),
        headers=request.headers.raw,
        content=request.stream,
        extensions=request.extensions,
    )


class _ResponseStream(httpx.SyncByteStream):
    def __init__(self, stream: Any, request: httpx.Request) -> None:
        self.stream = stream
        self.request = request

    def __iter__(self) -> Iterator[bytes]:
        with _httpx_errors(self.request):
            yield from self.stream

    def close(self) -> None:
        with _httpx_errors(self.request):
            self.stream.close()


class _AsyncResponseStream(httpx.AsyncByteStream):
    def __init__(self, stream: Any, request: httpx.Request) -> None:
        self.stream = stream
        self.request = request

    async def __aiter__(self) -> AsyncIterator[bytes]:
        with _httpx_errors(self.request):
            async for chunk in self.stream:
                yield chunk

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True), _httpx_errors(self.request):
            await self.stream.aclose()


class TailcatTransport(httpx.BaseTransport):
    """Route one HTTP origin to a Tailcat peer, preserving Host and TLS SNI.

    Supply either address (the transport owns a Client) or client (borrowed).
    Configure TLS, pool limits, and http2=True here, rather than on httpx.Client.
    HTTP/2 negotiates over HTTPS, falling back to HTTP/1.1. Set http1=False
    together with http2=True for prior-knowledge HTTP/2 (including cleartext).
    """

    def __init__(
        self,
        *,
        origin: str,
        address: str | None = None,
        client: Client | None = None,
        verify: bool | ssl.SSLContext = True,
        limits: httpx.Limits | None = None,
        retries: int = 0,
        http1: bool = True,
        http2: bool = False,
    ) -> None:
        self._origin, settings = _settings(origin, verify, limits, http1, http2)
        if (address is None) == (client is None):
            raise ValueError("Supply exactly one of address or client")
        if retries < 0:
            raise ValueError("retries must be non-negative")
        self._owned = client is None
        if client is None:
            assert address is not None
            client = Client(address)
        self._client = client
        self._closed = False
        self._pool = httpcore.ConnectionPool(
            **settings,
            retries=retries,
            network_backend=_Backend(self._client, self._origin[1], self._origin[2]),
        )

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if self._closed:
            raise RuntimeError("Tailcat transport is closed")
        if not isinstance(request.stream, httpx.SyncByteStream):
            raise TypeError(
                "A synchronous transport requires a synchronous request stream"
            )
        with _httpx_errors(request):
            response = self._pool.handle_request(_request(request, self._origin))
        return httpx.Response(
            response.status,
            headers=response.headers,
            stream=_ResponseStream(response.stream, request),
            extensions=response.extensions,
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                with _httpx_errors():
                    self._pool.close()
            finally:
                if self._owned:
                    self._client.close()


class AsyncTailcatTransport(httpx.AsyncBaseTransport):
    """Asyncio/Trio counterpart of TailcatTransport, with cancellable native I/O."""

    def __init__(
        self,
        *,
        origin: str,
        address: str | None = None,
        client: AsyncClient | None = None,
        verify: bool | ssl.SSLContext = True,
        limits: httpx.Limits | None = None,
        retries: int = 0,
        http1: bool = True,
        http2: bool = False,
    ) -> None:
        self._origin, settings = _settings(origin, verify, limits, http1, http2)
        if (address is None) == (client is None):
            raise ValueError("Supply exactly one of address or client")
        if retries < 0:
            raise ValueError("retries must be non-negative")
        self._owned = client is None
        if client is None:
            assert address is not None
            client = AsyncClient(address)
        self._client = client
        self._closed = False
        self._pool = httpcore.AsyncConnectionPool(
            **settings,
            retries=retries,
            network_backend=_AsyncBackend(
                self._client, self._origin[1], self._origin[2]
            ),
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self._closed:
            raise RuntimeError("Tailcat transport is closed")
        if not isinstance(request.stream, httpx.AsyncByteStream):
            raise TypeError(
                "An asynchronous transport requires an asynchronous request stream"
            )
        with _httpx_errors(request):
            response = await self._pool.handle_async_request(
                _request(request, self._origin)
            )
        return httpx.Response(
            response.status,
            headers=response.headers,
            stream=_AsyncResponseStream(response.stream, request),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True):
            if not self._closed:
                self._closed = True
                try:
                    with _httpx_errors():
                        await self._pool.aclose()
                finally:
                    if self._owned:
                        await self._client.aclose()
