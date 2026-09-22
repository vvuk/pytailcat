import ssl
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import anyio
import httpx
import pytest

from pytailcat.httpx import AsyncTailcatTransport, TailcatTransport


def transport(peer, *, tls=True, asynchronous=False, **kwargs):
    scheme = "https" if tls else "http"
    origin = f"{scheme}://service.internal:{peer[f'{scheme}_port']}"
    context = ssl.create_default_context()
    peer["ca"].configure_trust(context)
    cls = AsyncTailcatTransport if asynchronous else TailcatTransport
    kwargs.setdefault("http2", True)
    kwargs.setdefault("http1", tls)
    return origin, cls(
        address=peer["address"],
        origin=origin,
        verify=context,
        limits=httpx.Limits(max_connections=1),
        **kwargs,
    )


def check_response(response, *, tls, connection=None):
    assert response.status_code == 200
    assert response.http_version == "HTTP/2"
    assert response.headers["x-http-protocol"] == "HTTP/2.0"
    assert response.headers["x-request-host"] == response.request.url.netloc.decode()
    if tls:
        assert response.headers["x-tls-alpn"] == "h2"
        assert response.headers["x-tls-sni"] == "service.internal"
    if connection is not None:
        assert response.headers["x-connection-id"] == connection
    return response.headers["x-connection-id"]


@pytest.mark.parametrize("tls", [False, True])
def test_sync_http2_multiplexes_uploads_and_streaming(http2_peer, tls):
    origin, t = transport(http2_peer, tls=tls)
    payload = b"flow-controlled upload\n" * 100_000
    with httpx.Client(transport=t, base_url=origin, timeout=15) as client:
        # Use a finite response here. The optional HTTPcore reproducer covers
        # the upstream synchronous reader-starvation case with a silent stream.
        with client.stream("GET", "/stream") as waiting:
            connection = check_response(waiting, tls=tls)
            started = threading.Event()

            def read_waiting():
                started.set()
                return waiting.read()

            def upload(index):
                body = payload + str(index).encode()
                response = client.post(
                    "/echo",
                    content=(body[n : n + 65536] for n in range(0, len(body), 65536)),
                )
                check_response(response, tls=tls, connection=connection)
                assert response.content == body

            with ThreadPoolExecutor(max_workers=5) as workers:
                blocked = workers.submit(read_waiting)
                assert started.wait(5)
                uploads = [workers.submit(upload, index) for index in range(4)]
                for future in uploads:
                    future.result(timeout=30)
                assert blocked.result(timeout=15) == bytes(16 * 16384)
        with client.stream("GET", "/stream") as response:
            check_response(response, tls=tls, connection=connection)
            assert sum(map(len, response.iter_bytes())) == 16 * 16384


@pytest.mark.anyio
@pytest.mark.parametrize("tls", [False, True])
async def test_async_http2_multiplexes_uploads_and_streaming(http2_peer, tls):
    origin, t = transport(http2_peer, tls=tls, asynchronous=True)
    token = uuid4().hex
    payload = b"async flow-controlled upload\n" * 80_000
    async with httpx.AsyncClient(transport=t, base_url=origin, timeout=15) as client:
        async with client.stream("GET", f"/wait?token={token}") as waiting:
            connection = check_response(waiting, tls=tls)
            results = []

            async def read_waiting():
                results.append(await waiting.aread())

            async def upload(index):
                body = payload + str(index).encode()

                async def chunks():
                    for n in range(0, len(body), 65536):
                        yield body[n : n + 65536]
                        await anyio.sleep(0)

                response = await client.post("/echo", content=chunks())
                check_response(response, tls=tls, connection=connection)
                assert response.content == body

            with anyio.fail_after(40):
                async with anyio.create_task_group() as readers:
                    readers.start_soon(read_waiting)
                    async with anyio.create_task_group() as uploads:
                        for index in range(4):
                            uploads.start_soon(upload, index)
                    await client.get(f"/release?token={token}")
            assert results == [b"released"]
        async with client.stream("GET", "/stream") as response:
            check_response(response, tls=tls, connection=connection)
            assert (
                sum([len(chunk) async for chunk in response.aiter_bytes()])
                == 16 * 16384
            )


@pytest.mark.anyio
@pytest.mark.parametrize("tls", [False, True])
async def test_cancelling_http2_response_read_preserves_other_streams(http2_peer, tls):
    origin, t = transport(http2_peer, tls=tls, asynchronous=True)
    tokens = [uuid4().hex, uuid4().hex]
    async with httpx.AsyncClient(transport=t, base_url=origin, timeout=10) as client:
        async with client.stream("GET", f"/wait?token={tokens[0]}") as cancelled:
            connection = check_response(cancelled, tls=tls)
            async with client.stream("GET", f"/wait?token={tokens[1]}") as survivor:
                with anyio.move_on_after(0.05) as scope:
                    await cancelled.aread()
                assert scope.cancel_called
                # The cancelled reader must release HTTPcore's connection-wide
                # read lock, and leave the encrypted stream synchronized.
                response = await client.post("/echo", content=b"still usable")
                check_response(response, tls=tls, connection=connection)
                assert response.content == b"still usable"
                for token in tokens:
                    await client.get(f"/release?token={token}")
                assert await survivor.aread() == b"released"
                check_response(survivor, tls=tls, connection=connection)


@pytest.mark.parametrize("tls", [False, True])
def test_reset_and_early_response_close_leave_http2_connection_usable(http2_peer, tls):
    origin, t = transport(http2_peer, tls=tls)
    with httpx.Client(transport=t, base_url=origin) as client:
        connection = check_response(client.get("/"), tls=tls)
        with pytest.raises(httpx.RemoteProtocolError):
            client.get("/reset")
        with client.stream("GET", "/stream") as response:
            assert next(response.iter_bytes())
        check_response(client.get("/"), tls=tls, connection=connection)


def test_http2_read_timeout_maps_to_httpx_and_pool_recovers(http2_peer):
    origin, t = transport(http2_peer)
    with httpx.Client(transport=t, base_url=origin) as client:
        before = check_response(client.get("/"), tls=True)
        with pytest.raises(httpx.ReadTimeout) as error:
            client.get("/slow", timeout=httpx.Timeout(10, read=0.05))
        assert error.value.request.url.path == "/slow"
        after = check_response(client.get("/"), tls=True)
        assert before != after


@pytest.mark.parametrize(
    "peer_name,kwargs",
    [
        ("relay", {}),  # HTTP/1.1-only TLS server: ALPN fallback.
        ("http2_peer", {"http2": False}),  # Opt-in only.
        ("http2_peer", {"tls": False, "http1": True}),  # No h2c Upgrade.
    ],
)
def test_http1_fallback_and_default_are_preserved(request, peer_name, kwargs):
    peer = request.getfixturevalue(peer_name)
    origin, t = transport(peer, **kwargs)
    with httpx.Client(transport=t, base_url=origin) as client:
        assert client.get("/").http_version == "HTTP/1.1"


@pytest.mark.anyio
async def test_async_http2_falls_back_to_http1(relay):
    origin, t = transport(relay, asynchronous=True)
    async with httpx.AsyncClient(transport=t, base_url=origin) as client:
        assert (await client.get("/")).http_version == "HTTP/1.1"


@pytest.mark.parametrize("cls", [TailcatTransport, AsyncTailcatTransport])
def test_http2_configuration_is_validated_before_creating_peer(cls, monkeypatch):
    with pytest.raises(ValueError, match="At least one"):
        cls(
            origin="https://service.internal",
            address="invalid",
            http1=False,
            http2=False,
        )
    monkeypatch.setitem(sys.modules, "h2", None)
    with pytest.raises(ImportError, match=r"install pytailcat\[httpx\]"):
        cls(origin="https://service.internal", address="invalid", http2=True)
