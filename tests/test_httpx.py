import ssl
import time

import anyio
import httpx
import pytest

import pytailcat as tc
from pytailcat.httpx import AsyncTailcatTransport, TailcatTransport


def transport(relay, tls=False, **kwargs):
    origin = f"{'https' if tls else 'http'}://service.internal:{relay['https_port' if tls else 'http_port']}"
    if tls and "verify" not in kwargs:
        context = ssl.create_default_context()
        relay["ca"].configure_trust(context)
        kwargs["verify"] = context
    return origin, TailcatTransport(address=relay["address"], origin=origin, **kwargs)


@pytest.mark.parametrize("tls", [False, True])
def test_sync_requests_streaming_and_pool_reuse(relay, tls):
    origin, t = transport(relay, tls)
    with httpx.Client(transport=t, base_url=origin) as client:
        first = client.get("/hello?q=one%20two")
        second = client.post("/upload", content=iter([b"hello", b" world"]))
        assert first.json()["path"] == "/hello?q=one%20two"
        assert first.json()["host"] == origin.split("://")[1]
        assert first.headers.get_list("x-repeated") == ["one", "two"]
        assert second.json()["body"] == "hello world"
        assert first.headers["x-connection-id"] == second.headers["x-connection-id"]
        with client.stream("GET", "/stream") as response:
            assert b"".join(response.iter_bytes()) == b"".join(
                f"chunk-{i}\n".encode() for i in range(8)
            )
        with client.stream("GET", "/stream") as response:
            assert next(response.iter_bytes())
        assert client.get("/after-early-close").status_code == 200


def test_origin_mismatch_and_redirect_are_rejected(relay):
    origin, t = transport(relay)
    with httpx.Client(transport=t, base_url=origin, follow_redirects=True) as client:
        with pytest.raises(httpx.UnsupportedProtocol):
            client.get("http://another.internal/")
        with pytest.raises(httpx.UnsupportedProtocol):
            client.get("/redirect", params={"to": "http://another.internal/"})
        assert (
            client.get("/redirect", params={"to": "/final"}).json()["path"] == "/final"
        )


def test_tls_verification_and_read_timeout_use_httpx_errors(relay):
    origin, t = transport(relay, True, verify=True)
    with httpx.Client(transport=t, base_url=origin) as client:
        with pytest.raises(httpx.ConnectError):
            client.get("/")
    origin, t = transport(relay)
    with httpx.Client(transport=t, base_url=origin) as client:
        with pytest.raises(httpx.ReadTimeout) as error:
            client.get("/slow", timeout=httpx.Timeout(10, read=0.02))
        assert error.value.request.url.path == "/slow"
        assert client.get("/after-timeout").status_code == 200


def test_transport_does_not_close_borrowed_client(relay):
    origin = f"http://service.internal:{relay['http_port']}"
    with tc.Client(relay["address"]) as peer:
        with httpx.Client(
            transport=TailcatTransport(client=peer, origin=origin), base_url=origin
        ) as client:
            assert client.get("/").status_code == 200
        assert not peer.closed
        assert peer.ping(timeout=5)["latency"] >= 0


def test_pool_timeout_and_idle_connection_closure(relay):
    origin, t = transport(relay, limits=httpx.Limits(max_connections=1))
    with httpx.Client(transport=t, base_url=origin) as client:
        with client.stream("GET", "/stream"):
            with pytest.raises(httpx.PoolTimeout):
                client.get("/", timeout=httpx.Timeout(10, pool=0.02))
        client.get("/close")
        assert client.get("/").status_code == 200
        assert client.get("/idle-close").text == "ok"
        time.sleep(0.05)
        assert client.get("/after-idle-close").status_code == 200


def test_tls_hostname_must_match_the_http_origin(relay):
    origin = f"https://wrong.internal:{relay['https_port']}"
    context = ssl.create_default_context()
    relay["ca"].configure_trust(context)
    t = TailcatTransport(address=relay["address"], origin=origin, verify=context)
    with httpx.Client(transport=t, base_url=origin) as client:
        with pytest.raises(httpx.ConnectError):
            client.get("/")


def test_connect_timeout_includes_tailcat_handshake(relay):
    allowed = tc.KeyPair.generate()
    with tc.Server(
        region=relay["region"], allowed_clients=[allowed.public_key]
    ) as server:
        with server.listen_tcp(timeout=10) as listener:
            origin = f"http://service.internal:{listener.port}"
            t = TailcatTransport(address=server.address, origin=origin)
            with httpx.Client(transport=t, base_url=origin) as client:
                with pytest.raises(httpx.ConnectTimeout):
                    client.get("/", timeout=0.02)


@pytest.mark.anyio
@pytest.mark.parametrize("tls", [False, True])
async def test_async_requests_streaming_and_pool_reuse(relay, tls):
    scheme = "https" if tls else "http"
    origin = (
        f"{scheme}://service.internal:{relay['https_port' if tls else 'http_port']}"
    )
    context = ssl.create_default_context()
    relay["ca"].configure_trust(context)
    t = AsyncTailcatTransport(address=relay["address"], origin=origin, verify=context)

    async def upload():
        yield b"hello"
        await anyio.sleep(0)
        yield b" world"

    async with httpx.AsyncClient(transport=t, base_url=origin) as client:
        first = await client.get("/")
        second = await client.post("/upload", content=upload())
        assert second.json()["body"] == "hello world"
        assert first.headers["x-connection-id"] == second.headers["x-connection-id"]
        async with client.stream("GET", "/stream") as response:
            assert b"".join([chunk async for chunk in response.aiter_bytes()]).endswith(
                b"chunk-7\n"
            )


@pytest.mark.anyio
async def test_async_cancelled_request_does_not_block_loop_or_poison_pool(relay):
    origin = f"http://service.internal:{relay['http_port']}"
    t = AsyncTailcatTransport(address=relay["address"], origin=origin)
    async with httpx.AsyncClient(transport=t, base_url=origin) as client:
        await client.get("/")
        with anyio.move_on_after(0.02) as scope:
            await client.get("/slow", timeout=None)
        assert scope.cancel_called
        with anyio.fail_after(5):
            assert (await client.get("/after-cancel")).status_code == 200


@pytest.mark.anyio
async def test_async_pool_timeout_early_close_and_borrowed_peer(relay):
    origin = f"http://service.internal:{relay['http_port']}"
    async with tc.AsyncClient(relay["address"]) as peer:
        t = AsyncTailcatTransport(
            client=peer, origin=origin, limits=httpx.Limits(max_connections=1)
        )
        async with httpx.AsyncClient(transport=t, base_url=origin) as client:
            async with client.stream("GET", "/stream") as response:
                with pytest.raises(httpx.PoolTimeout):
                    await client.get("/", timeout=httpx.Timeout(10, pool=0.02))
                async for chunk in response.aiter_bytes():
                    assert chunk
                    break
            assert (await client.get("/after-close")).status_code == 200
            with pytest.raises(httpx.ReadTimeout):
                await client.get("/slow", timeout=httpx.Timeout(10, read=0.02))
        assert not peer.closed
        assert (await peer.ping(timeout=5))["latency"] >= 0
