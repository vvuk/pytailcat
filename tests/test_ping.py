import pytest

import pytailcat as tc


def assert_ping_result(method, result):
    if method == "ping":
        assert type(result) is int
        assert 0 <= result < 10_000
    else:
        assert set(result) == {
            "latency",
            "endpoint",
            "derp_region_id",
            "derp_region_code",
        }
        assert 0 <= result["latency"] < 10
        assert result["endpoint"] or result["derp_region_id"]


@pytest.mark.parametrize("method", ["ping", "disco_ping"])
def test_ping_timeout_preserves_client_and_returns_typed_result(relay, method):
    with tc.Client(relay["address"]) as client:
        ping = getattr(client, method)
        with pytest.raises(tc.TailcatTimeout):
            ping(timeout=0)
        assert_ping_result(method, ping(timeout=10))
    with pytest.raises(tc.ClosedError):
        ping(timeout=10)


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["ping", "disco_ping"])
async def test_async_ping_timeout_preserves_client_and_returns_typed_result(
    relay, method
):
    async with tc.AsyncClient(relay["address"]) as client:
        ping = getattr(client, method)
        with pytest.raises(tc.TailcatTimeout):
            await ping(timeout=0)
        assert_ping_result(method, await ping(timeout=10))
    with pytest.raises(tc.ClosedError):
        await ping(timeout=10)
