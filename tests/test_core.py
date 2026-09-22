import concurrent.futures

import anyio
import pytest

import pytailcat as tc
from pytailcat._ffi import Operation, native


def test_identity_can_be_reused_without_exposing_secrets():
    key = tc.KeyPair.generate()
    assert key.private_key not in repr(key)
    assert key.preshared_key not in repr(key)
    assert tc.build_info()["abi_version"] == 1
    with tc.Server(key=key):
        pass


def test_invalid_address_is_rejected_without_echoing_it():
    with pytest.raises(tc.InvalidArgumentError) as error:
        tc.Client("secret-invalid-address")
    assert "secret-invalid-address" not in str(error.value)


def test_tcp_roundtrip_half_close_and_listener_lifetime(relay):
    with (
        tc.Server(region=relay["region"]) as server,
        tc.Client(relay["address"]) as unused,
    ):
        with (
            server.listen_tcp(timeout=10) as listener,
            tc.Client(server.address) as client,
        ):
            with concurrent.futures.ThreadPoolExecutor() as executor:
                accepted = executor.submit(listener.accept, timeout=10)
                with (
                    client.dial_tcp(listener.port, timeout=10) as outgoing,
                    accepted.result(timeout=10) as incoming,
                ):
                    listener.close()
                    outgoing.sendall(b"hello", timeout=5)
                    outgoing.close_write()
                    assert incoming.recv(timeout=5) == b"hello"
                    assert incoming.recv(timeout=5) == b""
                    incoming.sendall(b"reply", timeout=5)
                    assert outgoing.recv(timeout=5) == b"reply"
                    client.close()
                    with pytest.raises(tc.ClosedError):
                        outgoing.recv(timeout=1)
    assert unused.closed


def test_accept_timeout_preserves_listener(relay):
    with tc.Server(region=relay["region"]) as server:
        with server.listen_tcp(timeout=10) as listener:
            with pytest.raises(tc.TailcatTimeout):
                listener.accept(timeout=0.01)
            with tc.Client(server.address) as client:
                with (
                    client.dial_tcp(listener.port, timeout=10) as outgoing,
                    listener.accept(timeout=5) as incoming,
                ):
                    with pytest.raises(tc.TailcatTimeout):
                        incoming.recv(timeout=0.01)
                    outgoing.sendall(b"still works", timeout=5)
                    assert incoming.recv(timeout=5) == b"still works"


def test_datagrams_preserve_boundaries_and_allow_empty_packets(relay):
    with tc.Server(region=relay["region"]) as server:
        with (
            server.listen_udp(timeout=10) as listener,
            tc.Client(server.address) as client,
        ):
            with client.dial_udp(listener.port, timeout=10) as outgoing:
                outgoing.send(b"first", timeout=5)
                with listener.accept(timeout=5) as incoming:
                    assert incoming.recv(timeout=5) == b"first"
                    outgoing.send(b"", timeout=5)
                    outgoing.send(b"third", timeout=5)
                    assert incoming.recv(timeout=5) == b""
                    assert incoming.recv(timeout=5) == b"third"
                    with pytest.raises(tc.InvalidArgumentError):
                        outgoing.send(b"x" * (tc.MAX_UDP_PAYLOAD + 1))


def test_address_parse_and_resolve_preserve_identity(relay):
    info = tc.parse_address(relay["address"])
    assert info["has_preshared_key"]
    assert "preshared_key" not in info
    assert tc.resolve_address(relay["address"], timeout=1) == relay["address"]


def test_c_abi_rejects_wrong_type_and_stale_handles():
    lib = native()
    with Operation() as op:
        with pytest.raises(tc.InvalidArgumentError):
            lib.handle("tc_client_dial", op.handle, op.handle, 80, 1)
    with pytest.raises(tc.ClosedError):
        lib.invoke("tc_operation_cancel", op.handle)


@pytest.mark.anyio
async def test_async_accept_and_read_cancellation_release_native_work(relay):
    async with tc.AsyncServer(region=relay["region"]) as server:
        async with await server.listen_tcp(timeout=10) as listener:
            with anyio.move_on_after(0.02) as scope:
                await listener.accept()
            assert scope.cancel_called
            async with tc.AsyncClient(server.address) as client:
                async with await client.dial_tcp(listener.port, timeout=10) as outgoing:
                    async with await listener.accept(timeout=5) as incoming:
                        with anyio.move_on_after(0.02) as scope:
                            await incoming.recv()
                        assert scope.cancel_called
                        await outgoing.sendall(b"after cancellation", timeout=5)
                        assert await incoming.recv(timeout=5) == b"after cancellation"


@pytest.mark.anyio
async def test_async_close_interrupts_pending_accept(relay):
    async with tc.AsyncServer(region=relay["region"]) as server:
        listener = await server.listen_tcp(timeout=10)
        started = anyio.Event()

        async def accept():
            started.set()
            with pytest.raises(tc.ClosedError):
                await listener.accept()

        with anyio.fail_after(5):
            async with anyio.create_task_group() as group:
                group.start_soon(accept)
                await started.wait()
                await listener.aclose()
