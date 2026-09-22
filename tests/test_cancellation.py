import threading

import anyio
import pytest

import pytailcat as tc
from pytailcat._async import _io_limiter, _run
from pytailcat._ffi import native


@pytest.mark.anyio
async def test_cancelled_dial_result_is_disposed_even_when_native_call_succeeds():
    entered = threading.Event()
    release = threading.Event()
    disposed = []
    scope_ready = anyio.Event()
    scopes = []

    def delayed_result(op):
        entered.set()
        assert release.wait(5)
        return "new connection"

    async def operation():
        with anyio.CancelScope() as scope:
            scopes.append(scope)
            scope_ready.set()
            await _run(delayed_result, dispose=disposed.append)

    with anyio.fail_after(5):
        async with anyio.create_task_group() as group:
            group.start_soon(operation)
            await scope_ready.wait()
            await anyio.to_thread.run_sync(entered.wait)
            scopes[0].cancel()
            release.set()
    assert disposed == ["new connection"]


@pytest.mark.anyio
async def test_saturated_worker_queue_can_timeout_or_cancel_without_running_work():
    limiter = _io_limiter("queue-test")
    limiter.total_tokens = 1
    await limiter.acquire()
    calls = []

    async def queued():
        with pytest.raises(tc.TailcatTimeout):
            await _run(lambda op: calls.append(True), timeout=0.01, kind="queue-test")
        with anyio.move_on_after(0.01) as scope:
            await _run(lambda op: calls.append(True), kind="queue-test")
        assert scope.cancel_called

    try:
        with anyio.fail_after(2):
            async with anyio.create_task_group() as group:
                group.start_soon(queued)
    finally:
        limiter.release()
    assert calls == []


@pytest.mark.anyio
async def test_cancelled_write_cannot_leave_a_worker_using_freed_buffers(relay):
    async with tc.AsyncServer(region=relay["region"]) as server:
        async with await server.listen_tcp(timeout=10) as listener:
            async with tc.AsyncClient(server.address) as client:
                async with await client.dial_tcp(listener.port, timeout=10) as outgoing:
                    async with await listener.accept(timeout=5):
                        # Larger than the receive window; the peer deliberately never reads.
                        with anyio.move_on_after(0.05) as scope:
                            await outgoing.sendall(b"x" * (16 * 1024 * 1024))
                        assert scope.cancel_called
                        await outgoing.aclose()


def test_close_and_drain_before_startup_are_safe():
    with tc.Server() as server:
        server.drain(timeout=0.1)


def test_abi_null_output_is_an_error_and_error_memory_is_releasable():
    lib = native()
    with pytest.raises(tc.InvalidArgumentError):
        lib.invoke("tc_key_generate", lib.ffi.NULL)
