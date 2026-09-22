import threading
from concurrent.futures import ThreadPoolExecutor

import anyio
import httpcore
import pytest

from pytailcat import TailcatTimeout
from pytailcat._ffi import Operation
from pytailcat._httpcore import _AsyncStream, _Stream


class Connection:
    closed = False

    def close(self):
        self.closed = True


@pytest.mark.anyio
async def test_cancellation_racing_successful_network_read_preserves_bytes():
    entered, release = threading.Event(), threading.Event()
    scopes = []
    payload = b"frames for several HTTP/2 streams"

    class ControlledConnection(Connection):
        def _recv(self, op, size):
            entered.set()
            assert release.wait(5)
            return payload

    stream = _AsyncStream(_Stream(ControlledConnection()))

    async def read():
        with anyio.CancelScope() as scope:
            scopes.append(scope)
            await stream.read(65536)

    with anyio.fail_after(5):
        async with anyio.create_task_group() as group:
            group.start_soon(read)
            assert await anyio.to_thread.run_sync(entered.wait, 3)
            scopes[0].cancel()
            release.set()
    assert await stream.read(6) == payload[:6]
    assert await stream.read(65536) == payload[6:]
    assert not stream._sync.connection.closed


@pytest.mark.anyio
async def test_tls_lock_wait_can_be_cancelled_without_waiting_for_other_io():
    stream = _AsyncStream(_Stream(Connection()))
    # A TLS record has been generated, but a concurrent writer owns the wire.
    stream._sync._outgoing.write(b"pending TLS record")
    with stream._sync._send_lock:
        from pytailcat._async import _run

        with anyio.fail_after(3):
            with anyio.move_on_after(0.05) as scope:
                await _run(lambda op: stream._sync._flush(op))
            assert scope.cancel_called
    assert stream._sync.connection.closed


def test_tls_lock_wait_counts_toward_operation_deadline():
    stream = _Stream(Connection())
    stream._outgoing.write(b"pending TLS record")
    with stream._send_lock, Operation(0.01) as op:
        with pytest.raises(TailcatTimeout):
            stream._flush(op)
    assert stream.connection.closed


def test_tls_flush_keeps_record_order_without_holding_ssl_state_lock():
    entered, release = threading.Event(), threading.Event()
    sent = []

    class ControlledConnection(Connection):
        def _sendall(self, op, data):
            if data == b"first":
                entered.set()
                assert release.wait(5)
            sent.append(data)

    stream = _Stream(ControlledConnection())

    def flush():
        with Operation(5) as op:
            stream._flush(op)

    stream._outgoing.write(b"first")
    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(flush)
        assert entered.wait(3)
        # The blocked write must not prevent the TLS reader from using SSL/BIOs.
        acquired = stream._state_lock.acquire(timeout=1)
        try:
            assert acquired
            stream._outgoing.write(b"second")
        finally:
            if acquired:
                stream._state_lock.release()
            release.set()
        second = workers.submit(flush)
        first.result(timeout=5)
        second.result(timeout=5)
    assert sent == [b"first", b"second"]


def test_partial_network_write_failure_invalidates_connection():
    class FailingConnection(Connection):
        def _sendall(self, op, data):
            raise TailcatTimeout("partial write")

    stream = _Stream(FailingConnection())
    with pytest.raises(httpcore.WriteTimeout):
        stream.write(b"HTTP/2 frame")
    assert stream.connection.closed
