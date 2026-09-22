"""Opt-in, timing-dependent reproducer of HTTPcore 1.0.9 sync reader starvation.

Run explicitly with pytest; this is deliberately outside default test discovery.
The stock backend bypasses Tailcat TCP and the pytailcat TLS adapter entirely.
"""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import httpcore
import httpx
import pytest
from test_http2 import transport


@pytest.mark.parametrize("backend", ["tailcat", "stock"])
def test_silent_reader_and_flow_controlled_uploads(http2_peer, backend):
    origin, t = transport(http2_peer)
    if backend == "stock":
        # Keep the logical origin/SNI, but connect to the same Go HTTP server
        # over an ordinary loopback socket using HTTPcore's own TLS machinery.
        class StockBackend(httpcore.SyncBackend):
            def connect_tcp(self, host, port, **options):
                return super().connect_tcp(
                    "127.0.0.1", http2_peer["local_https_port"], **options
                )

        t._pool._network_backend = StockBackend()

    token = uuid4().hex
    payload = b"flow-controlled upload\n" * 100_000
    with httpx.Client(transport=t, base_url=origin, timeout=5) as client:
        with client.stream("GET", f"/wait?token={token}") as waiting:

            def upload():
                response = client.post(
                    "/echo",
                    content=(
                        payload[n : n + 65536] for n in range(0, len(payload), 65536)
                    ),
                )
                assert response.content == payload

            with ThreadPoolExecutor(max_workers=5) as workers:
                silent = workers.submit(waiting.read)
                uploads = [workers.submit(upload) for _ in range(4)]
                try:
                    for future in uploads:
                        future.result(timeout=15)
                finally:
                    client.get(f"/release?token={token}")
                assert silent.result(timeout=10) == b"released"
