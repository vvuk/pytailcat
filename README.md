# pytailcat

Python 3.12+ bindings for [Tailcat](https://github.com/tailscale/tailcat), with
synchronous and asynchronous HTTPX transports. Tailcat creates userspace,
WireGuard-encrypted point-to-point connections without a Tailscale account or
control plane. It uses DERP for rendezvous and relay fallback, and upgrades to
direct connectivity when possible. Root access and OS network configuration are
not required.

The Python package loads the Tailcat Go module through a versioned C API. It
supports TCP streams, connected UDP flows, client and server identities, private
DERP configuration, address utilities, and HTTP/1.1 or HTTP/2 over HTTP or HTTPS.

## Development installation

With these two checkouts next to each other:

```text
workspace/
  tailcat/       # Includes cmd/libtailcat
  pytailcat/
```

Run in `pytailcat/`:

```sh
uv sync --python 3.12 --extra httpx
```

Go (with automatic toolchain selection enabled) and a C compiler are required to
build from source. The required Go version is declared in Tailcat's `go.mod`.
Set `PYTAILCAT_SOURCE` to use a checkout somewhere else. After changing Go code,
rebuild with `uv sync --reinstall-package pytailcat --extra httpx`.

Built wheels include the native library and need no Go installation. Once a wheel
is available, install it with the `httpx` extra to enable the transport module.
The core package does not import or require HTTPX.

## HTTPX

On the remote machine, expose an existing HTTP service:

```sh
tailcat serve 8080
```

Pass the resulting secret Tailcat address as configuration. Keep the HTTP origin
as an ordinary URL:

```python
import os
import httpx
from pytailcat.httpx import TailcatTransport

origin = "http://service.internal:8080"
transport = TailcatTransport(
    address=os.environ["TAILCAT_ADDRESS"],
    origin=origin,
)
with httpx.Client(transport=transport, base_url=origin) as client:
    response = client.get("/health")
    response.raise_for_status()
    print(response.text)
```

The request dials port 8080 on the configured peer. The hostname is used for the
Host header and, with HTTPS, TLS certificate validation and SNI. It is not sent
to DNS to choose the peer. Requests to another origin, including cross-origin
redirects, are rejected. Use HTTPX mounts for multiple explicit peer mappings.

Asyncio and Trio use the same API:

```python
import os
import anyio
import httpx
from pytailcat.httpx import AsyncTailcatTransport


async def main():
    origin = "http://service.internal:8080"
    transport = AsyncTailcatTransport(
        address=os.environ["TAILCAT_ADDRESS"],
        origin=origin,
    )
    async with httpx.AsyncClient(transport=transport, base_url=origin) as client:
        async with client.stream("GET", "/events") as response:
            async for chunk in response.aiter_bytes():
                print(chunk)


anyio.run(main)  # or anyio.run(main, backend="trio")
```

HTTPS uses Python's TLS implementation over the Tailcat stream. Verification is
enabled by default with certifi's CA bundle. For a private CA or client certificate,
configure an `ssl.SSLContext` and pass `verify=context` to the transport. WireGuard
does not disable HTTPS certificate verification. `verify=False` is available for
explicitly unverified TLS connections.

Pass `limits=httpx.Limits(...)` and `retries=N` to the transport. Retries cover
connection failures/timeouts, following HTTPcore's semantics. Configure request
timeouts on the HTTPX client as usual. Connect, read, write, and pool timeouts
raise their corresponding HTTPX exceptions, including during response streaming.

To share a peer, pass `client=pytailcat.Client(...)` (or `AsyncClient`) instead of
`address`. The transport closes its connection pool but leaves a borrowed peer
open. A transport constructed from an address owns and closes its peer.

Proxy chaining, arbitrary exit-node destinations, OS socket options, and an OS
`fileno()` are not supported. The transport's origin is required. Configuration on
a custom transport, including TLS verification, pool limits, and HTTP versions,
takes precedence over the similarly named HTTPX client parameters.

### HTTP/2

The `httpx` extra includes the HTTP/2 dependencies. Enable HTTP/2 on either
transport (not just on the HTTPX client):

```python
transport = TailcatTransport(
    address=os.environ["TAILCAT_ADDRESS"],
    origin="https://service.internal:8443",
    http2=True,
)
# AsyncTailcatTransport accepts the same options.
```

HTTPS negotiates `h2` through TLS ALPN and falls back to HTTP/1.1 when necessary.
Both `http1=True` and `http2=False` are the defaults. Concurrent requests can
multiplex over a single connection; `limits.max_connections` limits connections,
not the number of HTTP/2 streams. Inspect `response.http_version` to see which
protocol was used. TLS certificate and hostname verification still apply.

For a peer known to speak HTTP/2 directly, use `http1=False, http2=True`. This also
supports a plain `http://` origin, whose traffic remains WireGuard-encrypted by
Tailcat. With `http1=True`, plain HTTP uses HTTP/1.1; HTTPcore does not implement
the HTTP/1.1 `Upgrade: h2c` exchange. See [HTTPcore's protocol negotiation
documentation](https://www.encode.io/httpcore/http2/#http2-negotiation).

Known upstream limitation: with HTTPcore 1.0.9, a synchronous thread reading an
indefinitely silent HTTP/2 response can hold the shared reader lock and stall
other streams, notably concurrent flow-controlled uploads. We reproduced this
with HTTPcore's stock TCP/TLS backend as well as Tailcat. For this workload, use
the async transport or separate transports/connections, and retain finite timeouts.
The timing-dependent reproducer is kept outside default test discovery:

```sh
uv run pytest tests/repro_httpcore_stall.py -v
```

## Raw TCP

Server:

```python
from pytailcat import Server

with Server() as server:
    with server.listen_tcp(8080, timeout=30) as listener:
        # Share server.address out of band with the intended client.
        with listener.accept(timeout=60) as connection:
            message = connection.recv(timeout=10)
            connection.sendall(message, timeout=10)
```

Client:

```python
import os
from pytailcat import Client

with Client(os.environ["TAILCAT_ADDRESS"]) as client:
    with client.dial_tcp(8080, timeout=30) as connection:
        connection.sendall(b"hello", timeout=10)
        print(connection.recv(timeout=10))
```

TCP is a byte stream: a receive may return less than a complete application
message. `recv()` returns `b""` at EOF; `send()` can return a short count;
`sendall()` sends everything within one deadline. `close_write()` sends a FIN
while allowing further reads. The core API supports one reader and one writer
concurrently. Failures expose `partial_data` or `bytes_written` when applicable.

`listen_tcp(0)` chooses a free port, available as `listener.port`. Connections
expose `(host, port)` tuples through `local_address` and `remote_address`.
Closing a listener leaves accepted connections open; closing a server or client
closes its dependent resources. Explicit context managers are recommended.
`drain(timeout=5)` optionally waits for TCP shutdown before closing the peer.

The async classes mirror these operations with `await` and `async with`:
`AsyncClient`, `AsyncServer`, `AsyncListener`, and `AsyncConnection`. Resource
closure is named `aclose()`. Construction performs no network access; dialing
starts clients lazily, and listening starts servers automatically. `start()` is
also available for servers. Properties such as `server.address` are local reads.

## UDP, identities, and relays

`listen_udp()` and `dial_udp()` return connected datagram flows. Each `send()`
sends one packet, and each `recv()` consumes one packet. An empty packet is valid
and is not EOF. Receive buffers smaller than a packet truncate it. Payloads larger
than `MAX_UDP_PAYLOAD` (1232) are rejected. UDP flows expire according to the
server's `udp_idle_timeout`; zero selects Tailcat's default.

`KeyPair.generate()` returns private, public, and pre-shared keys. Pass `key=pair`
to `Server` to preserve both identity and connection capability across restarts,
or to `Client` for a stable client identity. Store keys securely; their `repr`
omits secrets. `allowed_clients=[public_key, ...]` restricts a server, and
`allow_client(public_key)` adds a key. As in Tailcat, an empty allowlist permits
any client that possesses the address.

`Server(region=...)` accepts a DERPRegion dictionary with Tailcat's JSON names,
such as `RegionID` and `Nodes`. `region_id` selects a particular region from a
map. Both clients and servers support `derp_map_url`. With no configuration,
Tailcat uses its default public relay map. `parse_address()` returns public
metadata, and `resolve_address()` embeds relay metadata for later offline
resolution. `resolve_address_async()` is the asynchronous counterpart.
`client.ping()` returns relay latency as an integer number of milliseconds
(fractional milliseconds are truncated). `client.disco_ping()` probes discovery
and returns a dictionary with `latency` in seconds, `endpoint`, `derp_region_id`,
and `derp_region_code`. Both methods also exist on `AsyncClient` and accept a
`timeout` in seconds; use a timeout to bound discovery if no pong arrives.

Tailcat addresses normally contain a pre-shared secret. Keep them out of URL
hostnames, logs, and public DNS. The Go bridge discards diagnostic logs and parsed
address metadata omits that secret.

## Cancellation and process model

Async calls run blocking native operations in AnyIO worker threads. Cancelling a
task cancels the Go operation and waits for that worker to finish before releasing
buffers. Connection results produced during a cancellation race are closed.
Cancelled reads and accepts leave their resource usable; cancelled writes may
already have transmitted bytes. Cancelling an HTTP/1.1 request discards its
affected connection while leaving the peer and pool usable. For HTTP/2, cancelling
a response read preserves the connection and other streams, including when a
successful native read races with cancellation. A failed or cancelled in-flight
HTTP write closes the connection conservatively, because partially transmitted
frames or TLS records cannot safely be skipped. Connection-level I/O errors and
read/write timeouts can therefore affect all streams sharing that connection;
per-request cancellation is not the same as a connection read timeout.

Each event loop permits 64 active native operations per category (read, write,
accept, connect, control). Queue time counts toward deadlines, and queued calls
remain cancellable. Shutdown uses a separate worker path. These limits bound
worker-thread use; this version is not designed for thousands of simultaneous
blocked native operations. Use one async client/transport per event loop.

The shared library embeds a Go runtime. Do not unload it or use it after fork;
use the `spawn` multiprocessing start method. The Python wrapper rejects calls
from a forked child once the native library has been loaded. Free-threaded Python
builds are not part of the tested support matrix.

## Builds and verification

```sh
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
uv build
uv run python scripts/check_wheel.py --python 3.12 --integration
```

Tests start a local DERP/STUN relay and Tailcat HTTP/HTTPS services, with no public
relay dependency. They cover TLS ALPN, HTTP/1.1 fallback, prior-knowledge HTTP/2,
concurrent flow-controlled uploads, streaming, and cancellation using sync threads,
asyncio, and Trio. Go is required for the test fixtures. The native ABI's Go tests
also run with the race detector:

```sh
cd ../tailcat
go test -race ./internal/capi
```

Source distributions include a generated snapshot of the Go module, its pinned
`go.mod`/`go.sum`, and the C API changes. They build independently of the sibling
checkout, including before the C API is published upstream. The snapshot lives
under `native/tailcat/` and should not be edited; canonical Go changes belong in
`tailcat/`. Wheel builds include an SHA-256 fingerprint of their source inputs and
the Tailcat license. `build_info()` reports the native ABI and Go build metadata.

Wheels are platform-specific but independent of the CPython extension ABI. A local
Linux build produces a `linux_*` wheel; release wheels are built in manylinux
containers and repaired with `auditwheel`. macOS builds target
`MACOSX_DEPLOYMENT_TARGET` when set (release wheels use 12.0, the current Go
toolchain's minimum) and otherwise the build host's OS version. Windows builds
require a cgo-compatible C compiler. No package or release is published by the
build commands above.

## Continuous integration and releases

All workflows check out Tailcat from the `TAILCAT_REPOSITORY` and `TAILCAT_REF`
repository variables, defaulting to the fork branch that carries the C API until
it lands upstream.

- **CI** (`ci.yml`): runs the commands above plus the Go race tests on Linux
  x86_64, Linux arm64, and macOS arm64 for every push to `main` and every pull
  request. After verification passes it also runs the release wheel build so a
  broken release is caught early.
- **Build wheels** (`wheels.yml`): reusable workflow producing the sdist and one
  wheel per platform (`manylinux_2_28_x86_64`, `manylinux_2_28_aarch64`,
  `macosx_12_0_arm64`), each bundling that platform's `libtailcat`. Every wheel is
  then installed and smoke-tested on a plain runner without Go.
- **Release** (`release.yml`): pushing a tag `release-X.Y.Z` that matches the
  version in `pyproject.toml` builds all distributions and creates a GitHub release
  with them attached and generated notes. Versions with `a`, `b`, `rc`, or `dev` segments are
  marked pre-releases.
- **Publish** (`publish.yml`): when a release is published (or manually for a
  tag), downloads its assets and uploads them with `uv publish` to the private
  CodeArtifact index shared with Inferno (domain `nura`, repository `python`).
  Authentication is keyless GitHub OIDC: the job runs in the `production` GitHub
  Environment, which only `release-*` tags may deploy to, and assumes the
  `pytailcat-ci-publish` IAM role defined in Inferno's
  `infra/terraform/global/codeartifact.tf`. That role can publish only the
  `pytailcat` package.

To cut a release:

```sh
uv version X.Y.Z          # updates pyproject.toml and uv.lock
git commit -am "Release X.Y.Z"
git tag release-X.Y.Z && git push origin main release-X.Y.Z
```

The manual verification workflow (`verify.yml`) tests Python 3.12–3.14 on Linux,
macOS, and Windows and uploads build artifacts. It takes a Tailcat repository and
commit containing the C API, so it can test paired changes before an upstream
release.
