"""Point-to-point WireGuard networking powered by the Tailcat Go library."""

from ._async import (
    AsyncClient,
    AsyncConnection,
    AsyncDatagramConnection,
    AsyncListener,
    AsyncServer,
)
from ._async import (
    resolve_address as resolve_address_async,
)
from ._core import (
    MAX_UDP_PAYLOAD,
    Client,
    Connection,
    DatagramConnection,
    KeyPair,
    Listener,
    Server,
    build_info,
    parse_address,
    resolve_address,
)
from ._errors import (
    CancelledError,
    ClosedError,
    InvalidArgumentError,
    TailcatError,
    TailcatTimeout,
)

__all__ = [
    "MAX_UDP_PAYLOAD",
    "AsyncClient",
    "AsyncConnection",
    "AsyncDatagramConnection",
    "AsyncListener",
    "AsyncServer",
    "CancelledError",
    "Client",
    "ClosedError",
    "Connection",
    "DatagramConnection",
    "InvalidArgumentError",
    "KeyPair",
    "Listener",
    "Server",
    "TailcatError",
    "TailcatTimeout",
    "build_info",
    "parse_address",
    "resolve_address",
    "resolve_address_async",
]
