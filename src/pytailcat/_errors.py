class TailcatError(OSError):
    """An operation failed in the native Tailcat library."""

    partial_data: bytes = b""
    bytes_written: int = 0


class InvalidArgumentError(TailcatError, ValueError):
    """An address, configuration value, or operation is invalid."""


class ClosedError(TailcatError):
    """The resource, or its parent, has been closed."""


class TailcatTimeout(TailcatError, TimeoutError):
    """The operation's deadline expired."""


class CancelledError(TailcatError):
    """A native operation was cancelled."""
