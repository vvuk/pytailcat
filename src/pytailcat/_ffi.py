from __future__ import annotations

import json
import math
import os
import platform
import threading
from pathlib import Path
from typing import Any

from cffi import FFI

from ._errors import (
    CancelledError,
    ClosedError,
    InvalidArgumentError,
    TailcatError,
    TailcatTimeout,
)

_lock = threading.Lock()
_instance: Native | None = None


class Native:
    def __init__(self) -> None:
        self.pid = os.getpid()
        directory = Path(__file__).parent / "_native"
        name = {"Darwin": "libtailcat.dylib", "Windows": "tailcat.dll"}.get(
            platform.system(), "libtailcat.so"
        )
        path = Path(os.environ.get("PYTAILCAT_LIBRARY", directory / name)).resolve()
        try:
            header = (directory / "tailcat.h").read_text()
            self.ffi: Any = FFI()
            self.ffi.cdef(
                header.split("/* BEGIN CFFI */")[1].split("/* END CFFI */")[0]
            )
            self.lib: Any = self.ffi.dlopen(str(path))
        except (OSError, IndexError) as exc:
            raise ImportError(
                "Cannot load libtailcat. Install a platform wheel or rebuild pytailcat "
                "with Go and a C compiler (uv sync --reinstall-package pytailcat)."
            ) from exc
        if self.lib.tc_abi_version() != 1:
            raise ImportError(
                "Unsupported libtailcat ABI; pytailcat requires ABI version 1"
            )

    def invoke(self, name: str, *args: Any, allow_eof: bool = False) -> int:
        if os.getpid() != self.pid:
            raise RuntimeError(
                "Tailcat cannot be used after fork; use multiprocessing spawn"
            )
        error = self.ffi.new("char **")
        code = getattr(self.lib, name)(*args, error)
        message = "Tailcat operation failed"
        if error[0] != self.ffi.NULL:
            try:
                message = self.ffi.string(error[0]).decode("utf-8", "replace")
            finally:
                self.lib.tc_free(error[0])
        if code == 0 or (allow_eof and code == 6):
            return code
        exception = {
            1: InvalidArgumentError,
            2: ClosedError,
            3: TailcatTimeout,
            4: CancelledError,
        }.get(code, TailcatError)
        raise exception(message)

    def handle(self, name: str, *args: Any) -> int:
        out = self.ffi.new("tc_handle *")
        self.invoke(name, *args, out)
        return int(out[0])

    def text(self, name: str, *args: Any) -> str:
        out = self.ffi.new("char **")
        self.invoke(name, *args, out)
        try:
            return self.ffi.string(out[0]).decode()
        finally:
            self.lib.tc_free(out[0])

    def json(self, name: str, *args: Any) -> dict[str, Any]:
        return json.loads(self.text(name, *args))

    def close(self, handle: int) -> None:
        try:
            self.invoke("tc_close", handle)
        except ClosedError:
            pass


def native() -> Native:
    global _instance
    if _instance is not None and _instance.pid != os.getpid():
        raise RuntimeError(
            "Tailcat cannot be used after fork; use multiprocessing spawn"
        )
    with _lock:
        if _instance is None:
            _instance = Native()
        return _instance


def encode(value: str) -> bytes:
    if not isinstance(value, str) or "\0" in value:
        raise InvalidArgumentError("Expected a string without NUL bytes")
    return value.encode()


def config_bytes(config: dict[str, Any]) -> bytes:
    return json.dumps(config, allow_nan=False).encode()


class Operation:
    def __init__(self, timeout: float | None = None) -> None:
        if timeout is None:
            ns = -1
        elif not math.isfinite(timeout) or timeout < 0 or timeout >= 2**63 / 1e9:
            raise ValueError("timeout must be a finite non-negative number or None")
        else:
            ns = int(timeout * 1e9)
        self.native = native()
        self.handle = self.native.handle("tc_operation_new", ns)

    def cancel(self) -> None:
        self.native.invoke("tc_operation_cancel", self.handle)

    def close(self) -> None:
        self.native.close(self.handle)

    def __enter__(self) -> Operation:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
