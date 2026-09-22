"""Install and verify a built wheel in an isolated, disposable environment."""

from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", default="3.12")
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--integration", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    wheel = args.wheel
    if wheel is None:
        wheels = sorted((root / "dist").glob("*.whl"), key=lambda p: p.stat().st_mtime)
        if not wheels:
            parser.error("Build a wheel with uv build first")
        wheel = wheels[-1]
    wheel = wheel.resolve()
    with tempfile.TemporaryDirectory(prefix="pytailcat-wheel-") as temp:
        directory = Path(temp)
        environment = directory / "env"
        subprocess.run(
            ["uv", "venv", "--python", args.python, str(environment)], check=True
        )
        python = environment / (
            "Scripts/python.exe" if os.name == "nt" else "bin/python"
        )
        requirements = [f"{wheel}[httpx]"]
        if args.integration:
            requirements += ["pytest", "trustme", "trio"]
        subprocess.run(
            ["uv", "pip", "install", "--python", str(python), *requirements], check=True
        )
        subprocess.run(
            [
                str(python),
                "-I",
                "-c",
                """
from pathlib import Path
import pytailcat
import httpx
from pytailcat.httpx import TailcatTransport, AsyncTailcatTransport
assert 'site-packages' in Path(pytailcat.__file__).parts
assert pytailcat.build_info()['abi_version'] == 1
assert issubclass(TailcatTransport, httpx.BaseTransport)
assert issubclass(AsyncTailcatTransport, httpx.AsyncBaseTransport)
with pytailcat.Server(key=pytailcat.KeyPair.generate()) as server:
    server.drain(timeout=1)
print('Installed wheel smoke test passed:', pytailcat.__file__)
""",
            ],
            cwd=directory,
            # Prove importing/using the wheel does not require Go on PATH.
            env={**os.environ, "PATH": str(python.parent)},
            check=True,
        )
        if args.integration:
            subprocess.run(
                [str(python), "-I", "-m", "pytest", "-q", str(root / "tests")],
                cwd=directory,
                check=True,
            )


if __name__ == "__main__":
    main()
