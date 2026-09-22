from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
import trustme


@pytest.fixture(scope="session")
def relay(tmp_path_factory):
    directory = tmp_path_factory.mktemp("tailcat-relay")
    ca = trustme.CA()
    cert = ca.issue_cert("service.internal")
    cert.private_key_pem.write_to_path(directory / "key.pem")
    cert.cert_chain_pems[0].write_to_path(directory / "cert.pem")
    root = Path(__file__).resolve().parents[1]
    source = Path(os.environ.get("PYTAILCAT_SOURCE", root.parent / "tailcat"))
    if not source.is_dir():
        source = root / "native" / "tailcat"
    executable = directory / ("fixture.exe" if os.name == "nt" else "fixture")
    subprocess.run(
        [
            "go",
            "build",
            "-tags",
            (source / "build-tags.txt").read_text().strip(),
            "-o",
            str(executable),
            "./internal/capitest",
        ],
        cwd=source,
        check=True,
    )
    with (directory / "stderr.log").open("w+") as log:
        process = subprocess.Popen(
            [
                str(executable),
                "-cert",
                str(directory / "cert.pem"),
                "-key",
                str(directory / "key.pem"),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
        )
        try:
            line = process.stdout.readline()
            if not line:
                log.seek(0)
                pytest.fail(f"Tailcat fixture failed: {log.read()}")
            config = json.loads(line)
            config["ca"] = ca
            yield config
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request):
    return request.param
