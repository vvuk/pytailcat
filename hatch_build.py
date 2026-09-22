"""Bundle the Go module and build its C ABI; no compiler is needed at runtime."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sysconfig
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        root = Path(self.root)
        bundled = root / "native" / "tailcat"
        override = os.environ.get("PYTAILCAT_SOURCE")
        sibling = root.parent / "tailcat"
        if override:
            source = Path(override).resolve()
        elif (sibling / "cmd" / "libtailcat" / "tailcat.h").is_file():
            source = sibling
        else:
            source = bundled
        if not (source / "cmd" / "libtailcat" / "tailcat.h").is_file():
            raise RuntimeError(
                "Tailcat sources with the C API are required. Set PYTAILCAT_SOURCE "
                "to that checkout, or build from the pytailcat source distribution."
            )
        files = sorted(
            p
            for p in source.rglob("*")
            if p.is_file()
            and (
                p.suffix in {".go", ".h", ".c"}
                or p.name
                in {"go.mod", "go.sum", "build-tags.txt", "LICENSE", "README.md"}
            )
            and not any(part.startswith(".") for part in p.relative_to(source).parts)
        )
        digest = hashlib.sha256()
        for path in files:
            digest.update(path.relative_to(source).as_posix().encode())
            digest.update(path.read_bytes())
        if self.target_name == "sdist":
            # Include an exact source snapshot, including the local C API changes.
            # This makes an sdist independent of a sibling checkout or unpublished ref.
            if source != bundled:
                if bundled.exists():
                    shutil.rmtree(bundled)
                for path in files:
                    target = bundled / path.relative_to(source)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, target)
            build_data["artifacts"] = ["native/tailcat/**"]
            return
        dest = root / "src" / "pytailcat" / "_native"
        dest.mkdir(parents=True, exist_ok=True)
        filename = {"Darwin": "libtailcat.dylib", "Windows": "tailcat.dll"}.get(
            platform.system(), "libtailcat.so"
        )
        env = {**os.environ, "CGO_ENABLED": "1"}
        target = os.environ.get("MACOSX_DEPLOYMENT_TARGET")
        if platform.system() == "Darwin" and target:
            # Go's build cache keys on CGO_CFLAGS/CGO_LDFLAGS but not on
            # MACOSX_DEPLOYMENT_TARGET, so pass the target explicitly. Otherwise a
            # cached library built for another deployment target is reused.
            flag = f"-mmacosx-version-min={target}"
            for name in ("CGO_CFLAGS", "CGO_LDFLAGS"):
                env[name] = f"{flag} {env.get(name, '')}".strip()
        tags = (source / "build-tags.txt").read_text().strip()
        subprocess.run(
            [
                "go",
                "build",
                "-buildmode=c-shared",
                "-trimpath",
                "-tags",
                tags,
                "-ldflags=-s -w",
                "-o",
                str(dest / filename),
                "./cmd/libtailcat",
            ],
            cwd=source,
            env=env,
            check=True,
        )
        shutil.copy2(source / "cmd" / "libtailcat" / "tailcat.h", dest / "tailcat.h")
        shutil.copy2(source / "LICENSE", dest / "TAILCAT_LICENSE")
        (dest / "source.json").write_text(
            json.dumps({"sha256": digest.hexdigest()}, indent=2) + "\n"
        )
        build_data["pure_python"] = False
        tag = sysconfig.get_platform().replace("-", "_").replace(".", "_")
        # Honor MACOSX_DEPLOYMENT_TARGET (which clang/cgo also read); otherwise
        # use the running macOS version as a conservative deployment floor.
        if platform.system() == "Darwin":
            release = (target or platform.mac_ver()[0]).split(".")
            minor = "0" if int(release[0]) >= 11 else release[1]
            tag = f"macosx_{release[0]}_{minor}_{platform.machine()}"
        build_data["tag"] = f"py3-none-{tag}"
        build_data["artifacts"] = ["src/pytailcat/_native/**"]
