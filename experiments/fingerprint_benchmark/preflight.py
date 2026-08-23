from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from .config import BenchmarkConfig
from .external import audfprint_python, nmfp_python


def _check(ok: bool, detail: str, *, required: bool = True) -> dict[str, Any]:
    return {"ok": bool(ok), "required": required, "detail": detail}


def run_preflight(config: BenchmarkConfig, engine: str = "all") -> dict[str, Any]:
    selected = {"ast", "nmfp_triplet", "audfprint"} if engine == "all" else {engine}
    checks: dict[str, dict[str, Any]] = {}
    for binary in ("ffmpeg", "ffprobe", "yt-dlp", "git"):
        resolved = shutil.which(binary)
        checks[f"binary:{binary}"] = _check(bool(resolved), resolved or "not found")

    free_gib = shutil.disk_usage(config.artifacts).free / 1024**3
    checks["disk_space"] = _check(free_gib >= 2.0, f"{free_gib:.1f} GiB free")

    if "ast" in selected:
        try:
            import torch
            import transformers

            accelerator = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
            checks["ast:python"] = _check(True, f"torch={torch.__version__}, transformers={transformers.__version__}")
            checks["ast:accelerator"] = _check(
                accelerator != "cpu",
                accelerator,
                required=False,
            )
        except Exception as exc:
            checks["ast:python"] = _check(False, repr(exc))

    if "audfprint" in selected:
        script = config.external_dir / "audfprint" / "audfprint.py"
        checks["audfprint:source"] = _check(script.exists(), str(script))
        try:
            checks["audfprint:python"] = _check(True, str(audfprint_python(config)))
        except Exception as exc:
            checks["audfprint:python"] = _check(False, repr(exc))

    if "nmfp_triplet" in selected:
        repository = config.external_dir / "neural-music-fp"
        configs = list((repository / "pretrained_models").rglob("config.yaml")) if repository.exists() else []
        checkpoints = list((repository / "pretrained_models").rglob("ckpt-*.index")) if repository.exists() else []
        checks["nmfp:source"] = _check((repository / "extraction.py").exists(), str(repository))
        checks["nmfp:weights"] = _check(bool(configs and checkpoints), f"configs={len(configs)}, checkpoints={len(checkpoints)}")
        try:
            python = nmfp_python(config)
            result = subprocess.run(
                [
                    str(python),
                    "-c",
                    (
                        "import json,sys,tensorflow as tf,essentia,yaml; "
                        "print(json.dumps({'python':sys.version.split()[0],"
                        "'tensorflow':tf.__version__,'gpus':len(tf.config.list_physical_devices('GPU'))}))"
                    ),
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode != 0:
                raise RuntimeError((result.stderr or result.stdout).strip())
            runtime = json.loads(result.stdout.strip().splitlines()[-1])
            checks["nmfp:python"] = _check(runtime["python"].startswith("3.11."), json.dumps(runtime, sort_keys=True))
            checks["nmfp:gpu"] = _check(runtime["gpus"] > 0, f"TensorFlow GPUs={runtime['gpus']}", required=False)
        except Exception as exc:
            checks["nmfp:python"] = _check(False, repr(exc))

    checks["source_audio"] = _check(
        config.source_audio.exists(),
        str(config.source_audio),
        required=False,
    )
    required_failures = [name for name, value in checks.items() if value["required"] and not value["ok"]]
    return {
        "ok": not required_failures,
        "selected_engine": engine,
        "required_failures": required_failures,
        "checks": checks,
    }
