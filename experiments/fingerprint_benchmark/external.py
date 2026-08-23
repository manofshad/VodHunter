from __future__ import annotations

import os
import shutil
import subprocess
import sys
import urllib.request
import venv
import zipfile
from pathlib import Path

from .audio import CommandError, check_free_space, run_command
from .config import BenchmarkConfig


NMFP_REPOSITORY = "https://github.com/raraz15/neural-music-fp.git"
NMFP_COMMIT = "15c6f3bcdf6a6da1daddfe47a1ffa5a0d22deadc"
NMFP_MODEL_URL = "https://zenodo.org/records/15719945/files/nmfp-triplet.zip?download=1"
AUDFPRINT_REPOSITORY = "https://github.com/dpwe/audfprint.git"
AUDFPRINT_COMMIT = "cb03ba99feafd41b8874307f0f4e808a6ce34362"


def _clone_pinned(repository: str, commit: str, target: Path) -> None:
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        run_command(["git", "clone", repository, str(target)])
    run_command(["git", "fetch", "--depth", "1", "origin", commit], cwd=target)
    run_command(["git", "checkout", "--detach", commit], cwd=target)


def setup_audfprint(config: BenchmarkConfig) -> dict[str, str]:
    target = config.external_dir / "audfprint"
    _clone_pinned(AUDFPRINT_REPOSITORY, AUDFPRINT_COMMIT, target)
    try:
        python = audfprint_python(config)
    except FileNotFoundError:
        venv_root = config.external_dir / "audfprint-venv"
        venv.EnvBuilder(with_pip=True).create(venv_root)
        python = _venv_python(venv_root)
        run_command([str(python), "-m", "pip", "install", "-r", str(target / "requirements.txt")])
    return {
        "repository": AUDFPRINT_REPOSITORY,
        "commit": AUDFPRINT_COMMIT,
        "path": str(target),
        "python": str(python),
    }


def setup_nmfp(config: BenchmarkConfig, *, download_weights: bool = True) -> dict[str, str]:
    check_free_space(config.external_dir, minimum_free_gb=1.0)
    target = config.external_dir / "neural-music-fp"
    _clone_pinned(NMFP_REPOSITORY, NMFP_COMMIT, target)
    model_root = target / "pretrained_models" / "nmfp-triplet"
    if download_weights and not list(model_root.rglob("ckpt-*.index")):
        zip_path = target / "nmfp-triplet.zip"
        with urllib.request.urlopen(NMFP_MODEL_URL) as response, zip_path.open("wb") as output:
            shutil.copyfileobj(response, output)
        model_root.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(model_root.parent)
        zip_path.unlink(missing_ok=True)
    config_candidates = list(model_root.rglob("config.yaml"))
    if download_weights and not config_candidates:
        raise RuntimeError(f"NMFP model archive did not create a config under {model_root}")
    return {
        "repository": NMFP_REPOSITORY,
        "commit": NMFP_COMMIT,
        "path": str(target),
        "model_root": str(model_root),
    }


def nmfp_python(config: BenchmarkConfig) -> Path:
    configured = os.environ.get("VODHUNTER_NMFP_PYTHON", "").strip()
    candidates = [Path(configured)] if configured else []
    candidates.extend(
        [
            config.external_dir / "nmfp-venv" / "bin" / "python",
            config.external_dir / "nmfp-venv" / "Scripts" / "python.exe",
        ]
    )
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    raise FileNotFoundError(
        "NMFP requires an isolated Python 3.11 environment. Set VODHUNTER_NMFP_PYTHON "
        "to that environment's Python executable."
    )


def _venv_python(venv_root: Path) -> Path:
    unix = venv_root / "bin" / "python"
    windows = venv_root / "Scripts" / "python.exe"
    return windows if windows.exists() else unix


def audfprint_python(config: BenchmarkConfig) -> Path:
    isolated = _venv_python(config.external_dir / "audfprint-venv")
    candidates = [isolated, Path(sys.executable)]
    for candidate in candidates:
        if not candidate.exists():
            continue
        result = subprocess.run(
            [str(candidate), "-c", "import docopt, joblib, numpy, psutil, scipy"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return candidate
    raise FileNotFoundError("No Python environment with audfprint's dependencies was found")
