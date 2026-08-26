from pathlib import Path
import math
import shutil
import subprocess
import uuid


class QueryPreprocessor:
    def __init__(self, temp_dir: str):
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def prepare(
        self,
        clip_path: str,
        duration_limit_seconds: float | None = None,
    ) -> str:
        src = Path(clip_path)
        if not src.exists():
            raise FileNotFoundError(f"Query clip not found: {clip_path}")

        if duration_limit_seconds is not None:
            duration_limit_seconds = float(duration_limit_seconds)
            if not math.isfinite(duration_limit_seconds) or duration_limit_seconds <= 0:
                raise ValueError("duration_limit_seconds must be a positive finite number")

        output_path = self.temp_dir / f"query_{uuid.uuid4().hex}.wav"
        cmd = [
            "ffmpeg",
            "-i",
            str(src),
        ]
        if duration_limit_seconds is not None:
            cmd.extend(["-t", f"{duration_limit_seconds:.6f}"])
        cmd.extend([
            "-vn",
            "-ar",
            "8000",
            "-ac",
            "1",
            "-c:a",
            "pcm_s16le",
            "-y",
            str(output_path),
            "-loglevel",
            "error",
        ])

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0 or not output_path.exists():
            message = result.stderr.strip() or "ffmpeg failed to preprocess query"
            raise RuntimeError(message)

        return str(output_path)

    def cleanup(self, path: str) -> None:
        target = Path(path)
        if target.exists():
            target.unlink()

    def cleanup_all(self) -> None:
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir, ignore_errors=True)
