from pathlib import Path
import shutil
import subprocess
import uuid


class QueryPreprocessor:
    def __init__(self, temp_dir: str):
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def prepare(self, clip_path: str) -> str:
        src = Path(clip_path)
        if not src.exists():
            raise FileNotFoundError(f"Query clip not found: {clip_path}")

        output_path = self.temp_dir / f"query_{uuid.uuid4().hex}.wav"
        cmd = [
            "ffmpeg",
            "-i",
            str(src),
            "-ar",
            "16000",
            "-ac",
            "1",
            "-y",
            str(output_path),
            "-loglevel",
            "error",
        ]

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
