from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Sequence

from .config import BenchmarkConfig
from .manifest import upsert_records
from .models import QueryKind, QueryRecord


class CommandError(RuntimeError):
    pass


def run_command(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    accepted_return_codes: set[int] | None = None,
    timeout: int | None = None,
    env: dict[str, str] | None = None,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [str(part) for part in command],
        cwd=str(cwd) if cwd else None,
        capture_output=capture_output,
        text=True,
        timeout=timeout,
        env=({**os.environ, **env} if env else None),
    )
    accepted = accepted_return_codes or {0}
    if result.returncode not in accepted:
        detail = (result.stderr or result.stdout or "command failed").strip()
        raise CommandError(f"Command failed ({result.returncode}): {' '.join(command)}\n{detail}")
    return result


def require_binary(name: str) -> str:
    resolved = shutil.which(name)
    if not resolved:
        raise FileNotFoundError(f"Required executable is not installed: {name}")
    return resolved


def probe_duration(path: Path) -> float:
    ffprobe = require_binary("ffprobe")
    result = run_command(
        [
            ffprobe,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ]
    )
    try:
        duration = float(result.stdout.strip())
    except ValueError as exc:
        raise RuntimeError(f"Could not determine duration for {path}") from exc
    if duration <= 0:
        raise RuntimeError(f"Audio duration must be positive: {path}")
    return duration


def sha256_file(path: Path, block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(block_size):
            digest.update(block)
    return digest.hexdigest()


def resolve_twitch_audio_url(vod_url: str) -> str:
    yt_dlp = require_binary("yt-dlp")
    result = run_command(
        [yt_dlp, "--no-playlist", "--no-progress", "-f", "Audio_Only/bestaudio", "-g", vod_url],
        timeout=120,
    )
    media_url = next((line.strip() for line in result.stdout.splitlines() if line.strip()), "")
    if not media_url:
        raise RuntimeError("yt-dlp did not return a Twitch media URL")
    return media_url


def check_free_space(path: Path, minimum_free_gb: float = 2.0) -> None:
    path.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(path).free
    minimum = int(minimum_free_gb * 1024**3)
    if free < minimum:
        raise RuntimeError(
            f"Only {free / 1024**3:.1f} GiB is free; at least {minimum_free_gb:.1f} GiB is required"
        )


def prepare_vod(config: BenchmarkConfig, *, force: bool = False) -> dict[str, object]:
    config.ensure_directories()
    check_free_space(config.source_dir)
    if config.source_audio.exists() and not force:
        duration = probe_duration(config.source_audio)
    else:
        media_url = resolve_twitch_audio_url(config.vod_url)
        ffmpeg = require_binary("ffmpeg")
        temp_path = config.source_audio.with_suffix(".part.wav")
        temp_path.unlink(missing_ok=True)
        try:
            run_command(
                [
                    ffmpeg,
                    "-hide_banner",
                    "-loglevel",
                    "warning",
                    "-stats",
                    "-i",
                    media_url,
                    "-vn",
                    "-ar",
                    "16000",
                    "-ac",
                    "1",
                    "-c:a",
                    "pcm_s16le",
                    "-y",
                    str(temp_path),
                ],
                capture_output=False,
            )
            duration = probe_duration(temp_path)
            temp_path.replace(config.source_audio)
        finally:
            temp_path.unlink(missing_ok=True)

    metadata: dict[str, object] = {
        "vod_id": config.vod_id,
        "vod_url": config.vod_url,
        "streamer": config.streamer,
        "audio_path": str(config.source_audio),
        "duration_seconds": duration,
        "sample_rate": 16000,
        "channels": 1,
        "sha256": sha256_file(config.source_audio),
        "size_bytes": config.source_audio.stat().st_size,
    }
    config.source_metadata.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return metadata


def extract_audio_clip(
    source: Path,
    output: Path,
    *,
    start_seconds: float,
    duration_seconds: float,
    sample_rate: int = 16000,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    ffmpeg = require_binary("ffmpeg")
    temp_path = output.with_name(f".{output.stem}.{uuid.uuid4().hex}.tmp.wav")
    try:
        run_command(
            [
                ffmpeg,
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                f"{start_seconds:.3f}",
                "-i",
                str(source),
                "-t",
                f"{duration_seconds:.3f}",
                "-ar",
                str(sample_rate),
                "-ac",
                "1",
                "-c:a",
                "pcm_s16le",
                "-y",
                str(temp_path),
            ]
        )
        if not temp_path.exists() or probe_duration(temp_path) < max(1.0, duration_seconds - 0.25):
            raise RuntimeError(f"Generated clip is incomplete: {output}")
        temp_path.replace(output)
    finally:
        temp_path.unlink(missing_ok=True)


def normalize_audio(source: Path, output: Path, *, sample_rate: int) -> None:
    duration = probe_duration(source)
    extract_audio_clip(
        source,
        output,
        start_seconds=0.0,
        duration_seconds=duration,
        sample_rate=sample_rate,
    )


def generate_clean_queries(config: BenchmarkConfig, *, force: bool = False) -> list[QueryRecord]:
    if not config.source_audio.exists():
        raise FileNotFoundError("Prepare the VOD before generating clean clips")
    duration = probe_duration(config.source_audio)
    clip_duration = config.clean_clip_seconds
    margin = min(config.clean_edge_margin_seconds, max(0.0, (duration - clip_duration) / 10.0))
    available = duration - (2 * margin) - clip_duration
    if available < 0:
        raise RuntimeError(f"VOD is too short for a {clip_duration:.1f}-second clean clip")

    records: list[QueryRecord] = []
    for index in range(config.clean_clip_count):
        fraction = (index + 0.5) / config.clean_clip_count
        start = round(margin + available * fraction, 3)
        query_id = f"clean_{index + 1:02d}"
        output = config.queries_dir / "clean" / f"{query_id}.wav"
        if force or not output.exists():
            extract_audio_clip(
                config.source_audio,
                output,
                start_seconds=start,
                duration_seconds=clip_duration,
            )
        records.append(
            QueryRecord(
                query_id=query_id,
                kind="clean",
                path=str(output.relative_to(config.artifacts)),
                expected_match=True,
                expected_start_seconds=start,
                source_url=config.vod_url,
                duration_seconds=probe_duration(output),
                notes="Deterministic evenly distributed source-VOD excerpt",
            )
        )
    upsert_records(config.manifest_path, records)
    return records


def import_query(
    config: BenchmarkConfig,
    *,
    source: str,
    kind: QueryKind,
    query_id: str,
    expected_start_seconds: float | None,
    notes: str | None = None,
) -> QueryRecord:
    if kind == "clean":
        raise ValueError("Use generate-clean for clean source queries")
    expected_match = kind != "no_match"
    if expected_match and expected_start_seconds is None:
        raise ValueError("TikTok queries require a verified expected start timestamp")
    target = config.queries_dir / kind / f"{query_id}.wav"
    target.parent.mkdir(parents=True, exist_ok=True)
    source_url: str | None = None
    if source.startswith(("http://", "https://")):
        source_url = source
        yt_dlp = require_binary("yt-dlp")
        template = target.with_suffix(".download.%(ext)s")
        run_command([yt_dlp, "--no-playlist", "--no-progress", "-o", str(template), source], timeout=180)
        candidates = list(target.parent.glob(f"{target.stem}.download.*"))
        if not candidates:
            raise RuntimeError("yt-dlp completed without creating a query file")
        downloaded = max(candidates, key=lambda item: item.stat().st_size)
        try:
            normalize_audio(downloaded, target, sample_rate=16000)
        finally:
            for candidate in candidates:
                candidate.unlink(missing_ok=True)
    else:
        local_source = Path(source).expanduser().resolve()
        if not local_source.exists():
            raise FileNotFoundError(local_source)
        normalize_audio(local_source, target, sample_rate=16000)

    record = QueryRecord(
        query_id=query_id,
        kind=kind,
        path=str(target.relative_to(config.artifacts)),
        expected_match=expected_match,
        expected_start_seconds=expected_start_seconds,
        source_url=source_url,
        duration_seconds=probe_duration(target),
        notes=notes,
    )
    upsert_records(config.manifest_path, [record])
    return record
