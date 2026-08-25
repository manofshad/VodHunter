import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from math import ceil
import re
from typing import Any, Callable, Optional
import urllib.parse
import urllib.request

from pipeline.nmfp_inference import NMFP_HOP_SECONDS, NMFP_SAMPLE_RATE
from sources.audio_chunk import AudioChunk
from sources.audio_source import AudioSource
from storage.vector_store import VectorStore


@dataclass(frozen=True)
class HLSSegment:
    index: int
    start_seconds: float
    duration_seconds: float
    url: str
    key_tag: str | None = None
    map_tag: str | None = None
    byte_range_tag: str | None = None
    discontinuity: bool = False

    @property
    def end_seconds(self) -> float:
        return self.start_seconds + self.duration_seconds


@dataclass(frozen=True)
class HLSMediaPlaylist:
    url: str
    version: int
    media_sequence: int
    target_duration: int
    segments: tuple[HLSSegment, ...]


@dataclass(frozen=True)
class HLSChunkSelection:
    manifest: str
    local_seek_seconds: float
    first_segment_start_seconds: float


_HLS_URI_PATTERN = re.compile(r'URI="([^"]+)"')


def _absolutize_hls_tag_uri(tag: str, playlist_url: str) -> str:
    match = _HLS_URI_PATTERN.search(tag)
    if match is None:
        return tag
    absolute = urllib.parse.urljoin(playlist_url, match.group(1))
    return f'{tag[:match.start(1)]}{absolute}{tag[match.end(1):]}'


def parse_hls_media_playlist(manifest: str, playlist_url: str) -> HLSMediaPlaylist:
    version = 3
    media_sequence = 0
    target_duration = 0
    elapsed = 0.0
    pending_duration: float | None = None
    pending_byte_range: str | None = None
    pending_discontinuity = False
    active_key: str | None = None
    active_map: str | None = None
    segments: list[HLSSegment] = []

    for raw_line in manifest.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-VERSION:"):
            version = int(line.split(":", 1)[1])
        elif line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            media_sequence = int(line.split(":", 1)[1])
        elif line.startswith("#EXT-X-TARGETDURATION:"):
            target_duration = int(line.split(":", 1)[1])
        elif line.startswith("#EXT-X-KEY:"):
            active_key = _absolutize_hls_tag_uri(line, playlist_url)
        elif line.startswith("#EXT-X-MAP:"):
            active_map = _absolutize_hls_tag_uri(line, playlist_url)
        elif line.startswith("#EXT-X-BYTERANGE:"):
            pending_byte_range = line
        elif line == "#EXT-X-DISCONTINUITY":
            pending_discontinuity = True
        elif line.startswith("#EXTINF:"):
            pending_duration = float(line.split(":", 1)[1].split(",", 1)[0])
        elif not line.startswith("#") and pending_duration is not None:
            segments.append(
                HLSSegment(
                    index=len(segments),
                    start_seconds=elapsed,
                    duration_seconds=pending_duration,
                    url=urllib.parse.urljoin(playlist_url, line),
                    key_tag=active_key,
                    map_tag=active_map,
                    byte_range_tag=pending_byte_range,
                    discontinuity=pending_discontinuity,
                )
            )
            elapsed += pending_duration
            pending_duration = None
            pending_byte_range = None
            pending_discontinuity = False

    if not segments:
        raise RuntimeError("Resolved Twitch media URL did not contain HLS segments")
    if target_duration <= 0:
        target_duration = int(ceil(max(segment.duration_seconds for segment in segments)))

    return HLSMediaPlaylist(
        url=playlist_url,
        version=version,
        media_sequence=media_sequence,
        target_duration=target_duration,
        segments=tuple(segments),
    )


def select_hls_chunk(
    playlist: HLSMediaPlaylist,
    start_seconds: float,
    duration_seconds: float,
) -> HLSChunkSelection:
    requested_start = max(0.0, float(start_seconds))
    requested_end = requested_start + float(duration_seconds)
    selected = [
        segment
        for segment in playlist.segments
        if segment.end_seconds > requested_start and segment.start_seconds < requested_end
    ]
    if not selected:
        raise RuntimeError(
            f"HLS playlist has no media for {requested_start:.3f}-{requested_end:.3f} seconds"
        )

    lines = [
        "#EXTM3U",
        f"#EXT-X-VERSION:{playlist.version}",
        f"#EXT-X-TARGETDURATION:{playlist.target_duration}",
        f"#EXT-X-MEDIA-SEQUENCE:{playlist.media_sequence + selected[0].index}",
        "#EXT-X-PLAYLIST-TYPE:VOD",
    ]
    previous_key: str | None = None
    previous_map: str | None = None
    for segment in selected:
        if segment.discontinuity:
            lines.append("#EXT-X-DISCONTINUITY")
        if segment.key_tag is not None and segment.key_tag != previous_key:
            lines.append(segment.key_tag)
            previous_key = segment.key_tag
        if segment.map_tag is not None and segment.map_tag != previous_map:
            lines.append(segment.map_tag)
            previous_map = segment.map_tag
        if segment.byte_range_tag is not None:
            lines.append(segment.byte_range_tag)
        lines.append(f"#EXTINF:{segment.duration_seconds:.6f},")
        lines.append(segment.url)
    lines.append("#EXT-X-ENDLIST")

    return HLSChunkSelection(
        manifest="\n".join(lines) + "\n",
        local_seek_seconds=max(0.0, requested_start - selected[0].start_seconds),
        first_segment_start_seconds=selected[0].start_seconds,
    )


class HistoricalArchiveVODSource(AudioSource):
    def __init__(
        self,
        streamer: str,
        vod_metadata: dict[str, Any],
        store: VectorStore,
        creator_metadata: dict[str, Any] | None = None,
        chunk_seconds: int = 60,
        temp_dir: str = "temp_backfill_chunks",
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        extract_timeout_seconds: float = 180.0,
        metadata_timeout_seconds: float = 30.0,
    ):
        self.streamer = streamer.strip().lower()
        self.vod_metadata = dict(vod_metadata)
        self.creator_metadata = dict(creator_metadata or {})
        self.store = store
        self.chunk_seconds = int(chunk_seconds)
        self.temp_dir = temp_dir
        self.progress_callback = progress_callback
        self.extract_timeout_seconds = max(1.0, float(extract_timeout_seconds))
        self.metadata_timeout_seconds = max(1.0, float(metadata_timeout_seconds))

        self.video_id: int | None = None
        self.ingest_cursor_seconds: int = 0

        self._creator_id: int | None = None
        self._creator_profile_image_url: str | None = str(self.creator_metadata.get("profile_image_url") or "") or None
        self._started = False
        self._finished = False
        self._pending_commit_end_seconds: int | None = None
        self._pending_chunk_path: str | None = None
        self._media_url: str | None = None
        self._media_url_resolved_at = 0.0
        self._media_playlist: HLSMediaPlaylist | None = None

        self._vod_platform_id = str(self.vod_metadata.get("id", "")).strip()
        self.current_vod_url = str(self.vod_metadata.get("url", "")).strip()
        self._vod_title = str(self.vod_metadata.get("title") or f"Stream by {self.streamer}")
        self._vod_thumbnail_url = str(self.vod_metadata["thumbnail_url"]) if self.vod_metadata.get("thumbnail_url") else None
        self._duration_seconds = int(self.vod_metadata.get("duration_seconds") or 0)

        raw_created_at = str(self.vod_metadata.get("created_at") or "").strip()
        self._streamed_at: datetime | None = None
        if raw_created_at:
            try:
                self._streamed_at = datetime.fromisoformat(raw_created_at.replace("Z", "+00:00"))
            except ValueError:
                pass

    def start(self) -> None:
        if not self.streamer:
            raise ValueError("streamer is required")
        if not self._vod_platform_id:
            raise ValueError("vod id is required")
        if not self.current_vod_url:
            raise ValueError("vod url is required")
        if self._duration_seconds <= 0:
            raise ValueError("vod duration_seconds must be positive")

        os.makedirs(self.temp_dir, exist_ok=True)
        self._started = True

        creator_url = f"https://twitch.tv/{self.streamer}"
        self._creator_id = self.store.create_or_get_creator(
            self.streamer,
            creator_url,
            profile_image_url=self._creator_profile_image_url,
        )

        existing_video = self.store.get_video_by_url(self.current_vod_url)
        if existing_video is None:
            self.video_id = self.store.create_video(
                creator_id=self._creator_id,
                url=self.current_vod_url,
                title=self._vod_title,
                thumbnail_url=self._vod_thumbnail_url,
                processed=False,
                streamed_at=self._streamed_at,
                status="indexing",
            )
        else:
            self.video_id = int(existing_video[0])
            existing_status = None
            get_video_status = getattr(self.store, "get_video_status", None)
            if callable(get_video_status):
                existing_status = get_video_status(self.video_id)
            if existing_status == "reindex_requested":
                self.store.delete_vod_ingest_state(self._vod_platform_id)
            self.store.update_video_metadata(
                self.video_id,
                title=self._vod_title,
                thumbnail_url=self._vod_thumbnail_url,
                streamed_at=self._streamed_at,
                status="indexing",
            )

        state = self.store.get_vod_ingest_state(self._vod_platform_id)
        self.ingest_cursor_seconds = int(state.get("last_ingested_seconds", 0)) if state else 0
        self._save_ingest_state()

    def next_chunk(self) -> Optional[AudioChunk]:
        if self._finished:
            return None
        if not self._started:
            raise RuntimeError("source not started")

        self._commit_pending_progress()

        if self.ingest_cursor_seconds >= self._duration_seconds:
            self._finalize()
            return None

        chunk_len = min(self.chunk_seconds, self._duration_seconds - self.ingest_cursor_seconds)
        self._emit_progress(
            {
                "event": "chunk_start",
                "vod_id": self._vod_platform_id,
                "vod_url": self.current_vod_url,
                "streamer": self.streamer,
                "start_seconds": self.ingest_cursor_seconds,
                "end_seconds": self.ingest_cursor_seconds + chunk_len,
                "duration_seconds": self._duration_seconds,
                "percent_complete": ((self.ingest_cursor_seconds + chunk_len) / self._duration_seconds) * 100.0,
            }
        )
        extraction_start = max(0.0, float(self.ingest_cursor_seconds) - NMFP_HOP_SECONDS)
        overlap_seconds = float(self.ingest_cursor_seconds) - extraction_start
        extraction_duration = float(chunk_len) + overlap_seconds
        chunk_path = self._extract_chunk(extraction_start, extraction_duration)
        self._pending_commit_end_seconds = self.ingest_cursor_seconds + chunk_len
        self._pending_chunk_path = chunk_path

        return AudioChunk(
            audio_path=chunk_path,
            offset_seconds=extraction_start,
            duration_seconds=extraction_duration,
        )

    def stop(self) -> None:
        self._finished = True
        self._pending_commit_end_seconds = None
        if self._pending_chunk_path and os.path.exists(self._pending_chunk_path):
            os.remove(self._pending_chunk_path)
        self._pending_chunk_path = None
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    @property
    def creator_id(self) -> int | None:
        return self._creator_id

    @property
    def is_finished(self) -> bool:
        return self._finished

    def _extract_chunk(self, start_seconds: float, duration_seconds: float) -> str:
        start_milliseconds = int(round(float(start_seconds) * 1000.0))
        duration_milliseconds = int(round(float(duration_seconds) * 1000.0))
        output_path = os.path.join(
            self.temp_dir,
            f"vod_{self._vod_platform_id}_{start_milliseconds:012d}_{duration_milliseconds:08d}.wav",
        )
        playlist_path = f"{output_path}.m3u8"
        last_error = "unknown extraction failure"

        for attempt in range(2):
            self._remove_file(output_path)
            self._remove_file(playlist_path)
            try:
                playlist = self._load_media_playlist(force_refresh=attempt > 0)
                selection = select_hls_chunk(playlist, start_seconds, duration_seconds)
                with open(playlist_path, "w", encoding="utf-8") as handle:
                    handle.write(selection.manifest)

                cmd = [
                    "ffmpeg",
                    "-nostdin",
                    "-loglevel",
                    "error",
                    "-protocol_whitelist",
                    "file,http,https,tcp,tls,crypto",
                    "-i",
                    playlist_path,
                    "-ss",
                    f"{selection.local_seek_seconds:.3f}",
                    "-t",
                    f"{float(duration_seconds):.3f}",
                    "-vn",
                    "-ar",
                    str(NMFP_SAMPLE_RATE),
                    "-ac",
                    "1",
                    "-c:a",
                    "pcm_s16le",
                    "-y",
                    output_path,
                ]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.extract_timeout_seconds,
                )
                if result.returncode != 0:
                    message = result.stderr.strip() or "ffmpeg failed"
                    raise RuntimeError(message)
                self._validate_extracted_chunk(output_path, duration_seconds)
                return output_path
            except subprocess.TimeoutExpired:
                last_error = (
                    f"ffmpeg timed out after {self.extract_timeout_seconds:.1f}s "
                    f"for VOD range {start_seconds:.3f}-{start_seconds + duration_seconds:.3f}"
                )
            except (OSError, RuntimeError, ValueError) as exc:
                last_error = str(exc)
            finally:
                self._remove_file(playlist_path)

            self._invalidate_media_playlist()

        self._remove_file(output_path)
        raise RuntimeError(f"Failed to extract VOD chunk: {last_error}")

    def _resolve_media_url(self, *, force_refresh: bool = False) -> str:
        now = time.time()
        if not force_refresh and self._media_url and (now - self._media_url_resolved_at) < 900:
            return self._media_url

        try:
            result = subprocess.run(
                ["yt-dlp", "-g", self.current_vod_url],
                capture_output=True,
                text=True,
                timeout=self.metadata_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"yt-dlp timed out after {self.metadata_timeout_seconds:.1f}s"
            ) from exc
        if result.returncode != 0:
            raise RuntimeError(f"yt-dlp failed: {result.stderr.strip()}")

        media_url = ""
        for line in result.stdout.splitlines():
            candidate = line.strip()
            if candidate:
                media_url = candidate
                break
        if not media_url:
            raise RuntimeError("yt-dlp returned no media URL for VOD")

        self._media_url = media_url
        self._media_url_resolved_at = now
        return media_url

    def _load_media_playlist(self, *, force_refresh: bool = False) -> HLSMediaPlaylist:
        if not force_refresh and self._media_playlist is not None:
            return self._media_playlist

        media_url = self._resolve_media_url(force_refresh=force_refresh)
        try:
            with urllib.request.urlopen(media_url, timeout=self.metadata_timeout_seconds) as response:
                manifest = response.read().decode("utf-8", errors="replace")
        except (OSError, TimeoutError) as exc:
            raise RuntimeError(f"Failed to download Twitch HLS playlist: {exc}") from exc

        playlist = parse_hls_media_playlist(manifest, media_url)
        self._media_playlist = playlist
        return playlist

    def _validate_extracted_chunk(self, output_path: str, duration_seconds: float) -> None:
        if not os.path.exists(output_path) or os.path.getsize(output_path) <= 44:
            raise RuntimeError("Extracted chunk is missing or contains no PCM audio")

        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    output_path,
                ],
                capture_output=True,
                text=True,
                timeout=self.metadata_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"ffprobe timed out after {self.metadata_timeout_seconds:.1f}s"
            ) from exc
        if result.returncode != 0:
            message = result.stderr.strip() or "ffprobe failed"
            raise RuntimeError(f"Failed to validate extracted chunk: {message}")

        try:
            actual_duration = float(result.stdout.strip())
        except ValueError as exc:
            raise RuntimeError("ffprobe returned an invalid chunk duration") from exc

        allowed_shortfall = min(1.0, max(0.1, float(duration_seconds) * 0.02))
        minimum_duration = max(1.0, float(duration_seconds) - allowed_shortfall)
        if actual_duration < minimum_duration:
            raise RuntimeError(
                f"Extracted chunk is too short: expected at least {minimum_duration:.3f}s, "
                f"got {actual_duration:.3f}s"
            )

    def _invalidate_media_playlist(self) -> None:
        self._media_url = None
        self._media_url_resolved_at = 0.0
        self._media_playlist = None

    @staticmethod
    def _remove_file(path: str) -> None:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def _commit_pending_progress(self) -> None:
        if self._pending_commit_end_seconds is None:
            return

        self.ingest_cursor_seconds = int(self._pending_commit_end_seconds)
        self._pending_commit_end_seconds = None
        self._save_ingest_state()

        if self._pending_chunk_path and os.path.exists(self._pending_chunk_path):
            os.remove(self._pending_chunk_path)
        self._pending_chunk_path = None

    def _save_ingest_state(self) -> None:
        if self.video_id is None:
            return
        self.store.upsert_vod_ingest_state(
            vod_platform_id=self._vod_platform_id,
            video_id=self.video_id,
            streamer=self.streamer,
            last_ingested_seconds=self.ingest_cursor_seconds,
            last_seen_duration_seconds=self._duration_seconds,
        )

    def _finalize(self) -> None:
        self._commit_pending_progress()
        if self.video_id is not None:
            update_video_status = getattr(self.store, "update_video_status", None)
            if callable(update_video_status):
                update_video_status(self.video_id, "searchable")
            else:
                self.store.mark_video_processed(self.video_id, processed=True)
        self.store.delete_vod_ingest_state(self._vod_platform_id)
        self._finished = True
        self._emit_progress(
            {
                "event": "vod_complete",
                "vod_id": self._vod_platform_id,
                "vod_url": self.current_vod_url,
                "streamer": self.streamer,
                "duration_seconds": self._duration_seconds,
                "percent_complete": 100.0,
            }
        )

    def _emit_progress(self, event: dict[str, Any]) -> None:
        if self.progress_callback is None:
            return
        self.progress_callback(dict(event))
