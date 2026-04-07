import os
import shutil
import subprocess
import time
from datetime import datetime
from typing import Any, Callable, Optional

from sources.audio_chunk import AudioChunk
from sources.audio_source import AudioSource
from storage.vector_store import VectorStore


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
    ):
        self.streamer = streamer.strip().lower()
        self.vod_metadata = dict(vod_metadata)
        self.creator_metadata = dict(creator_metadata or {})
        self.store = store
        self.chunk_seconds = int(chunk_seconds)
        self.temp_dir = temp_dir
        self.progress_callback = progress_callback

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
            )
        else:
            self.video_id = int(existing_video[0])
            self.store.update_video_metadata(
                self.video_id,
                title=self._vod_title,
                thumbnail_url=self._vod_thumbnail_url,
                processed=False,
                streamed_at=self._streamed_at,
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
        chunk_path = self._extract_chunk(self.ingest_cursor_seconds, chunk_len)
        self._pending_commit_end_seconds = self.ingest_cursor_seconds + chunk_len
        self._pending_chunk_path = chunk_path

        return AudioChunk(
            audio_path=chunk_path,
            offset_seconds=float(self.ingest_cursor_seconds),
            duration_seconds=float(chunk_len),
        )

    def stop(self) -> None:
        self._commit_pending_progress()
        self._finished = True
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    @property
    def creator_id(self) -> int | None:
        return self._creator_id

    @property
    def is_finished(self) -> bool:
        return self._finished

    def _extract_chunk(self, start_seconds: int, duration_seconds: int) -> str:
        output_path = os.path.join(
            self.temp_dir,
            f"vod_{self._vod_platform_id}_{start_seconds:08d}_{duration_seconds:04d}.wav",
        )
        media_url = self._resolve_media_url()
        cmd = [
            "ffmpeg",
            "-loglevel",
            "error",
            "-ss",
            str(start_seconds),
            "-i",
            media_url,
            "-t",
            str(duration_seconds),
            "-ar",
            "16000",
            "-ac",
            "1",
            "-y",
            output_path,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            self._media_url = None
            self._media_url_resolved_at = 0.0
            cmd[6] = self._resolve_media_url()
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                message = result.stderr.strip() or "ffmpeg failed"
                raise RuntimeError(f"Failed to extract VOD chunk: {message}")

        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            raise RuntimeError("Extracted chunk is missing or empty")
        return output_path

    def _resolve_media_url(self) -> str:
        now = time.time()
        if self._media_url and (now - self._media_url_resolved_at) < 60:
            return self._media_url

        result = subprocess.run(
            ["yt-dlp", "-g", self.current_vod_url],
            capture_output=True,
            text=True,
        )
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
