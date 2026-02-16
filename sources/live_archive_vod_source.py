import os
import shutil
import subprocess
import time
from typing import Optional

from sources.audio_chunk import AudioChunk
from sources.audio_source import AudioSource
from services.twitch_monitor import TwitchMonitor
from storage.vector_store import VectorStore


class LiveArchiveVODSource(AudioSource):
    def __init__(
        self,
        streamer: str,
        store: VectorStore,
        twitch_monitor: TwitchMonitor,
        chunk_seconds: int = 60,
        lag_seconds: int = 120,
        poll_seconds: float = 15.0,
        finalize_checks: int = 3,
        temp_dir: str = "temp_live_chunks",
    ):
        self.streamer = streamer.strip().lower()
        self.store = store
        self.twitch_monitor = twitch_monitor
        self.chunk_seconds = int(chunk_seconds)
        self.lag_seconds = int(lag_seconds)
        self.poll_seconds = float(poll_seconds)
        self.finalize_checks = int(finalize_checks)
        self.temp_dir = temp_dir

        self.video_id: int | None = None
        self.current_vod_url: str | None = None
        self.ingest_cursor_seconds: int | None = 0

        self._finished = False
        self._started = False

        self._user_id: str | None = None
        self._vod_platform_id: str | None = None
        self._vod_title: str | None = None
        self._last_seen_duration_seconds = 0
        self._last_is_live: bool | None = None
        self._no_growth_checks = 0
        self._last_refresh_at = 0.0

        self._pending_commit_end_seconds: int | None = None
        self._pending_chunk_path: str | None = None

        self._media_url: str | None = None
        self._media_url_resolved_at = 0.0

    def start(self) -> None:
        os.makedirs(self.temp_dir, exist_ok=True)
        self._started = True
        self._refresh_state(force=True)

    def next_chunk(self) -> Optional[AudioChunk]:
        if self._finished:
            return None

        self._commit_pending_progress()
        self._refresh_state()

        if self._vod_platform_id is None or self.video_id is None:
            if self._last_is_live is False:
                self._finished = True
            return None

        cursor = int(self.ingest_cursor_seconds or 0)
        safe_end = self._last_seen_duration_seconds - (self.lag_seconds if self._last_is_live else 0)

        if safe_end > cursor:
            chunk_len = min(self.chunk_seconds, safe_end - cursor)
            chunk_path = self._extract_chunk(cursor, chunk_len)

            self._pending_commit_end_seconds = cursor + chunk_len
            self._pending_chunk_path = chunk_path

            return AudioChunk(
                audio_path=chunk_path,
                offset_seconds=float(cursor),
                duration_seconds=float(chunk_len),
            )

        if self._last_is_live is False and self._no_growth_checks >= self.finalize_checks:
            self._finalize()

        return None

    def stop(self) -> None:
        self._finished = True

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    @property
    def is_finished(self) -> bool:
        return self._finished

    def _refresh_state(self, force: bool = False) -> None:
        if not self._started:
            return

        now = time.time()
        if not force and (now - self._last_refresh_at) < self.poll_seconds:
            return

        self._last_refresh_at = now
        self._last_is_live = self.twitch_monitor.is_live(self.streamer)

        if self._user_id is None:
            self._user_id = self.twitch_monitor.get_user_id(self.streamer)

        latest_vod = self.twitch_monitor.get_latest_archive_vod(self._user_id)
        if latest_vod is None:
            return

        vod_platform_id = str(latest_vod["id"])
        if self._vod_platform_id != vod_platform_id:
            self._switch_to_vod(latest_vod)

        duration_seconds = int(latest_vod.get("duration_seconds") or 0)
        if duration_seconds > self._last_seen_duration_seconds:
            self._last_seen_duration_seconds = duration_seconds
            self._no_growth_checks = 0
        elif self._last_is_live is False:
            self._no_growth_checks += 1

        self._save_ingest_state()

    def _switch_to_vod(self, vod: dict) -> None:
        self._vod_platform_id = str(vod["id"])
        self.current_vod_url = str(vod["url"])
        self._vod_title = str(vod.get("title") or f"Live stream by {self.streamer}")

        creator_url = f"https://twitch.tv/{self.streamer}"
        creator_id = self.store.create_or_get_creator(self.streamer, creator_url)

        existing_video = self.store.get_video_by_url(self.current_vod_url)
        if existing_video is None:
            self.video_id = self.store.create_video(
                creator_id=creator_id,
                url=self.current_vod_url,
                title=self._vod_title,
                processed=False,
            )
        else:
            self.video_id = int(existing_video[0])
            self.store.mark_video_processed(self.video_id, processed=False)

        state = self.store.get_live_ingest_state(self._vod_platform_id)
        if state is None:
            self.ingest_cursor_seconds = 0
        else:
            self.ingest_cursor_seconds = int(state.get("last_ingested_seconds", 0))
            self._last_seen_duration_seconds = int(state.get("last_seen_duration_seconds", 0))

        self._pending_commit_end_seconds = None
        self._pending_chunk_path = None
        self._media_url = None
        self._media_url_resolved_at = 0.0
        self._no_growth_checks = 0

    def _extract_chunk(self, start_seconds: int, duration_seconds: int) -> str:
        if duration_seconds <= 0:
            raise RuntimeError("duration_seconds must be positive")
        if not self.current_vod_url:
            raise RuntimeError("current_vod_url is not set")

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
            media_url = self._resolve_media_url()
            cmd[6] = media_url
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                message = result.stderr.strip() or "ffmpeg failed"
                raise RuntimeError(f"Failed to extract VOD chunk: {message}")

        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            raise RuntimeError("Extracted chunk is missing or empty")

        return output_path

    def _resolve_media_url(self) -> str:
        if self.current_vod_url is None:
            raise RuntimeError("current_vod_url is not set")

        now = time.time()
        if self._media_url and (now - self._media_url_resolved_at) < 60:
            return self._media_url

        cmd = ["yt-dlp", "-g", self.current_vod_url]
        result = subprocess.run(cmd, capture_output=True, text=True)
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
        if self._vod_platform_id is None or self.video_id is None:
            return

        self.ingest_cursor_seconds = int(self._pending_commit_end_seconds)
        self._pending_commit_end_seconds = None

        self._save_ingest_state()

        if self._pending_chunk_path and os.path.exists(self._pending_chunk_path):
            os.remove(self._pending_chunk_path)
        self._pending_chunk_path = None

    def _save_ingest_state(self) -> None:
        if self._vod_platform_id is None or self.video_id is None:
            return

        self.store.upsert_live_ingest_state(
            vod_platform_id=self._vod_platform_id,
            video_id=self.video_id,
            streamer=self.streamer,
            last_ingested_seconds=int(self.ingest_cursor_seconds or 0),
            last_seen_duration_seconds=int(self._last_seen_duration_seconds),
        )

    def _finalize(self) -> None:
        self._commit_pending_progress()
        if self.video_id is not None:
            self.store.mark_video_processed(self.video_id, processed=True)
        self._finished = True
