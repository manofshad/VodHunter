import os
import time
import shutil
import sqlite3
import subprocess
from typing import Optional

from sources.audio_source import AudioSource
from sources.audio_chunk import AudioChunk


class LiveTwitchSource(AudioSource):
    def __init__(
        self,
        streamer: str,
        chunk_seconds: int = 60,
        temp_dir: str = "temp_live_chunks",
        db_path: str = "metadata.db",
    ):
        self.streamer = streamer
        self.chunk_seconds = chunk_seconds
        self.temp_dir = temp_dir
        self.db_path = db_path

        self.video_id: int | None = None
        self._next_index = 0
        self._finished = False

        self._ffmpeg_proc: subprocess.Popen | None = None

    # --------------------
    # Lifecycle
    # --------------------

    def start(self) -> None:
        """
        Resolve Twitch stream, register video, and start ffmpeg.
        """
        os.makedirs(self.temp_dir, exist_ok=True)

        hls_url = self._resolve_hls_url()
        ffmpeg_proc = self._start_ffmpeg(hls_url)

        # Create DB rows only after a live stream URL is resolved and recorder starts.
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.cursor()

            cur.execute(
                "INSERT OR IGNORE INTO creators (name, url) VALUES (?, ?)",
                (self.streamer, f"https://twitch.tv/{self.streamer}"),
            )
            cur.execute(
                "SELECT id FROM creators WHERE name = ?",
                (self.streamer,),
            )
            creator_id = cur.fetchone()[0]

            cur.execute(
                "INSERT INTO videos (creator_id, url, title, processed) VALUES (?, ?, ?, ?)",
                (
                    creator_id,
                    f"https://twitch.tv/{self.streamer}",
                    f"Live stream by {self.streamer}",
                    False,
                ),
            )
            self.video_id = cur.lastrowid
            conn.commit()
        except Exception:
            if ffmpeg_proc.poll() is None:
                ffmpeg_proc.terminate()
            raise
        finally:
            conn.close()

        self.hls_url = hls_url
        self._ffmpeg_proc = ffmpeg_proc

    def _start_ffmpeg(self, hls_url: str) -> subprocess.Popen:
        cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-i", hls_url,
            "-f", "segment",
            "-segment_time", str(self.chunk_seconds),
            "-ar", "16000",
            "-ac", "1",
            os.path.join(self.temp_dir, "chunk_%06d.wav"),
        ]

        return subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def next_chunk(self) -> Optional[AudioChunk]:
        """
        Return the next completed chunk if available.
        """
        if self._finished:
            return None

        expected_path = os.path.join(
            self.temp_dir,
            f"chunk_{self._next_index:06d}.wav",
        )

        if os.path.exists(expected_path) and self._is_file_stable(expected_path):
            chunk = AudioChunk(
                audio_path=expected_path,
                offset_seconds=self._next_index * self.chunk_seconds,
                duration_seconds=self.chunk_seconds,
            )
            self._next_index += 1
            return chunk

        # Check if ffmpeg exited
        if self._ffmpeg_proc and self._ffmpeg_proc.poll() is not None:
            self._finished = True

        return None

    def stop(self) -> None:
        """
        Stop ingest and clean up.
        """
        self._finished = True

        if self._ffmpeg_proc and self._ffmpeg_proc.poll() is None:
            self._ffmpeg_proc.terminate()

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    @property
    def is_finished(self) -> bool:
        return self._finished

    def _resolve_hls_url(self) -> str:
        """
        Resolve Twitch live stream to an HLS URL using yt-dlp.
        """
        import subprocess
        import json

        cmd = [
            "yt-dlp",
            "-f", "bestaudio",
            "-j",
            f"https://www.twitch.tv/{self.streamer}",
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"yt-dlp failed: {result.stderr.strip()}"
            )

        info = json.loads(result.stdout)

        if "url" not in info:
            raise RuntimeError("No stream URL found (stream may be offline)")

        return info["url"]

    def _is_file_stable(
            self,
            path: str,
            wait_seconds: float = 0.2,
    ) -> bool:
        """
        Returns True if file size is stable across a short interval.
        """
        try:
            size1 = os.path.getsize(path)
            time.sleep(wait_seconds)
            size2 = os.path.getsize(path)
            return size1 == size2 and size1 > 0
        except OSError:
            return False
