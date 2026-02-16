import os
import sqlite3
import numpy as np
from datetime import datetime, timezone
from typing import List


class VectorStore:
    def __init__(
        self,
        db_path: str = "metadata.db",
        vector_file: str = "vectors.npy",
        id_file: str = "ids.npy",
    ):
        self.db_path = db_path
        self.vector_file = vector_file
        self.id_file = id_file

    # ---------- DB INIT ----------
    def init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS creators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                url TEXT UNIQUE
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creator_id INTEGER,
                url TEXT,
                title TEXT,
                processed BOOLEAN DEFAULT FALSE,
                FOREIGN KEY(creator_id) REFERENCES creators(id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fingerprints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id INTEGER,
                timestamp_seconds REAL,
                FOREIGN KEY(video_id) REFERENCES videos(id)
            )
        """)

        # Migration safety: older databases may contain duplicate
        # (video_id, timestamp_seconds) rows. Remove extras before the
        # unique index is created so startup does not fail.
        cur.execute(
            """
            DELETE FROM fingerprints
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM fingerprints
                GROUP BY video_id, timestamp_seconds
            )
            """
        )

        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_fingerprints_video_timestamp
            ON fingerprints(video_id, timestamp_seconds)
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS live_ingest_state (
                vod_platform_id TEXT PRIMARY KEY,
                video_id INTEGER NOT NULL,
                streamer TEXT NOT NULL,
                last_ingested_seconds INTEGER NOT NULL,
                last_seen_duration_seconds INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(video_id) REFERENCES videos(id)
            )
            """
        )

        conn.commit()
        conn.close()

    # ---------- VECTOR STORAGE ----------
    def append_vectors(
        self,
        embeddings: np.ndarray,
        ids: List[int],
    ) -> None:
        if embeddings.size == 0:
            return

        if os.path.exists(self.vector_file) and os.path.exists(self.id_file):
            existing_vecs = np.load(self.vector_file)
            existing_ids = np.load(self.id_file)

            combined_vecs = np.concatenate([existing_vecs, embeddings], axis=0)
            combined_ids = np.concatenate([existing_ids, np.array(ids)], axis=0)
        else:
            combined_vecs = embeddings
            combined_ids = np.array(ids)

        np.save(self.vector_file, combined_vecs)
        np.save(self.id_file, combined_ids)

        print(f"💾 Saved {len(combined_vecs)} total vectors")

    # ---------- HIGH-LEVEL STORE ----------
    def store_fingerprints(
        self,
        video_id: int,
        timestamps: np.ndarray,
    ) -> List[int]:
        """
        Inserts fingerprint rows (idempotent per video+timestamp) and returns row IDs.
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        ids: List[int] = []

        for ts in timestamps:
            value = float(ts)
            cur.execute(
                "INSERT OR IGNORE INTO fingerprints (video_id, timestamp_seconds) VALUES (?, ?)",
                (video_id, value),
            )

            if cur.rowcount == 1:
                ids.append(int(cur.lastrowid))
                continue

            cur.execute(
                "SELECT id FROM fingerprints WHERE video_id = ? AND timestamp_seconds = ?",
                (video_id, value),
            )
            existing = cur.fetchone()
            if existing is None:
                raise RuntimeError("Failed to resolve fingerprint id after idempotent insert")
            ids.append(int(existing[0]))

        conn.commit()
        conn.close()
        return ids

    def get_video_by_url(self, url: str) -> tuple[int, int, str, str, bool] | None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT videos.id, videos.creator_id, videos.url, videos.title, videos.processed
            FROM videos
            WHERE videos.url = ?
            LIMIT 1
            """,
            (url,),
        )
        row = cur.fetchone()
        conn.close()

        if row is None:
            return None

        return (
            int(row[0]),
            int(row[1]),
            str(row[2]),
            str(row[3]),
            bool(row[4]),
        )

    def create_or_get_creator(self, name: str, url: str) -> int:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO creators (name, url) VALUES (?, ?)",
            (name, url),
        )
        cur.execute(
            "SELECT id FROM creators WHERE url = ?",
            (url,),
        )
        row = cur.fetchone()
        conn.commit()
        conn.close()

        if row is None:
            raise RuntimeError("Failed to resolve creator id")
        return int(row[0])

    def create_video(
        self,
        creator_id: int,
        url: str,
        title: str,
        processed: bool,
    ) -> int:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO videos (creator_id, url, title, processed) VALUES (?, ?, ?, ?)",
            (int(creator_id), url, title, bool(processed)),
        )
        video_id = int(cur.lastrowid)
        conn.commit()
        conn.close()
        return video_id

    def mark_video_processed(self, video_id: int, processed: bool = True) -> None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            "UPDATE videos SET processed = ? WHERE id = ?",
            (bool(processed), int(video_id)),
        )
        conn.commit()
        conn.close()

    def get_live_ingest_state(self, vod_platform_id: str) -> dict | None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT vod_platform_id, video_id, streamer, last_ingested_seconds,
                   last_seen_duration_seconds, updated_at
            FROM live_ingest_state
            WHERE vod_platform_id = ?
            LIMIT 1
            """,
            (vod_platform_id,),
        )
        row = cur.fetchone()
        conn.close()

        if row is None:
            return None

        return {
            "vod_platform_id": str(row[0]),
            "video_id": int(row[1]),
            "streamer": str(row[2]),
            "last_ingested_seconds": int(row[3]),
            "last_seen_duration_seconds": int(row[4]),
            "updated_at": str(row[5]),
        }

    def upsert_live_ingest_state(
        self,
        vod_platform_id: str,
        video_id: int,
        streamer: str,
        last_ingested_seconds: int,
        last_seen_duration_seconds: int,
    ) -> None:
        updated_at = datetime.now(timezone.utc).isoformat()

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO live_ingest_state (
                vod_platform_id,
                video_id,
                streamer,
                last_ingested_seconds,
                last_seen_duration_seconds,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(vod_platform_id) DO UPDATE SET
                video_id = excluded.video_id,
                streamer = excluded.streamer,
                last_ingested_seconds = excluded.last_ingested_seconds,
                last_seen_duration_seconds = excluded.last_seen_duration_seconds,
                updated_at = excluded.updated_at
            """,
            (
                vod_platform_id,
                int(video_id),
                streamer,
                int(last_ingested_seconds),
                int(last_seen_duration_seconds),
                updated_at,
            ),
        )
        conn.commit()
        conn.close()

    # ---------- READ HELPERS ----------
    def load_vectors_and_ids(self) -> tuple[np.ndarray, np.ndarray]:
        if not os.path.exists(self.vector_file) or not os.path.exists(self.id_file):
            return np.array([]), np.array([])

        vectors = np.load(self.vector_file)
        ids = np.load(self.id_file)
        return vectors, ids

    def get_fingerprint_rows(
        self,
        ids: List[int],
    ) -> List[tuple[int, int, float]]:
        if not ids:
            return []

        unique_ids = [int(v) for v in sorted(set(ids))]
        placeholders = ",".join("?" for _ in unique_ids)
        sql = (
            "SELECT id, video_id, timestamp_seconds "
            f"FROM fingerprints WHERE id IN ({placeholders})"
        )

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(sql, unique_ids)
        rows = cur.fetchall()
        conn.close()

        return [(int(r[0]), int(r[1]), float(r[2])) for r in rows]

    def get_video_with_creator(
        self,
        video_id: int,
    ) -> tuple[int, str, str, str] | None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT videos.id, videos.url, videos.title, creators.name
            FROM videos
            JOIN creators ON creators.id = videos.creator_id
            WHERE videos.id = ?
            """,
            (int(video_id),),
        )
        row = cur.fetchone()
        conn.close()

        if row is None:
            return None

        return int(row[0]), str(row[1]), str(row[2]), str(row[3])
