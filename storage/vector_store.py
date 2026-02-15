import os
import sqlite3
import numpy as np
from typing import List, Tuple


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
        Inserts fingerprint rows and returns their row IDs
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        ids: List[int] = []

        for ts in timestamps:
            cur.execute(
                "INSERT INTO fingerprints (video_id, timestamp_seconds) VALUES (?, ?)",
                (video_id, float(ts)),
            )
            ids.append(cur.lastrowid)

        conn.commit()
        conn.close()
        return ids

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
