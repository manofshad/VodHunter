import sqlite3


class SessionQueryService:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def list_live_sessions(self, limit: int, offset: int):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT videos.id, creators.name, videos.url, videos.title, videos.processed
            FROM videos
            JOIN creators ON creators.id = videos.creator_id
            WHERE videos.url LIKE 'https://twitch.tv/%' OR videos.url LIKE 'https://www.twitch.tv/%'
            ORDER BY videos.id DESC
            LIMIT ? OFFSET ?
            """,
            (int(limit), int(offset)),
        )
        rows = cur.fetchall()
        conn.close()

        return [
            {
                "video_id": int(r[0]),
                "creator_name": str(r[1]),
                "url": str(r[2]),
                "title": str(r[3]),
                "processed": bool(r[4]),
            }
            for r in rows
        ]
