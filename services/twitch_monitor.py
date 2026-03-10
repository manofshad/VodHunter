import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Optional


class TwitchMonitor:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        request_timeout: float = 10.0,
    ):
        if not client_id:
            raise ValueError("TWITCH_CLIENT_ID is required")
        if not client_secret:
            raise ValueError("TWITCH_CLIENT_SECRET is required")

        self.client_id = client_id
        self.client_secret = client_secret
        self.request_timeout = request_timeout
        self._access_token: Optional[str] = None

    @classmethod
    def from_env(cls) -> "TwitchMonitor":
        return cls(
            client_id=os.getenv("TWITCH_CLIENT_ID", "").strip(),
            client_secret=os.getenv("TWITCH_CLIENT_SECRET", "").strip(),
        )

    def _request_access_token(self) -> str:
        payload = urllib.parse.urlencode(
            {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            }
        ).encode("utf-8")

        req = urllib.request.Request(
            "https://id.twitch.tv/oauth2/token",
            data=payload,
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=self.request_timeout) as resp:
            body = resp.read().decode("utf-8")

        data = json.loads(body)
        token = data.get("access_token")
        if not token:
            raise RuntimeError("Failed to acquire Twitch access token")

        self._access_token = token
        return token

    def _ensure_token(self) -> str:
        if self._access_token:
            return self._access_token
        return self._request_access_token()

    def _helix_request(
        self,
        path: str,
        method: str = "GET",
        params: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = self._ensure_token()
        params = params or {}
        query = urllib.parse.urlencode(params)
        url = f"https://api.twitch.tv/helix/{path}"
        if query:
            url = f"{url}?{query}"

        def _call(token_value: str):
            payload = None
            headers = {
                "Client-ID": self.client_id,
                "Authorization": f"Bearer {token_value}",
            }
            if body is not None:
                payload = json.dumps(body).encode("utf-8")
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(
                url,
                headers=headers,
                data=payload,
                method=method,
            )
            return urllib.request.urlopen(req, timeout=self.request_timeout)

        try:
            with _call(token) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                fresh_token = self._request_access_token()
                with _call(fresh_token) as resp:
                    body = resp.read().decode("utf-8")
            else:
                raise

        if not body.strip():
            return {}
        return json.loads(body)

    def _helix_get(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        return self._helix_request(path=path, method="GET", params=params)

    def is_live(self, streamer: str) -> bool:
        streamer = streamer.strip()
        if not streamer:
            raise ValueError("streamer is required")

        data = self._helix_get("streams", {"user_login": streamer})
        return bool(data.get("data"))

    def get_user_id(self, streamer: str) -> str:
        streamer = streamer.strip()
        if not streamer:
            raise ValueError("streamer is required")

        data = self._helix_get("users", {"login": streamer})
        rows = data.get("data") or []
        if not rows:
            raise RuntimeError(f"Streamer not found: {streamer}")

        user_id = str(rows[0].get("id", "")).strip()
        if not user_id:
            raise RuntimeError(f"Missing user id for streamer: {streamer}")
        return user_id

    def get_latest_archive_vod(self, user_id: str) -> dict[str, Any] | None:
        user_id = user_id.strip()
        if not user_id:
            raise ValueError("user_id is required")

        data = self._helix_get(
            "videos",
            {
                "user_id": user_id,
                "type": "archive",
                "first": "10",
            },
        )
        rows = data.get("data") or []
        if not rows:
            return None

        def _created(v: dict[str, Any]) -> datetime:
            raw = str(v.get("created_at", "")).strip()
            if not raw:
                return datetime.min
            try:
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return datetime.min

        latest = max(rows, key=_created)
        return self.normalize_vod_metadata(latest)

    @classmethod
    def normalize_vod_metadata(cls, raw_vod: dict[str, Any]) -> dict[str, Any] | None:
        vod_id = str(raw_vod.get("id", "")).strip()
        if not vod_id:
            return None

        duration_raw = str(raw_vod.get("duration", "")).strip()
        return {
            "id": vod_id,
            "url": cls.canonical_vod_url(vod_id),
            "title": str(raw_vod.get("title", "Untitled stream")),
            "thumbnail_url": cls.normalize_thumbnail_url(raw_vod.get("thumbnail_url")),
            "created_at": str(raw_vod.get("created_at", "")),
            "duration": duration_raw,
            "duration_seconds": cls.parse_duration_to_seconds(duration_raw),
            "viewable": str(raw_vod.get("viewable", "public")),
        }

    @staticmethod
    def normalize_thumbnail_url(thumbnail_url: Any, width: int = 320, height: int = 180) -> str | None:
        raw = str(thumbnail_url or "").strip()
        if not raw:
            return None
        return raw.replace("%{width}", str(int(width))).replace("%{height}", str(int(height)))

    @staticmethod
    def canonical_vod_url(vod_id: str) -> str:
        return f"https://www.twitch.tv/videos/{vod_id.strip()}"

    @staticmethod
    def parse_duration_to_seconds(duration: str) -> int:
        duration = (duration or "").strip().lower()
        if not duration:
            return 0

        total = 0
        number = ""
        unit_seconds = {
            "h": 3600,
            "m": 60,
            "s": 1,
        }

        for ch in duration:
            if ch.isdigit():
                number += ch
                continue

            if ch in unit_seconds and number:
                total += int(number) * unit_seconds[ch]
                number = ""

        return total
