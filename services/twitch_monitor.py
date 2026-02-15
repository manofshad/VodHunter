import json
import os
import urllib.parse
import urllib.request
from typing import Optional


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

    def is_live(self, streamer: str) -> bool:
        streamer = streamer.strip()
        if not streamer:
            raise ValueError("streamer is required")

        token = self._ensure_token()
        query = urllib.parse.urlencode({"user_login": streamer})
        url = f"https://api.twitch.tv/helix/streams?{query}"

        def _call(token_value: str) -> urllib.request.urlopen:
            req = urllib.request.Request(
                url,
                headers={
                    "Client-ID": self.client_id,
                    "Authorization": f"Bearer {token_value}",
                },
                method="GET",
            )
            return urllib.request.urlopen(req, timeout=self.request_timeout)

        try:
            with _call(token) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            # App token expired or invalid, refresh once.
            if exc.code == 401:
                fresh_token = self._request_access_token()
                with _call(fresh_token) as resp:
                    body = resp.read().decode("utf-8")
            else:
                raise

        data = json.loads(body)
        return bool(data.get("data"))
