import json
import http.client
import math
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Lock
import time
from typing import Any, Optional


class TwitchRequestError(RuntimeError):
    """A request failure safe to include in worker diagnostics."""

    def __init__(
        self, *, operation: str, hostname: str, error_class: str,
        retryable: bool, retry_at: float | None = None, status: int | None = None,
    ) -> None:
        self.operation = operation
        self.hostname = hostname
        self.error_class = error_class
        self.retryable = retryable
        self.retry_at = retry_at
        self.status = status
        super().__init__(
            f"operation={operation} hostname={hostname} error_class={error_class} "
            f"status={status or 'none'} retryable={str(retryable).lower()}"
        )


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
        self._token_lock = Lock()
        self._user_profile_cache: dict[str, dict[str, Any]] = {}
        self._rate_limit_lock = Lock()
        self._rate_limits: dict[str, float] = {}

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

        data = self._request_json(req, operation="oauth_token")
        token = data.get("access_token")
        if not isinstance(token, str) or not token:
            raise TwitchRequestError(
                operation="oauth_token", hostname="id.twitch.tv",
                error_class="MissingAccessToken", retryable=True,
            )

        self._access_token = token
        return token

    def _ensure_token(self) -> str:
        with self._token_lock:
            if self._access_token:
                return self._access_token
            return self._request_access_token()

    def _refresh_rejected_token(self, rejected_token: str) -> str:
        with self._token_lock:
            if self._access_token and self._access_token != rejected_token:
                return self._access_token
            return self._request_access_token()

    def _helix_request(
        self,
        path: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = self._ensure_token()
        params = params or {}
        query = urllib.parse.urlencode(params, doseq=True)
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
            return self._request_json(req, operation=f"helix_{path}")

        try:
            return _call(token)
        except TwitchRequestError as exc:
            if exc.status == 401:
                fresh_token = self._refresh_rejected_token(token)
                return _call(fresh_token)
            else:
                raise

    def _request_json(self, req: urllib.request.Request, *, operation: str) -> dict[str, Any]:
        hostname = urllib.parse.urlparse(req.full_url).hostname or "unknown"
        with self._rate_limit_lock:
            retry_at = self._rate_limits.get(hostname, 0.0)
        if retry_at > time.time():
            raise TwitchRequestError(
                operation=operation, hostname=hostname, error_class="RateLimited",
                retryable=True, retry_at=retry_at, status=429,
            )
        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            retry_at = None
            if exc.code == 429:
                retry_at = self._rate_limit_deadline(exc.headers)
                with self._rate_limit_lock:
                    self._rate_limits[hostname] = max(
                        self._rate_limits.get(hostname, 0.0), retry_at,
                    )
                    retry_at = self._rate_limits[hostname]
            exc.close()
            raise TwitchRequestError(
                operation=operation, hostname=hostname, error_class="HTTPError",
                retryable=exc.code in {408, 429} or 500 <= exc.code < 600,
                retry_at=retry_at, status=exc.code,
            ) from None
        except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
            reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
            raise TwitchRequestError(
                operation=operation, hostname=hostname,
                error_class=type(reason).__name__, retryable=True,
            ) from None
        try:
            body = body.decode("utf-8")
            if not body.strip():
                return {}
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError("expected object")
            return payload
        except ValueError:
            raise TwitchRequestError(
                operation=operation, hostname=hostname,
                error_class="InvalidResponse", retryable=True,
            ) from None

    @staticmethod
    def _rate_limit_deadline(headers: object) -> float:
        now = time.time()
        deadlines = [now + 5.0]
        for name in ("Ratelimit-Reset", "Retry-After"):
            value = headers.get(name) if headers is not None else None
            if not value:
                continue
            try:
                parsed = float(value)
                if math.isfinite(parsed):
                    deadlines.append(parsed + (now if name == "Retry-After" else 0.0))
            except ValueError:
                if name == "Retry-After":
                    try:
                        deadlines.append(parsedate_to_datetime(value).timestamp())
                    except (TypeError, ValueError, OverflowError):
                        pass
        return max(deadlines)

    def _helix_get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        payload = self._helix_request(path=path, method="GET", params=params)
        if payload.get("error") or not isinstance(payload.get("data"), list):
            raise TwitchRequestError(
                operation=f"helix_{path}", hostname="api.twitch.tv",
                error_class="InvalidResponse", retryable=True,
            )
        return payload

    @staticmethod
    def parse_twitch_datetime(value: str) -> datetime:
        raw = (value or "").strip()
        if not raw:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return datetime.min.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    def is_live(self, streamer: str) -> bool:
        streamer = streamer.strip()
        if not streamer:
            raise ValueError("streamer is required")

        data = self._helix_get("streams", {"user_login": streamer})
        return bool(data.get("data"))

    def get_live_streamers(self, streamers: list[str]) -> set[str]:
        normalized = sorted(
            {streamer.strip().lower() for streamer in streamers if streamer.strip()}
        )
        if not normalized:
            return set()
        if len(normalized) > 100:
            raise ValueError("Twitch Get Streams accepts at most 100 streamers")

        data = self._helix_get("streams", {"user_login": normalized, "first": "100"})
        return {
            str(row.get("user_login") or "").strip().lower()
            for row in data.get("data") or []
            if str(row.get("user_login") or "").strip()
        }

    def get_user_id(self, streamer: str) -> str:
        profile = self.get_user_profile(streamer)
        return str(profile["id"])

    def get_user_profile(self, streamer: str, force_refresh: bool = False) -> dict[str, Any]:
        streamer = streamer.strip()
        if not streamer:
            raise ValueError("streamer is required")

        cached = None if force_refresh else self._user_profile_cache.get(streamer.lower())
        if cached is not None:
            return dict(cached)

        data = self._helix_get("users", {"login": streamer})
        rows = data.get("data") or []
        if not rows:
            raise TwitchRequestError(
                operation="helix_users", hostname="api.twitch.tv",
                error_class="StreamerNotFound", retryable=False,
            )

        raw_user = rows[0]
        user_id = str(raw_user.get("id", "")).strip()
        if not user_id:
            raise TwitchRequestError(
                operation="helix_users", hostname="api.twitch.tv",
                error_class="MissingUserId", retryable=True,
            )
        normalized = {
            "id": user_id,
            "login": str(raw_user.get("login", streamer)).strip().lower(),
            "display_name": str(raw_user.get("display_name", "")).strip() or str(raw_user.get("login", streamer)).strip(),
            "profile_image_url": self.normalize_image_url(raw_user.get("profile_image_url")),
        }
        self._user_profile_cache[normalized["login"]] = normalized
        return dict(normalized)

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
            return self.parse_twitch_datetime(str(v.get("created_at", "")))

        latest = max(rows, key=_created)
        return self.normalize_vod_metadata(latest)

    def list_archive_vods_since(self, user_id: str, created_after: datetime) -> list[dict[str, Any]]:
        user_id = user_id.strip()
        if not user_id:
            raise ValueError("user_id is required")

        if created_after.tzinfo is None:
            created_after = created_after.replace(tzinfo=timezone.utc)
        else:
            created_after = created_after.astimezone(timezone.utc)

        vods: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            params = {
                "user_id": user_id,
                "type": "archive",
                "first": "100",
            }
            if cursor:
                params["after"] = cursor

            payload = self._helix_get("videos", params)
            rows = payload.get("data") or []
            if not rows:
                break

            reached_older_vod = False
            for raw_vod in rows:
                created_at = self.parse_twitch_datetime(str(raw_vod.get("created_at", "")))
                if created_at < created_after:
                    reached_older_vod = True
                    continue
                normalized = self.normalize_vod_metadata(raw_vod)
                if normalized is not None:
                    vods.append(normalized)

            cursor = str((payload.get("pagination") or {}).get("cursor") or "").strip() or None
            if reached_older_vod or cursor is None:
                break

        vods.sort(
            key=lambda vod: self.parse_twitch_datetime(str(vod.get("created_at", ""))),
            reverse=True,
        )
        return vods

    def get_archive_vod_by_id(
        self, vod_platform_id: str, *, expected_user_id: str
    ) -> dict[str, Any] | None:
        vod_id = vod_platform_id.strip()
        if not vod_id or not expected_user_id.strip():
            raise ValueError("vod_platform_id and expected_user_id are required")
        payload = self._helix_get("videos", {"id": vod_id})
        if not isinstance(payload, dict) or payload.get("error") or not isinstance(payload.get("data"), list):
            raise RuntimeError(f"Invalid Twitch video response for {vod_id}")
        rows = payload["data"]
        if not rows:
            return None
        if len(rows) != 1 or not isinstance(rows[0], dict):
            raise RuntimeError(f"Invalid Twitch video rows for {vod_id}")
        raw_vod = rows[0]
        duration = raw_vod.get("duration")
        if (
            str(raw_vod.get("id") or "") != vod_id
            or str(raw_vod.get("user_id") or "") != expected_user_id.strip()
            or raw_vod.get("type") != "archive"
            or raw_vod.get("viewable") != "public"
            or not isinstance(duration, str)
            or re.fullmatch(r"(?:\d+h)?(?:\d+m)?(?:\d+s)?", duration) is None
        ):
            raise RuntimeError(f"Invalid Twitch archive metadata for {vod_id}")
        normalized = self.normalize_vod_metadata(raw_vod)
        if normalized is None or normalized["duration_seconds"] <= 0:
            raise RuntimeError(f"Invalid Twitch archive duration for {vod_id}")
        return normalized

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
    def normalize_image_url(image_url: Any) -> str | None:
        raw = str(image_url or "").strip()
        return raw or None

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


class CachedMultiStreamerTwitchMonitor:
    """Share one batched live-status result across streamer controllers."""

    def __init__(
        self,
        monitor: TwitchMonitor,
        streamers: list[str],
        *,
        cache_seconds: float = 15.0,
    ) -> None:
        self.monitor = monitor
        self.streamers = sorted(
            {streamer.strip().lower() for streamer in streamers if streamer.strip()}
        )
        if not self.streamers:
            raise ValueError("at least one streamer is required")
        if len(self.streamers) > 100:
            raise ValueError("one monitor supports at most 100 streamers")
        self.cache_seconds = max(0.0, float(cache_seconds))
        self._lock = Lock()
        self._live_streamers: set[str] = set()
        self._refreshed_at: float | None = None

    def is_live(self, streamer: str) -> bool:
        normalized = streamer.strip().lower()
        if normalized not in self.streamers:
            raise ValueError(f"streamer is not monitored: {normalized}")
        now = time.monotonic()
        with self._lock:
            if (
                self._refreshed_at is None
                or now - self._refreshed_at >= self.cache_seconds
            ):
                self._live_streamers = self.monitor.get_live_streamers(self.streamers)
                self._refreshed_at = time.monotonic()
            return normalized in self._live_streamers

    def __getattr__(self, name: str):
        return getattr(self.monitor, name)
