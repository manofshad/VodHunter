"""Bearer capabilities for anonymous searches; database IDs stay internal."""

import hashlib
import re
import secrets


_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{43}\Z")


def create_search_token() -> str:
    return secrets.token_urlsafe(32)


def is_search_token(value: object) -> bool:
    return isinstance(value, str) and _TOKEN_PATTERN.fullmatch(value) is not None


def hash_search_token(token: str) -> str:
    if not is_search_token(token):
        raise ValueError("Invalid search access token")
    return hashlib.sha256(token.encode("ascii")).hexdigest()
