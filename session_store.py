"""Helpers to persist and restore requests.Session cookies."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests


def _cookie_to_dict(cookie: requests.cookies.Cookie) -> dict[str, Any]:
    return {
        "name": cookie.name,
        "value": cookie.value,
        "domain": cookie.domain,
        "path": cookie.path,
        "expires": cookie.expires,
        "secure": cookie.secure,
        "rest": dict(cookie._rest),
    }


def _cookie_from_dict(data: dict[str, Any]) -> requests.cookies.Cookie:
    return requests.cookies.create_cookie(
        name=str(data["name"]),
        value=str(data["value"]),
        domain=str(data.get("domain") or "pr0gramm.com"),
        path=str(data.get("path") or "/"),
        secure=bool(data.get("secure", False)),
        expires=data.get("expires"),
        rest=data.get("rest") or {},
    )


def save_session_to_file(session: requests.Session, path: str | Path) -> Path:
    out = Path(path)
    payload = {
        "format": "requests-session-cookies-v1",
        "cookies": [_cookie_to_dict(cookie) for cookie in session.cookies],
        "headers": {
            key: value
            for key, value in session.headers.items()
            if key.lower() in {"user-agent", "accept", "referer", "x-requested-with"}
        },
    }
    out.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return out


def load_session_from_file(session: requests.Session, path: str | Path) -> int:
    src = Path(path)
    payload = json.loads(src.read_text(encoding="utf-8"))
    cookies = payload.get("cookies", [])
    if not isinstance(cookies, list):
        raise ValueError(f"Invalid session file format in {src}")

    loaded = 0
    for raw_cookie in cookies:
        if not isinstance(raw_cookie, dict):
            continue
        session.cookies.set_cookie(_cookie_from_dict(raw_cookie))
        loaded += 1

    headers = payload.get("headers", {})
    if isinstance(headers, dict):
        for key, value in headers.items():
            if isinstance(key, str) and isinstance(value, str):
                session.headers[key] = value

    return loaded
