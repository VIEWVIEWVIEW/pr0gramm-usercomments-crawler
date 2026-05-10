"""Minimal pr0gramm login helper."""

from __future__ import annotations

import argparse
import base64
import json
import random
from dataclasses import dataclass
from pathlib import Path

import requests

from session_store import load_session_from_file, save_session_to_file


@dataclass
class LoginResult:
    ok: bool
    url: str
    status_code: int
    text: str

    def json(self) -> dict[str, object] | None:
        try:
            payload = json.loads(self.text)
        except json.JSONDecodeError:
            return None
        if isinstance(payload, dict):
            return payload
        return None


class Pr0grammLoginClient:
    def __init__(self, base_url: str = "https://pr0gramm.com/") -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.api_login_url = self.base_url.rstrip("/") + "/api/user/login"
        self.api_captcha_url = self.base_url.rstrip("/") + "/api/user/captcha"
        self.api_sync_url = self.base_url.rstrip("/") + "/api/user/sync"
        self.session = requests.Session()
        self._token: str | None = None
        self._headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "x-requested-with": "XMLHttpRequest",
            "referer": self.base_url,
        }

        response = self.session.get(self.base_url, timeout=30)
        response.raise_for_status()

    def save_session(self, path: str | Path = "pr0_session.json") -> Path:
        return save_session_to_file(self.session, path)

    def load_session(self, path: str | Path = "pr0_session.json") -> int:
        return load_session_from_file(self.session, path)

    def get_new_captcha(self) -> bytes:
        bust = random.random()
        response = self.session.get(
            self.api_captcha_url,
            params={"bust": bust},
            headers=self._headers,
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()
        token = payload.get("token")
        captcha_data_url = payload.get("captcha", "")
        if not token or not captcha_data_url:
            raise ValueError(f"Captcha response missing required fields: {payload!r}")

        prefix = "base64,"
        if prefix not in captcha_data_url:
            raise ValueError("Captcha payload is not a base64 data URL.")

        captcha_base64 = captcha_data_url.split(prefix, 1)[1]
        self._token = token
        return base64.b64decode(captcha_base64)

    def login(self, username: str, password: str, captchatext: str) -> LoginResult:
        if not self._token:
            raise RuntimeError("No captcha token available. Call get_new_captcha() first.")

        payload = {
            "name": username,
            "password": password,
            "captcha": captchatext,
            "token": self._token,
        }
        response = self.session.post(
            self.api_login_url,
            data=payload,
            headers={
                **self._headers,
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
            timeout=30,
        )
        response.raise_for_status()

        self._token = None
        return LoginResult(
            ok=response.ok,
            url=response.url,
            status_code=response.status_code,
            text=response.text,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="pr0gramm login helper")
    parser.add_argument(
        "--session-file",
        default="SESSION",
        help="Where to store cookies for later crawler reuse.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    client = Pr0grammLoginClient()
    captcha_bytes = client.get_new_captcha()
    with open("captcha.png", "wb") as f:
        f.write(captcha_bytes)

    print("Saved captcha to captcha.png")
    username = input("Username: ").strip()
    password = input("Password: ").strip()
    captcha_text = input("Captcha text: ").strip()

    result = client.login(username=username, password=password, captchatext=captcha_text)
    print(result.text)
    session_path = client.save_session(args.session_file)
    print(f"Saved session cookies to {session_path}")
