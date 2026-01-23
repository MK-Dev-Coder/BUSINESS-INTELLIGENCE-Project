from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request


def http_get_json(url: str, params: dict | None = None, headers: dict | None = None) -> dict:
    if params:
        query = urllib.parse.urlencode(params)
        url = f"{url}?{query}"
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = resp.read().decode("utf-8")
    return json.loads(payload)


def request_headers(api_key_env: str | None) -> dict:
    if not api_key_env:
        return {"User-Agent": "bi-student-project/1.0"}
    key = os.getenv(api_key_env)
    headers = {"User-Agent": "bi-student-project/1.0"}
    if not key:
        return headers
    headers["x-api-key"] = key
    return headers


def throttle_sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)
