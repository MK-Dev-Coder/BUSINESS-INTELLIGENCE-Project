from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.error import HTTPError

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

try:
    from .config import (
        CAT_BREEDS_URL,
        DOG_BREEDS_URL,
        OPENFDA_EVENTS_URL,
        RAW_CAT_BREEDS_PATH,
        RAW_DOG_BREEDS_PATH,
        RAW_EVENTS_PATH,
    )
    from .utils import http_get_json, request_headers, throttle_sleep
except ImportError:
    from bi.config import (
        CAT_BREEDS_URL,
        DOG_BREEDS_URL,
        OPENFDA_EVENTS_URL,
        RAW_CAT_BREEDS_PATH,
        RAW_DOG_BREEDS_PATH,
        RAW_EVENTS_PATH,
    )
    from bi.utils import http_get_json, request_headers, throttle_sleep


def fetch_openfda_events(limit: int, page_size: int, throttle_seconds: float) -> list[dict]:
    results: list[dict] = []
    skip = 0
    headers = request_headers("OPENFDA_API_KEY")
    while len(results) < limit:
        batch_size = min(page_size, limit - len(results))
        payload = http_get_json(
            OPENFDA_EVENTS_URL,
            params={"limit": batch_size, "skip": skip},
            headers=headers,
        )
        batch = payload.get("results", [])
        if not batch:
            break
        results.extend(batch)
        skip += len(batch)
        throttle_sleep(throttle_seconds)
    return results


def fetch_breeds(url: str, api_key_env: str | None) -> list[dict]:
    headers = request_headers(api_key_env)
    try:
        payload = http_get_json(url, headers=headers)
    except HTTPError as exc:
        if exc.code == 403:
            print(
                f"Skipping breed source {url} due to HTTP 403. "
                "Set the appropriate API key env var if required."
            )
            return []
        raise
    if isinstance(payload, list):
        return payload
    return payload.get("results", [])


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)


def write_jsonl(path: Path, payload: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in payload:
            handle.write(json.dumps(record, ensure_ascii=True))
            handle.write("\n")


def extract_all(limit: int = 2000, page_size: int = 200, throttle_seconds: float = 0.25) -> None:
    events = fetch_openfda_events(limit=limit, page_size=page_size, throttle_seconds=throttle_seconds)
    write_jsonl(RAW_EVENTS_PATH, events)

    dog_breeds = fetch_breeds(DOG_BREEDS_URL, "DOG_API_KEY")
    write_json(RAW_DOG_BREEDS_PATH, dog_breeds)

    cat_breeds = fetch_breeds(CAT_BREEDS_URL, "CAT_API_KEY")
    write_json(RAW_CAT_BREEDS_PATH, cat_breeds)


if __name__ == "__main__":
    extract_all()
