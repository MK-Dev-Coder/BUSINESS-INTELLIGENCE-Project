from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .config import RAW_CAT_BREEDS_PATH, RAW_DOG_BREEDS_PATH, RAW_EVENTS_PATH, STAGING_DB


def init_staging(db_path: Path = STAGING_DB) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staging_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staging_dog_breeds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staging_cat_breeds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL
            )
            """
        )


def load_jsonl_to_table(db_path: Path, table: str, path: Path) -> None:
    if not path.exists():
        return
    with sqlite3.connect(db_path) as conn, path.open("r", encoding="utf-8") as handle:
        rows = [(line.strip(),) for line in handle if line.strip()]
        conn.executemany(f"INSERT INTO {table} (payload) VALUES (?)", rows)


def load_json_to_table(db_path: Path, table: str, path: Path) -> None:
    if not path.exists():
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        payload = [payload]
    rows = [(json.dumps(item, ensure_ascii=True),) for item in payload]
    with sqlite3.connect(db_path) as conn:
        conn.executemany(f"INSERT INTO {table} (payload) VALUES (?)", rows)


def load_staging(db_path: Path = STAGING_DB) -> None:
    init_staging(db_path)
    load_jsonl_to_table(db_path, "staging_events", RAW_EVENTS_PATH)
    load_json_to_table(db_path, "staging_dog_breeds", RAW_DOG_BREEDS_PATH)
    load_json_to_table(db_path, "staging_cat_breeds", RAW_CAT_BREEDS_PATH)
