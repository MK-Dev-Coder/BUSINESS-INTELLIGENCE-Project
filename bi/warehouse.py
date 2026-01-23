from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .config import STAGING_DB, WAREHOUSE_DB


def init_warehouse(db_path: Path = WAREHOUSE_DB) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS dim_breed (
                breed_key INTEGER PRIMARY KEY AUTOINCREMENT,
                breed_name TEXT NOT NULL,
                species TEXT NOT NULL,
                group_name TEXT,
                purpose TEXT,
                source TEXT,
                UNIQUE (breed_name, species)
            );

            CREATE TABLE IF NOT EXISTS dim_reaction (
                reaction_key INTEGER PRIMARY KEY AUTOINCREMENT,
                reaction_name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS dim_outcome (
                outcome_key INTEGER PRIMARY KEY AUTOINCREMENT,
                outcome_name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS dim_active_ingredient (
                ingredient_key INTEGER PRIMARY KEY AUTOINCREMENT,
                ingredient_name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS dim_geo (
                geo_key INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT,
                country TEXT,
                UNIQUE (state, country)
            );

            CREATE TABLE IF NOT EXISTS fact_event (
                event_key INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id TEXT NOT NULL UNIQUE,
                breed_key INTEGER,
                geo_key INTEGER,
                species TEXT,
                sex TEXT,
                reproductive_status TEXT,
                weight_kg REAL,
                age_min REAL,
                days_to_reaction INTEGER,
                event_date TEXT,
                FOREIGN KEY (breed_key) REFERENCES dim_breed(breed_key),
                FOREIGN KEY (geo_key) REFERENCES dim_geo(geo_key)
            );

            CREATE TABLE IF NOT EXISTS bridge_event_reaction (
                event_key INTEGER NOT NULL,
                reaction_key INTEGER NOT NULL,
                PRIMARY KEY (event_key, reaction_key),
                FOREIGN KEY (event_key) REFERENCES fact_event(event_key),
                FOREIGN KEY (reaction_key) REFERENCES dim_reaction(reaction_key)
            );

            CREATE TABLE IF NOT EXISTS bridge_event_outcome (
                event_key INTEGER NOT NULL,
                outcome_key INTEGER NOT NULL,
                PRIMARY KEY (event_key, outcome_key),
                FOREIGN KEY (event_key) REFERENCES fact_event(event_key),
                FOREIGN KEY (outcome_key) REFERENCES dim_outcome(outcome_key)
            );

            CREATE TABLE IF NOT EXISTS bridge_event_ingredient (
                event_key INTEGER NOT NULL,
                ingredient_key INTEGER NOT NULL,
                PRIMARY KEY (event_key, ingredient_key),
                FOREIGN KEY (event_key) REFERENCES fact_event(event_key),
                FOREIGN KEY (ingredient_key) REFERENCES dim_active_ingredient(ingredient_key)
            );
            """
        )


def _get_or_create_dim(
    conn: sqlite3.Connection, table: str, key_col: str, unique_cols: dict
) -> int:
    columns = ", ".join(unique_cols.keys())
    placeholders = ", ".join("?" for _ in unique_cols)
    values = tuple(unique_cols.values())
    where_parts = []
    where_values: list[object] = []
    for col, val in unique_cols.items():
        if val is None:
            where_parts.append(f"{col} IS NULL")
        else:
            where_parts.append(f"{col} = ?")
            where_values.append(val)
    where_clause = " AND ".join(where_parts) if where_parts else "1=1"
    row = conn.execute(
        f"SELECT {key_col} FROM {table} WHERE {where_clause}", tuple(where_values)
    ).fetchone()
    if row:
        return int(row[0])
    cursor = conn.execute(
        f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})", values
    )
    if cursor.rowcount == 0:
        row = conn.execute(
            f"SELECT {key_col} FROM {table} WHERE {where_clause}", tuple(where_values)
        ).fetchone()
        if row:
            return int(row[0])
    return int(cursor.lastrowid)


def _normalize_weight(weight_value: str | dict | None, weight_unit: str | None) -> float | None:
    if weight_value is None or weight_value == "":
        return None
    unit = weight_unit
    value_raw = weight_value
    if isinstance(weight_value, dict):
        # Try 'min', 'max', 'value', or 'weight'
        value_raw = weight_value.get("min") or weight_value.get("max") or weight_value.get("value") or weight_value.get("weight")
        unit = unit or weight_value.get("unit")
    try:
        value = float(value_raw)
    except (TypeError, ValueError):
        return None
    if not unit:
        return value
    unit = str(unit).lower()
    if unit in {"lb", "lbs", "pound", "pounds"}:
        return value * 0.45359237
    return value


def _normalize_age(age_value: str | dict | None, age_unit: str | None) -> float | None:
    if age_value is None or age_value == "":
        return None
    unit = age_unit
    value_raw = age_value
    if isinstance(age_value, dict):
        value_raw = age_value.get("min") or age_value.get("max") or age_value.get("value") or age_value.get("age")
        unit = unit or age_value.get("unit")
    try:
        value = float(value_raw)
    except (TypeError, ValueError):
        return None
    if not unit:
        return value
    unit = str(unit).lower()
    if unit in {"year", "years"}:
        return value * 12
    if unit in {"day", "days"}:
        return value / 30
    return value


def _normalize_days_to_reaction(entry: dict) -> int | None:
    timing = entry.get("time_between_drug_administration_and_reaction")
    if not isinstance(timing, dict):
        return None
    value = timing.get("time_value")
    unit = (timing.get("time_unit") or "").lower()
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if unit in {"day", "days"}:
        return int(round(numeric))
    if unit in {"week", "weeks"}:
        return int(round(numeric * 7))
    if unit in {"month", "months"}:
        return int(round(numeric * 30))
    return None


def _extract_breed_name(animal: dict) -> str | None:
    breed = animal.get("breed")
    if isinstance(breed, str):
        return breed.strip() or None
    if isinstance(breed, dict):
        for key in ("breed_name", "breed_component", "name"):
            value = breed.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _extract_reactions(entry: dict) -> list[str]:
    reactions = []
    for item in entry.get("reaction", []) or []:
        if isinstance(item, dict):
            for key in ("veddra_term_name", "veddra_term", "reaction_name", "name"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    reactions.append(value.strip())
                    break
        elif isinstance(item, str):
            reactions.append(item.strip())
    return reactions


def _extract_outcomes(entry: dict) -> list[str]:
    outcomes = []
    for item in entry.get("outcome", []) or []:
        if isinstance(item, dict):
            for key in ("medical_status", "outcome", "outcome_name", "name"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    outcomes.append(value.strip())
                    break
        elif isinstance(item, str):
            outcomes.append(item.strip())
    return outcomes


def _extract_active_ingredients(entry: dict) -> list[str]:
    ingredients: list[str] = []
    for drug in entry.get("drug", []) or []:
        if not isinstance(drug, dict):
            continue
        # Try both 'active_ingredients' and 'active_ingredient'
        for ingredient in drug.get("active_ingredients", []) or drug.get("active_ingredient", []) or []:
            if isinstance(ingredient, dict):
                name = ingredient.get("name")
                if isinstance(name, str) and name.strip():
                    ingredients.append(name.strip())
            elif isinstance(ingredient, str):
                ingredients.append(ingredient.strip())
    return ingredients


def load_breeds_from_staging(staging_db: Path = STAGING_DB, warehouse_db: Path = WAREHOUSE_DB) -> None:
    with sqlite3.connect(staging_db) as stage_conn, sqlite3.connect(warehouse_db) as wh_conn:
        stage_conn.row_factory = sqlite3.Row
        for row in stage_conn.execute("SELECT payload FROM staging_dog_breeds"):
            payload = json.loads(row["payload"])
            breed_name = payload.get("name")
            if not breed_name:
                continue
            _get_or_create_dim(
                wh_conn,
                "dim_breed",
                "breed_key",
                {
                    "breed_name": breed_name,
                    "species": "dog",
                    "group_name": payload.get("breed_group"),
                    "purpose": payload.get("bred_for"),
                    "source": "thedogapi",
                },
            )
        for row in stage_conn.execute("SELECT payload FROM staging_cat_breeds"):
            payload = json.loads(row["payload"])
            breed_name = payload.get("name")
            if not breed_name:
                continue
            _get_or_create_dim(
                wh_conn,
                "dim_breed",
                "breed_key",
                {
                    "breed_name": breed_name,
                    "species": "cat",
                    "group_name": payload.get("breed_group"),
                    "purpose": payload.get("origin"),
                    "source": "thecatapi",
                },
            )
        wh_conn.commit()


def load_events_from_staging(staging_db: Path = STAGING_DB, warehouse_db: Path = WAREHOUSE_DB) -> None:
    with sqlite3.connect(staging_db) as stage_conn, sqlite3.connect(warehouse_db) as wh_conn:
        stage_conn.row_factory = sqlite3.Row
        for row in stage_conn.execute("SELECT payload FROM staging_events"):
            payload = json.loads(row["payload"])
            report_id = payload.get("unique_number") or payload.get("report_id")
            if not report_id:
                continue

            animal = payload.get("animal", {}) or {}
            species = animal.get("species")
            breed_name = _extract_breed_name(animal)
            weight_kg = _normalize_weight(animal.get("weight"), animal.get("weight_unit"))
            age_min = _normalize_age(animal.get("age"), animal.get("age_unit"))
            sex = animal.get("gender")
            reproductive_status = animal.get("reproductive_status")
            event_date = payload.get("original_receive_date")
            days_to_reaction = _normalize_days_to_reaction(payload)

            geo_key = _get_or_create_dim(
                wh_conn,
                "dim_geo",
                "geo_key",
                {"state": payload.get("state"), "country": payload.get("country")},
            )

            breed_key = None
            if breed_name and species:
                breed_key = _get_or_create_dim(
                    wh_conn,
                    "dim_breed",
                    "breed_key",
                    {
                        "breed_name": breed_name,
                        "species": species.lower(),
                        "group_name": None,
                        "purpose": None,
                        "source": "openfda",
                    },
                )

            cursor = wh_conn.execute(
                """
                INSERT OR IGNORE INTO fact_event (
                    report_id, breed_key, geo_key, species, sex, reproductive_status,
                    weight_kg, age_min, days_to_reaction, event_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_id,
                    breed_key,
                    geo_key,
                    species,
                    sex,
                    reproductive_status,
                    weight_kg,
                    age_min,
                    days_to_reaction,
                    event_date,
                ),
            )
            if cursor.rowcount == 0:
                event_key_row = wh_conn.execute(
                    "SELECT event_key FROM fact_event WHERE report_id = ?",
                    (report_id,),
                ).fetchone()
                if not event_key_row:
                    continue
                event_key = int(event_key_row[0])
            else:
                event_key = int(cursor.lastrowid)

            for reaction in _extract_reactions(payload):
                reaction_key = _get_or_create_dim(
                    wh_conn,
                    "dim_reaction",
                    "reaction_key",
                    {"reaction_name": reaction},
                )
                wh_conn.execute(
                    "INSERT OR IGNORE INTO bridge_event_reaction (event_key, reaction_key) VALUES (?, ?)",
                    (event_key, reaction_key),
                )

            for outcome in _extract_outcomes(payload):
                outcome_key = _get_or_create_dim(
                    wh_conn,
                    "dim_outcome",
                    "outcome_key",
                    {"outcome_name": outcome},
                )
                wh_conn.execute(
                    "INSERT OR IGNORE INTO bridge_event_outcome (event_key, outcome_key) VALUES (?, ?)",
                    (event_key, outcome_key),
                )

            for ingredient in _extract_active_ingredients(payload):
                ingredient_key = _get_or_create_dim(
                    wh_conn,
                    "dim_active_ingredient",
                    "ingredient_key",
                    {"ingredient_name": ingredient},
                )
                wh_conn.execute(
                    "INSERT OR IGNORE INTO bridge_event_ingredient (event_key, ingredient_key) VALUES (?, ?)",
                    (event_key, ingredient_key),
                )

        wh_conn.commit()


def build_warehouse(staging_db: Path = STAGING_DB, warehouse_db: Path = WAREHOUSE_DB) -> None:
    init_warehouse(warehouse_db)
    load_breeds_from_staging(staging_db, warehouse_db)
    load_events_from_staging(staging_db, warehouse_db)
