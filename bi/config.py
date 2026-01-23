from __future__ import annotations

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
WAREHOUSE_DIR = DATA_DIR / "warehouse"

STAGING_DB = STAGING_DIR / "staging.db"
WAREHOUSE_DB = WAREHOUSE_DIR / "bi_warehouse.db"

OPENFDA_EVENTS_URL = "https://api.fda.gov/animalandveterinary/event.json"
DOG_BREEDS_URL = "https://api.thedogapi.com/v1/breeds"
CAT_BREEDS_URL = "https://api.thecatapi.com/v1/breeds"

RAW_EVENTS_PATH = RAW_DIR / "fda_events.jsonl"
RAW_DOG_BREEDS_PATH = RAW_DIR / "dog_breeds.json"
RAW_CAT_BREEDS_PATH = RAW_DIR / "cat_breeds.json"
