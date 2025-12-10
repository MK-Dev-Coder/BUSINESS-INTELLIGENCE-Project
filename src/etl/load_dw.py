import json
import sqlite3
import os
from datetime import datetime

# Configuration
RAW_FDA_FILE = os.path.join("data", "raw", "fda_events.json")
RAW_DOGS_FILE = os.path.join("data", "raw", "dog_breeds.json")
RAW_CATS_FILE = os.path.join("data", "raw", "cat_breeds.json")
DB_FILE = os.path.join("data", "processed", "warehouse.db")

# --- Improved Helper Functions ---

def parse_date(date_str):
    """Parses YYYYMMDD format commonly found in FDA data."""
    if not date_str or len(str(date_str)) != 8:
        return None
    try:
        return datetime.strptime(str(date_str), "%Y%m%d")
    except ValueError:
        return None

def calculate_days(receive_date, onset_date):
    """
    Returns (Receive - Onset).
    If Onset is missing, we cannot calculate 'Days to Reaction' (Lag).
    If Onset > Receive, it might be a data error, but we return negative value.
    """
    if not receive_date or not onset_date:
        return None

    # Calculate difference
    delta = (receive_date - onset_date).days

    # Standard pharmacovigilance lag = Receive - Onset.
    return abs(delta) # Return absolute difference to avoid negatives if dates flipped

def normalize_weight(val, unit):
    if not val: return None
    try:
        v = float(val)
        if unit:
            u = unit.lower()
            if "lb" in u or "pound" in u: return v * 0.453592
            if "oz" in u or "ounce" in u: return v * 0.0283495
            if "g" == u or "gram" in u: return v / 1000.0
        return v # Default to KG
    except: return None

def normalize_age(val, unit):
    if not val: return None
    try:
        v = float(val)
        if unit:
            u = unit.lower()
            if "month" in u: return v / 12.0
            if "week" in u: return v / 52.0
            if "day" in u: return v / 365.0
        return v # Default to Years
    except: return None

def clean_breed_name(name):
    """Fixes 'Group - Breed' formatting to 'Breed Group'."""
    if not name: return "Unknown"
    name = str(name).strip()
    if " - " in name:
        parts = name.split(" - ")
        if len(parts) == 2:
            return f"{parts[1]} {parts[0]}".title()
    return name.title()

# --- Main Loader ---

def load_final_fixed():
    print(f"Reloading with fixes for Age and Dates in {DB_FILE}...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = OFF;")

    # 1. Load Breed Reference
    breed_ref = {}
    for fpath, species in [(RAW_DOGS_FILE, "Dog"), (RAW_CATS_FILE, "Cat")]:
        if os.path.exists(fpath):
            with open(fpath) as f:
                for item in json.load(f):
                    std_name = item.get("name", "").strip().lower()
                    breed_ref[(species, std_name)] = {
                        "group": item.get("breed_group"),
                        "temp": item.get("temperament")
                    }

    print("Processing Events...")
    if not os.path.exists(RAW_FDA_FILE): return

    with open(RAW_FDA_FILE, "r") as f:
        events = json.load(f)

    # Caches
    drug_cache = {}
    reaction_cache = {}

    # Clear old data
    cursor.execute("DELETE FROM fact_analysis")
    cursor.execute("DELETE FROM dim_animal")

    count = 0
    for event in events:
        event_id = event.get("unique_aer_id_number")
        if not event_id: continue

        # --- A. Date & Lag Logic ---
        rec_date = parse_date(event.get("original_receive_date"))
        onset_date = parse_date(event.get("onset_date"))

        date_key = None
        if rec_date:
            date_key = int(rec_date.strftime("%Y%m%d"))

            cursor.execute("""
                INSERT OR IGNORE INTO dim_date (date_key, full_date, year, month)
                VALUES (?, ?, ?, ?)
            """, (date_key, rec_date.strftime("%Y-%m-%d"), rec_date.year, rec_date.month))

        # Calculate lag
        days_to_react = calculate_days(rec_date, onset_date)

        # --- B. Animal & Age Logic ---
        animal = event.get("animal", {})
        species = animal.get("species", "Unknown")

        # Extract nested values
        age_obj = animal.get("age", {})
        age_val = normalize_age(age_obj.get("min"), age_obj.get("unit"))

        # Weight Fix
        w_obj = animal.get("weight", {})
        weight_val = normalize_weight(w_obj.get("min"), w_obj.get("unit"))

        # Breed Fix
        breed_raw = animal.get("breed", {}).get("breed_component", "Unknown")
        if isinstance(breed_raw, list): breed_raw = breed_raw[0]
        breed_clean = clean_breed_name(breed_raw)

        lookup = breed_ref.get((species, breed_clean.lower()), {})
        if not lookup: lookup = breed_ref.get((species, breed_raw.lower()), {})

        cursor.execute("""
            INSERT INTO dim_animal (species, breed, gender, breeding_group, temperament)
            VALUES (?, ?, ?, ?, ?)
        """, (species, breed_clean, animal.get("gender"), lookup.get("group"), lookup.get("temp")))
        animal_key = cursor.lastrowid

        # Outcome
        outcome_list = event.get("outcome", [])
        outcome = outcome_list[0].get("medical_status", "Unknown") if outcome_list else "Unknown"

        # --- C. Fact Table Explosion ---
        drugs = event.get("drug", [])
        reactions = event.get("reaction", [])

        if not drugs or not reactions: continue

        for drug in drugs:
            ing_list = [i.get("name") for i in drug.get("active_ingredients", [])]
            if not ing_list: ing_list = [drug.get("brand_name")]

            for ingredient in ing_list:
                if not ingredient: continue

                # Drug Dim
                if ingredient not in drug_cache:
                    cursor.execute("INSERT INTO dim_drug (active_ingredient, drug_name) VALUES (?, ?)",
                                  (ingredient, drug.get("brand_name")))
                    drug_cache[ingredient] = cursor.lastrowid
                d_key = drug_cache[ingredient]

                for reaction in reactions:
                    term = reaction.get("veddra_term_name")
                    if not term: continue

                    # Reaction Dim
                    if term not in reaction_cache:
                        cursor.execute("INSERT OR IGNORE INTO dim_reaction (reaction_term) VALUES (?)", (term,))
                        cursor.execute("SELECT reaction_key FROM dim_reaction WHERE reaction_term=?", (term,))
                        res = cursor.fetchone()
                        if res: reaction_cache[term] = res[0]
                    r_key = reaction_cache.get(term)

                    # Fact Insert with Fixed Metrics
                    if r_key and d_key:
                        cursor.execute("""
                            INSERT INTO fact_analysis
                            (event_id, drug_key, reaction_key, animal_key, received_date_key,
                             days_to_reaction, weight_kg, age_years, outcome)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (event_id, d_key, r_key, animal_key, date_key,
                              days_to_react, weight_val, age_val, outcome))

        count += 1
        if count % 2000 == 0:
            print(f"Processed {count} events...")
            conn.commit()

    conn.commit()
    conn.close()
    print(f"Done! {count} events loaded.")

if __name__ == "__main__":
    load_final_fixed()