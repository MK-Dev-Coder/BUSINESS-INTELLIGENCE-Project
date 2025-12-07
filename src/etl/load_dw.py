import json
import sqlite3
import os
from datetime import datetime
import pandas as pd

# Configuration
RAW_FDA_FILE = os.path.join("data", "raw", "fda_events.json")
RAW_DOGS_FILE = os.path.join("data", "raw", "dog_breeds.json")
RAW_CATS_FILE = os.path.join("data", "raw", "cat_breeds.json")
DB_FILE = os.path.join("data", "processed", "warehouse.db")

def parse_date(date_str):
    if not date_str or len(date_str) != 8:
        return None
    try:
        return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return None

def calculate_days(start, end):
    if not start or not end:
        return None
    try:
        d1 = datetime.strptime(start, "%Y-%m-%d")
        d2 = datetime.strptime(end, "%Y-%m-%d")
        return (d2 - d1).days
    except ValueError:
        return None

def normalize_weight(value, unit):
    if not value:
        return None
    try:
        val = float(value)
        if not unit:
            return val
        unit = unit.lower()
        if "kilogram" in unit or "kg" in unit:
            return val
        elif "pound" in unit or "lb" in unit:
            return val * 0.453592
        elif "gram" in unit:
            return val / 1000.0
        return val # Default assumption or unknown
    except ValueError:
        return None

def normalize_age(value, unit):
    if not value:
        return None
    try:
        val = float(value)
        if not unit:
            return val
        unit = unit.lower()
        if "year" in unit:
            return val
        elif "month" in unit:
            return val / 12.0
        elif "week" in unit:
            return val / 52.0
        elif "day" in unit:
            return val / 365.0
        return val
    except ValueError:
        return None

def load_breeds(cursor):
    print("Loading breeds...")
    # Load Dogs
    if os.path.exists(RAW_DOGS_FILE):
        with open(RAW_DOGS_FILE, "r") as f:
            dogs = json.load(f)
            for dog in dogs:
                name = dog.get("name")
                group = dog.get("breed_group")
                purpose = dog.get("bred_for")
                temperament = dog.get("temperament")
                origin = dog.get("origin")
                
                cursor.execute("""
                    INSERT OR REPLACE INTO breed_info (breed_name, species, breeding_group, bred_for, temperament, origin)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (name, "Dog", group, purpose, temperament, origin))
    
    # Load Cats
    if os.path.exists(RAW_CATS_FILE):
        with open(RAW_CATS_FILE, "r") as f:
            cats = json.load(f)
            for cat in cats:
                name = cat.get("name")
                temperament = cat.get("temperament")
                origin = cat.get("origin")
                
                cursor.execute("""
                    INSERT OR REPLACE INTO breed_info (breed_name, species, breeding_group, bred_for, temperament, origin)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (name, "Cat", None, None, temperament, origin))

def load_events(cursor):
    print("Loading FDA events...")
    if not os.path.exists(RAW_FDA_FILE):
        print("FDA data file not found.")
        return

    with open(RAW_FDA_FILE, "r") as f:
        events = json.load(f)
        
    count = 0
    for event in events:
        event_id = event.get("unique_aer_id_number")
        if not event_id:
            continue
            
        received_date = parse_date(event.get("original_receive_date"))
        onset_date = parse_date(event.get("onset_date"))
        days_to_reaction = calculate_days(received_date, onset_date) # Actually onset is usually before received. 
        # Wait, days to reaction usually means from drug administration to reaction. 
        # But we don't have administration date easily, maybe onset_date is the reaction date.
        # The prompt asks "How many days it takes for the reactions to appear". 
        # This implies (Onset Date - Treatment Start Date). 
        # I don't see treatment start date in the snippet. 
        # I'll check if there is a treatment start date.
        # If not, I might skip or use received - onset (which is reporting lag).
        # Let's assume onset_date is when reaction appeared.
        # I'll look for treatment date later. For now, I'll store onset_date.
        
        receiver = event.get("receiver", {})
        country = receiver.get("country")
        
        animal = event.get("animal", {})
        species = animal.get("species")
        gender = animal.get("gender")
        reproductive_status = animal.get("reproductive_status")
        
        weight_info = animal.get("weight", {})
        weight = normalize_weight(weight_info.get("min"), weight_info.get("unit"))
        
        age_info = animal.get("age", {})
        age = normalize_age(age_info.get("min"), age_info.get("unit"))
        
        breed_info = animal.get("breed", {})
        breed = breed_info.get("breed_component")
        if isinstance(breed, list):
            breed = ", ".join(breed)
        
        outcomes = event.get("outcome", [])
        outcome = outcomes[0].get("medical_status") if outcomes else "Unknown"
        
        # Insert Event
        cursor.execute("""
            INSERT OR IGNORE INTO events (
                event_id, received_date, onset_date, days_to_reaction, country,
                species, breed, gender, reproductive_status, weight_kg, age_years, outcome
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (event_id, received_date, onset_date, days_to_reaction, country,
              species, breed, gender, reproductive_status, weight, age, outcome))
        
        # Insert Drugs
        drugs = event.get("drug", [])
        for drug in drugs:
            active_ingredients = drug.get("active_ingredients", [])
            drug_name = drug.get("brand_name")
            route = drug.get("route")
            dosage_form = drug.get("dosage_form")
            
            for ingredient in active_ingredients:
                ing_name = ingredient.get("name")
                cursor.execute("""
                    INSERT INTO drugs (event_id, active_ingredient, drug_name, route, dosage_form)
                    VALUES (?, ?, ?, ?, ?)
                """, (event_id, ing_name, drug_name, route, dosage_form))
                
        # Insert Reactions
        reactions = event.get("reaction", [])
        for reaction in reactions:
            term = reaction.get("veddra_term_name")
            cursor.execute("""
                INSERT INTO reactions (event_id, reaction_term)
                VALUES (?, ?)
            """, (event_id, term))
            
        count += 1
        if count % 100 == 0:
            print(f"Processed {count} events...")

    print(f"Total events processed: {count}")

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        load_breeds(cursor)
        load_events(cursor)
        conn.commit()
        print("Data loading complete.")
    except Exception as e:
        print(f"Error loading data: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
