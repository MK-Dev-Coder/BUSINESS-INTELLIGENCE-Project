import sqlite3
import os

DB_FILE = os.path.join("data", "processed", "warehouse.db")

def create_schema():
    print(f"Creating database schema in {DB_FILE}...")
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # 1. Events Table (Fact-like, but denormalized for animal info for simplicity)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        received_date DATE,
        onset_date DATE,
        days_to_reaction INTEGER,
        country TEXT,
        
        -- Animal Info
        species TEXT,
        breed TEXT,
        gender TEXT,
        reproductive_status TEXT,
        weight_kg REAL,
        age_years REAL,
        
        -- Outcome
        outcome TEXT
    );
    """)
    
    # 2. Drugs Table (Many-to-One with Events)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS drugs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT,
        active_ingredient TEXT,
        drug_name TEXT,
        route TEXT,
        dosage_form TEXT,
        FOREIGN KEY (event_id) REFERENCES events(event_id)
    );
    """)
    
    # 3. Reactions Table (Many-to-One with Events)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT,
        reaction_term TEXT,
        FOREIGN KEY (event_id) REFERENCES events(event_id)
    );
    """)
    
    # 4. Breeds Reference Table (Enriched data)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS breed_info (
        breed_name TEXT PRIMARY KEY,
        species TEXT,
        breeding_group TEXT,
        bred_for TEXT,
        temperament TEXT,
        origin TEXT
    );
    """)
    
    conn.commit()
    conn.close()
    print("Schema created successfully.")

if __name__ == "__main__":
    create_schema()
