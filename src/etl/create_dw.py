import sqlite3
import os

DB_FILE = os.path.join("data", "processed", "warehouse.db")

def create_flattened_schema():
    print(f"Creating Flattened Star Schema in {DB_FILE}...")
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    # 1. DIMENSIONS (Context)

    # Date Dimension
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_date (
        date_key INTEGER PRIMARY KEY,
        full_date DATE,
        year INTEGER,
        month INTEGER
    );
    """)

    # Animal Dimension
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_animal (
        animal_key INTEGER PRIMARY KEY AUTOINCREMENT,
        species TEXT,
        breed TEXT,
        gender TEXT,
        breeding_group TEXT,
        temperament TEXT
    );
    """)

    # Drug Dimension
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_drug (
        drug_key INTEGER PRIMARY KEY AUTOINCREMENT,
        active_ingredient TEXT,
        drug_name TEXT,
        route TEXT
    );
    """)

    # Reaction Dimension
    cursor.execute("""CREATE TABLE IF NOT EXISTS dim_reaction (
        reaction_key INTEGER PRIMARY KEY AUTOINCREMENT,
        reaction_term TEXT UNIQUE
    );
    """)

    # 2. FACT TABLE (The Granular Table)
    # GRAIN: 1 Row per Event per Drug per Reaction
    cursor.execute("""CREATE TABLE IF NOT EXISTS fact_analysis (
        fact_id INTEGER PRIMARY KEY AUTOINCREMENT,

        -- Natural Key
        event_id TEXT,

        drug_key INTEGER,
        reaction_key INTEGER,
        animal_key INTEGER,
        received_date_key INTEGER,

        -- Event-Level Metrics (Note: These are duplicated across rows)
        days_to_reaction INTEGER,
        weight_kg REAL,
        age_years REAL,
        outcome TEXT,

        FOREIGN KEY (drug_key) REFERENCES dim_drug(drug_key),
        FOREIGN KEY (reaction_key) REFERENCES dim_reaction(reaction_key),
        FOREIGN KEY (animal_key) REFERENCES dim_animal(animal_key),
        FOREIGN KEY (received_date_key) REFERENCES dim_date(date_key)
    );
    """)

    # Indexes for speed
    cursor.execute("CREATE INDEX idx_fact_drug ON fact_analysis(drug_key);")
    cursor.execute("CREATE INDEX idx_fact_reaction ON fact_analysis(reaction_key);")
    cursor.execute("CREATE INDEX idx_fact_animal ON fact_analysis(animal_key);")

    conn.commit()
    conn.close()
    print("Flattened Schema created successfully.")

if __name__ == "__main__":
    create_flattened_schema()