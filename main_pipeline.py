import pandas as pd
import numpy as np
import requests
import sqlite3
import json
import os
import difflib
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
from collections import Counter
import re
import sys

# ==========================================
# Configuration & Setup
# ==========================================
# Determine the directory where the script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
DB_PATH = os.path.join(DATA_DIR, 'veterinary_dw.db')

def setup_directories():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    print(f"Directories setup: {RAW_DIR}, {PROCESSED_DIR}")

# ==========================================
# Data Extraction
# ==========================================
def fetch_fda_data(limit=500, api_key=None):
    """
    Fetches adverse event data from the openFDA Animal & Veterinary API.
    """
    print("Fetching FDA Adverse Event data...")
    base_url = "https://api.fda.gov/animalandveterinary/event.json"
    
    # Query for the last 15 years
    start_date = (datetime.now() - timedelta(days=15*365)).strftime('%Y%m%d')
    end_date = datetime.now().strftime('%Y%m%d')
    
    params = {
        'search': f'original_receive_date:[{start_date} TO {end_date}]',
        'limit': limit
    }
    
    if api_key:
        params['api_key'] = api_key
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        results = data.get('results', [])
        print(f"Successfully fetched {len(results)} records from FDA API.")
        return results
    except Exception as e:
        print(f"Error fetching FDA data: {e}")
        return []

def fetch_dog_breeds(api_key=None):
    """
    Fetches dog breed classifications from TheDogAPI.
    """
    print("Fetching Dog Breed data...")
    url = "https://api.thedogapi.com/v1/breeds"
    headers = {'x-api-key': api_key} if api_key else {}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        breeds = response.json()
        print(f"Successfully fetched {len(breeds)} dog breeds.")
        return breeds
    except Exception as e:
        print(f"Error fetching dog breeds: {e}")
        return []

# ==========================================
# Transformation & Cleaning
# ==========================================
def clean_fda_data(df):
    """
    Cleans and flattens the raw FDA data.
    """
    # 1. Flatten 'animal' column
    if 'animal' in df.columns:
        animal_df = pd.json_normalize(df['animal'])
        df = pd.concat([df.drop(['animal'], axis=1), animal_df], axis=1)
    
    # 2. Flatten 'receiver' column for Geography
    if 'receiver' in df.columns:
        receiver_df = pd.json_normalize(df['receiver'])
        loc_cols = ['city', 'state', 'country', 'postal_code']
        for col in loc_cols:
            if col in receiver_df.columns:
                df[f'receiver_{col}'] = receiver_df[col]
            else:
                df[f'receiver_{col}'] = 'Unknown'
        df.drop(['receiver'], axis=1, inplace=True, errors='ignore')
    
    # 3. Handle Missing Values
    df.replace('MSK', np.nan, inplace=True)
    
    # 4. Standardize Dates
    df['event_date'] = pd.to_datetime(df['original_receive_date'], format='%Y%m%d', errors='coerce')
    
    # 5. Standardize Gender
    if 'gender' in df.columns:
        df['gender'] = df['gender'].fillna('Unknown').str.title()
        
    # 6. Standardize Reproductive Status
    if 'reproductive_status' in df.columns:
        df['reproductive_status'] = df['reproductive_status'].fillna('Unknown').str.title()
        
    return df

def normalize_drugs(df):
    """
    Explodes the 'drug' list and extracts active ingredients.
    """
    # Explode the list of drugs so each drug gets a row
    df_exploded = df.explode('drug')
    
    # Normalize the drug dictionary
    if 'drug' in df_exploded.columns and df_exploded['drug'].notna().any():
        drug_details = pd.json_normalize(df_exploded['drug'])
        # Reset index to align
        df_exploded = df_exploded.reset_index(drop=True)
        drug_details = drug_details.reset_index(drop=True)
        df_combined = pd.concat([df_exploded.drop(['drug'], axis=1), drug_details], axis=1)
    else:
        df_combined = df_exploded

    def extract_ingredients(ingredients_list):
        if isinstance(ingredients_list, list):
            names = [item.get('name', '') for item in ingredients_list if 'name' in item]
            return ", ".join(names)
        return "Unknown"

    if 'active_ingredients' in df_combined.columns:
        df_combined['active_ingredient_normalized'] = df_combined['active_ingredients'].apply(extract_ingredients)
    else:
        df_combined['active_ingredient_normalized'] = "Unknown"
        
    return df_combined

def smart_breed_match(fda_breed, api_breed_list):
    """
    Matches FDA breed strings to standardized API breed names using swap logic and fuzzy matching.
    """
    if not isinstance(fda_breed, str):
        return None
        
    # Strategy 1: Check for hyphenated swap (e.g. "Retriever - Labrador" -> "Labrador Retriever")
    if '-' in fda_breed:
        parts = [p.strip() for p in fda_breed.split('-')]
        if len(parts) == 2:
            swapped = f"{parts[1]} {parts[0]}"
            if swapped in api_breed_list:
                return swapped
    
    # Strategy 2: Fuzzy Match
    matches = difflib.get_close_matches(fda_breed, api_breed_list, n=1, cutoff=0.6)
    return matches[0] if matches else None

def enrich_data(df, dog_breeds_data):
    """
    Enriches FDA data with Dog Breed Group information.
    """
    df_dog_lookup = pd.DataFrame(dog_breeds_data)
    if not df_dog_lookup.empty:
        df_dog_lookup = df_dog_lookup[['name', 'breed_group', 'bred_for']].copy()
        df_dog_lookup['name'] = df_dog_lookup['name'].str.lower()
        valid_dog_breeds = df_dog_lookup['name'].unique().tolist()
        
        if 'breed.breed_component' in df.columns:
            df['breed_lower'] = df['breed.breed_component'].astype(str).str.lower()
            
            # Create Mapping Dictionary
            unique_fda_breeds = df['breed_lower'].unique()
            breed_map = {}
            
            print(f"Performing smart matching on {len(unique_fda_breeds)} unique breeds...")
            for breed in unique_fda_breeds:
                match = smart_breed_match(breed, valid_dog_breeds)
                breed_map[breed] = match if match else "Unknown"
            
            df['clean_breed_name'] = df['breed_lower'].map(breed_map)
            
            # Merge
            df_enriched = pd.merge(
                df, 
                df_dog_lookup, 
                left_on='clean_breed_name', 
                right_on='name', 
                how='left'
            )
            
            df_enriched['breed_group'] = df_enriched['breed_group'].fillna('Unknown')
            df_enriched['bred_for'] = df_enriched['bred_for'].fillna('Unknown')
            return df_enriched
            
    # Fallback if enrichment fails
    print("Warning: Skipping enrichment (missing data or column).")
    df['breed_group'] = 'Unknown'
    df['bred_for'] = 'Unknown'
    return df

# ==========================================
# Data Warehousing (Schema & Load)
# ==========================================
def create_schema(conn):
    """
    Creates the Star Schema in SQLite.
    """
    cursor = conn.cursor()
    print("Creating Star Schema...")
    
    cursor.executescript("""
    DROP TABLE IF EXISTS FactAdverseEvents;
    DROP TABLE IF EXISTS DimTime;
    DROP TABLE IF EXISTS DimAnimal;
    DROP TABLE IF EXISTS DimDrug;
    DROP TABLE IF EXISTS DimReaction;
    DROP TABLE IF EXISTS DimGeography;
    DROP TABLE IF EXISTS DimOutcome;

    -- Enhanced Time Dimension
    CREATE TABLE DimTime (
        TimeKey INTEGER PRIMARY KEY AUTOINCREMENT,
        Date DATE UNIQUE,
        Year INTEGER,
        Month INTEGER,
        Day INTEGER,
        Quarter INTEGER,
        DayOfWeek TEXT,
        IsWeekend BOOLEAN
    );

    -- Geography Dimension
    CREATE TABLE DimGeography (
        GeographyKey INTEGER PRIMARY KEY AUTOINCREMENT,
        City TEXT,
        State TEXT,
        Country TEXT,
        PostalCode TEXT
    );

    -- Outcome Dimension
    CREATE TABLE DimOutcome (
        OutcomeKey INTEGER PRIMARY KEY AUTOINCREMENT,
        OutcomeName TEXT UNIQUE,
        SeverityLevel TEXT
    );

    -- Animal Dimension
    CREATE TABLE DimAnimal (
        AnimalKey INTEGER PRIMARY KEY AUTOINCREMENT,
        Species TEXT,
        Breed TEXT,
        Gender TEXT,
        ReproductiveStatus TEXT,
        BreedGroup TEXT,
        BreedPurpose TEXT
    );

    -- Drug Dimension
    CREATE TABLE DimDrug (
        DrugKey INTEGER PRIMARY KEY AUTOINCREMENT,
        BrandName TEXT,
        ActiveIngredients TEXT,
        Manufacturer TEXT
    );

    -- Reaction Dimension
    CREATE TABLE DimReaction (
        ReactionKey INTEGER PRIMARY KEY AUTOINCREMENT,
        ReactionName TEXT UNIQUE
    );

    -- Fact Table
    CREATE TABLE FactAdverseEvents (
        EventID INTEGER PRIMARY KEY AUTOINCREMENT,
        FDA_ReportID TEXT,
        TimeKey INTEGER,
        AnimalKey INTEGER,
        DrugKey INTEGER,
        ReactionKey INTEGER,
        GeographyKey INTEGER,
        OutcomeKey INTEGER,
        Age FLOAT,
        Weight FLOAT,
        DaysToOnset INTEGER,
        ReactionCount INTEGER DEFAULT 1, 
        FOREIGN KEY(TimeKey) REFERENCES DimTime(TimeKey),
        FOREIGN KEY(AnimalKey) REFERENCES DimAnimal(AnimalKey),
        FOREIGN KEY(DrugKey) REFERENCES DimDrug(DrugKey),
        FOREIGN KEY(ReactionKey) REFERENCES DimReaction(ReactionKey),
        FOREIGN KEY(GeographyKey) REFERENCES DimGeography(GeographyKey),
        FOREIGN KEY(OutcomeKey) REFERENCES DimOutcome(OutcomeKey)
    );

    -- Indexes
    CREATE INDEX idx_fact_time ON FactAdverseEvents(TimeKey);
    CREATE INDEX idx_fact_animal ON FactAdverseEvents(AnimalKey);
    CREATE INDEX idx_fact_drug ON FactAdverseEvents(DrugKey);
    CREATE INDEX idx_fact_geo ON FactAdverseEvents(GeographyKey);
    """)
    conn.commit()
    print("Schema created successfully.")

def populate_time_dimension(conn, start_year=2000, end_year=2025):
    """
    Pre-populates the Time Dimension.
    """
    print("Populating DimTime...")
    cursor = conn.cursor()
    
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    current_date = start_date
    
    batch_data = []
    while current_date <= end_date:
        is_weekend = current_date.weekday() >= 5 # 5=Sat, 6=Sun
        batch_data.append((
            str(current_date.date()), # Date string
            current_date.year,
            current_date.month,
            current_date.day,
            (current_date.month - 1) // 3 + 1, # Quarter
            current_date.strftime('%A'), # DayOfWeek
            is_weekend
        ))
        current_date += timedelta(days=1)
    
    cursor.executemany("""
        INSERT OR IGNORE INTO DimTime 
        (Date, Year, Month, Day, Quarter, DayOfWeek, IsWeekend)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, batch_data)
    
    conn.commit()
    print("DimTime populated.")

def get_or_create_key(cursor, table, search_cols, values, key_col):
    where_clause = " AND ".join([f"{col} = ?" for col in search_cols])
    query = f"SELECT {key_col} FROM {table} WHERE {where_clause}"
    cursor.execute(query, values)
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        cols = ", ".join(search_cols)
        placeholders = ", ".join(["?" for _ in search_cols])
        insert_sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        cursor.execute(insert_sql, values)
        return cursor.lastrowid

def load_data_to_warehouse(conn, df_enriched):
    """
    Loads data into dimensions and facts.
    """
    print("Loading Fact Table... this may take a moment.")
    cursor = conn.cursor()
    count = 0
    
    # Pre-populate DimReaction keys for efficiency (optional, but good practice from notebook)
    # Note: We will handle inline to ensure coverage
    
    for index, row in df_enriched.iterrows():
        try:
            # 1. Time Key
            event_date = row['event_date']
            if pd.isnull(event_date):
                continue
            dt = pd.to_datetime(event_date)
            cursor.execute("SELECT TimeKey FROM DimTime WHERE Date = ?", (str(dt.date()),))
            time_res = cursor.fetchone()
            
            if time_res:
                time_key = time_res[0]
            else:
                # Fallback insert if time dimension missing this date (though we pre-populated)
                pass # Should handle or skip
                continue
            
            # 2. Animal Key
            species = row.get('species', 'Unknown')
            breed = row.get('breed.breed_component', 'Unknown')
            gender = row.get('gender', 'Unknown')
            repro_status = row.get('reproductive_status', 'Unknown')
            breed_group = row.get('breed_group', 'Unknown')
            breed_purpose = row.get('bred_for', 'Unknown')
            
            animal_key = get_or_create_key(
                cursor, 'DimAnimal', 
                ['Species', 'Breed', 'Gender', 'ReproductiveStatus', 'BreedGroup', 'BreedPurpose'],
                (str(species), str(breed), str(gender), str(repro_status), str(breed_group), str(breed_purpose)),
                'AnimalKey'
            )
            
            # 3. Drug Key
            brand = row.get('brand_name', 'Unknown')
            active_ing = row.get('active_ingredient_normalized', 'Unknown')
            manufacturer = row.get('manufacturer.name', 'Unknown')
            
            drug_key = get_or_create_key(
                cursor, 'DimDrug',
                ['BrandName', 'ActiveIngredients', 'Manufacturer'],
                (str(brand), str(active_ing), str(manufacturer)),
                'DrugKey'
            )
            
            # 4. Geography Key
            city = row.get('receiver_city', 'Unknown')
            state = row.get('receiver_state', 'Unknown')
            country = row.get('receiver_country', 'Unknown')
            postal = row.get('receiver_postal_code', 'Unknown')
            
            geo_key = get_or_create_key(
                cursor, 'DimGeography',
                ['City', 'State', 'Country', 'PostalCode'],
                (str(city), str(state), str(country), str(postal)),
                'GeographyKey'
            )
            
            # 5. Outcome Key
            outcome_raw = row.get('outcome', [])
            outcome_name = "Unknown"
            if isinstance(outcome_raw, list) and len(outcome_raw) > 0:
                 outcome_name = outcome_raw[0].get('medical_status', 'Unknown')
            elif isinstance(outcome_raw, str):
                outcome_name = outcome_raw
                
            severity = "Normal"
            if str(outcome_name).lower() in ['died', 'euthanized']:
                severity = "Critical"
                
            outcome_key = get_or_create_key(
                cursor, 'DimOutcome',
                ['OutcomeName', 'SeverityLevel'],
                (str(outcome_name), severity),
                'OutcomeKey'
            )

            # 6. Reaction Key & Fact Insertion
            reactions_list = row.get('reaction', [])
            if isinstance(reactions_list, list):
                for r in reactions_list:
                    # FIX: Use 'veddra_term_name' as per user request
                    r_name = r.get('veddra_term_name', 'Unknown')
                    
                    # Get Reaction Key
                    cursor.execute("SELECT ReactionKey FROM DimReaction WHERE ReactionName = ?", (r_name,))
                    res = cursor.fetchone()
                    if not res:
                        cursor.execute("INSERT INTO DimReaction (ReactionName) VALUES (?)", (r_name,))
                        reaction_key = cursor.lastrowid
                    else:
                        reaction_key = res[0]
                    
                    # Insert Fact
                    fda_id = row.get('report_id', 'Unknown')
                    age = row.get('age.min', 0)
                    weight = row.get('weight.min', 0)
                    
                    # DaysToOnset
                    days_to_onset = None
                    if 'onset_date' in row and pd.notnull(row['onset_date']):
                        try:
                            onset_dt = pd.to_datetime(row['onset_date'], format='%Y%m%d', errors='coerce')
                            if pd.notnull(onset_dt):
                                days_to_onset = (onset_dt - dt).days
                        except:
                            pass
                    
                    cursor.execute("""
                        INSERT INTO FactAdverseEvents 
                        (FDA_ReportID, TimeKey, AnimalKey, DrugKey, ReactionKey, GeographyKey, OutcomeKey, Age, Weight, DaysToOnset)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (str(fda_id), time_key, animal_key, drug_key, reaction_key, geo_key, outcome_key, age, weight, days_to_onset))
                    
            count += 1
            if count % 100 == 0:
                conn.commit()
                
        except Exception as e:
            # print(f"Skipping row due to error: {e}")
            continue

    conn.commit()
    print(f"Finished loading Fact Table. Processed {count} events.")

# ==========================================
# Text Analytics
# ==========================================
def run_text_analytics(conn):
    print("\nStarting Text Analytics (Word Cloud)...")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT ReactionName FROM DimReaction")
        reactions = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching reactions: {e}")
        return

    text_data = " ".join([str(r[0]) for r in reactions if r[0]]).lower()
    
    if not text_data.strip():
        print("No data for Text Analytics.")
        return

    stop_words = set(STOPWORDS)
    stop_words.update(["nos", "specified", "unknown", "animal", "veterinary", "clinical", 
                      "see", "saw", "site", "application", "administration"])

    filtered_words = [w for w in re.findall(r'\w+', text_data) if w not in stop_words and len(w) > 2]

    if not filtered_words:
        print("Not enough valid words for analysis.")
    else:
        print(f"Generating Word Cloud from {len(reactions)} reactions...")
        try:
            wordcloud = WordCloud(width=800, height=400, 
                                background_color='white', 
                                stopwords=stop_words, 
                                collocations=True,
                                min_font_size=10).generate(text_data)

            # Display
            plt.figure(figsize=(10, 5), facecolor=None)
            # FIX: Use .to_image() to avoid numpy compatibility issue
            plt.imshow(wordcloud.to_image())
            plt.axis("off")
            plt.tight_layout(pad=0)
            plt.title("Most Common Adverse Reactions (Word Cloud)")
            print("Displaying Word Cloud... (Close window to continue)")
            plt.show()

            # Frequency
            word_counts = Counter(filtered_words)
            print("\n--- Top 10 Most Frequent Terms ---")
            for word, count in word_counts.most_common(10):
                print(f"{word.title()}: {count}")
                
        except Exception as e:
            print(f"Word Cloud Error: {e}")

# ==========================================
# Main Execution
# ==========================================
def main():
    print("=== Veterinary BI Pipeline Started ===")
    setup_directories()
    
    # 1. Extraction
    fda_data = fetch_fda_data(limit=500)
    dog_data = fetch_dog_breeds()
    
    # Save raw data (optional, preserves original notebook logic)
    with open(os.path.join(RAW_DIR, 'fda_adverse_events.json'), 'w') as f:
        json.dump(fda_data, f)
    with open(os.path.join(RAW_DIR, 'dog_breeds.json'), 'w') as f:
        json.dump(dog_data, f)

    # 2. Staging & Cleaning
    df_fda = pd.DataFrame(fda_data)
    df_fda_clean = clean_fda_data(df_fda)
    
    # 3. Transformation
    df_drugs = normalize_drugs(df_fda_clean)
    df_enriched = enrich_data(df_drugs, dog_data)
    
    # 4. Loading
    conn = sqlite3.connect(DB_PATH)
    try:
        create_schema(conn)
        populate_time_dimension(conn)
        load_data_to_warehouse(conn, df_enriched)
        
        # 5. Validation
        print("\n--- Geographic Distribution Check ---")
        print(pd.read_sql_query("SELECT Country, COUNT(*) as Cnt FROM FactAdverseEvents f JOIN DimGeography g ON f.GeographyKey=g.GeographyKey GROUP BY Country LIMIT 5", conn))
        
        # 6. Text Analytics
        run_text_analytics(conn)
        
    finally:
        conn.close()
        print("\n=== Pipeline Execution Complete ===")

if __name__ == '__main__':
    main()
