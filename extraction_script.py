import sqlite3
import pandas as pd
import os

# Configuration
DB_PATH = 'data/veterinary_dw.db'
OUTPUT_DIR = 'powerbi_data'

def export_tables_to_csv():
    # Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created directory: {OUTPUT_DIR}")

    try:
        # Connect to the database
        conn = sqlite3.connect(DB_PATH)

        # Get list of all tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        print(f"Found {len(tables)} tables. Starting export...")

        for table_name in tables:
            table = table_name[0]
            # Exclude internal SQLite tables
            if table.startswith('sqlite_'):
                continue

            # Read table into DataFrame
            query = f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, conn)

            # Export to CSV
            csv_path = os.path.join(OUTPUT_DIR, f"{table}.csv")
            df.to_csv(csv_path, index=False)
            print(f"Exported: {table} -> {csv_path}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    export_tables_to_csv()