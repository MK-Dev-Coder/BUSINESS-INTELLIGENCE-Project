"""
Check what data exists in the warehouse
"""
import sqlite3
from pathlib import Path

db_path = Path("data/warehouse/bi_warehouse.db")

if not db_path.exists():
    print("❌ Warehouse database does not exist!")
    print(f"   Expected at: {db_path}")
    print("\nRun: python main.py")
    exit(1)

with sqlite3.connect(db_path) as conn:
    conn.row_factory = sqlite3.Row

    print("=" * 60)
    print("DATABASE CONTENTS CHECK")
    print("=" * 60)

    # Check all tables
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()

    print("\nTables in database:")
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) as cnt FROM {table['name']}").fetchone()
        print(f"  {table['name']}: {count['cnt']} rows")

    print("\n" + "=" * 60)
    print("DETAILED CHECKS")
    print("=" * 60)

    # Check fact_event
    print("\nfact_event details:")
    event_stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(breed_key) as with_breed,
            COUNT(geo_key) as with_geo,
            COUNT(weight_kg) as with_weight,
            COUNT(days_to_reaction) as with_timing
        FROM fact_event
    """).fetchone()
    print(f"  Total events: {event_stats['total']}")
    print(f"  With breed: {event_stats['with_breed']}")
    print(f"  With geo: {event_stats['with_geo']}")
    print(f"  With weight: {event_stats['with_weight']}")
    print(f"  With timing: {event_stats['with_timing']}")

    # Check bridge tables
    print("\nBridge table counts:")
    reaction_count = conn.execute("SELECT COUNT(*) as cnt FROM bridge_event_reaction").fetchone()
    print(f"  Event-Reaction links: {reaction_count['cnt']}")

    outcome_count = conn.execute("SELECT COUNT(*) as cnt FROM bridge_event_outcome").fetchone()
    print(f"  Event-Outcome links: {outcome_count['cnt']}")

    ingredient_count = conn.execute("SELECT COUNT(*) as cnt FROM bridge_event_ingredient").fetchone()
    print(f"  Event-Ingredient links: {ingredient_count['cnt']}")

    # Sample some data
    print("\n" + "=" * 60)
    print("SAMPLE DATA")
    print("=" * 60)

    print("\nSample breeds:")
    breeds = conn.execute("SELECT * FROM dim_breed LIMIT 5").fetchall()
    for breed in breeds:
        print(f"  {breed['breed_name']} ({breed['species']}) - source: {breed['source']}")

    print("\nSample reactions:")
    reactions = conn.execute("SELECT * FROM dim_reaction LIMIT 5").fetchall()
    for reaction in reactions:
        print(f"  {reaction['reaction_name']}")

    print("\nSample ingredients:")
    ingredients = conn.execute("SELECT * FROM dim_active_ingredient LIMIT 5").fetchall()
    for ing in ingredients:
        print(f"  {ing['ingredient_name']}")

    print("\nSample outcomes:")
    outcomes = conn.execute("SELECT * FROM dim_outcome LIMIT 5").fetchall()
    for outcome in outcomes:
        print(f"  {outcome['outcome_name']}")

    print("\n" + "=" * 60)
    print("\nIf counts are zero or very low, you need to run the ETL pipeline:")
    print("  python main.py")
    print("=" * 60)
