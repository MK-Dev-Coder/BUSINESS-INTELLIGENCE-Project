import sqlite3
from pathlib import Path

db_path = Path("data/warehouse/bi_warehouse.db")

with sqlite3.connect(db_path) as conn:
    conn.row_factory = sqlite3.Row

    print("=== DATABASE TABLES ===")
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    for table in tables:
        print(f"  - {table['name']}")

    print("\n=== SAMPLE DATA ===\n")

    # Show breed counts
    print("Top 10 Breeds by Event Count:")
    breeds = conn.execute("""
        SELECT b.breed_name, b.species, COUNT(*) as event_count
        FROM fact_event e
        JOIN dim_breed b ON e.breed_key = b.breed_key
        GROUP BY b.breed_name, b.species
        ORDER BY event_count DESC
        LIMIT 10
    """).fetchall()
    for row in breeds:
        print(f"  {row['breed_name']} ({row['species']}): {row['event_count']} events")

    # Show reaction counts
    print("\nTop 10 Most Common Reactions:")
    reactions = conn.execute("""
        SELECT r.reaction_name, COUNT(*) as count
        FROM bridge_event_reaction br
        JOIN dim_reaction r ON br.reaction_key = r.reaction_key
        GROUP BY r.reaction_name
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()
    for row in reactions:
        print(f"  {row['reaction_name']}: {row['count']} occurrences")

    # Show total counts
    print("\n=== SUMMARY STATISTICS ===")
    stats = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM fact_event) as total_events,
            (SELECT COUNT(*) FROM dim_breed) as total_breeds,
            (SELECT COUNT(*) FROM dim_reaction) as total_reactions,
            (SELECT COUNT(*) FROM dim_outcome) as total_outcomes
    """).fetchone()
    print(f"  Total Events: {stats['total_events']}")
    print(f"  Total Breeds: {stats['total_breeds']}")
    print(f"  Total Reactions: {stats['total_reactions']}")
    print(f"  Total Outcomes: {stats['total_outcomes']}")
