"""
Inspect raw FDA data to understand structure
"""
import json
from pathlib import Path

raw_file = Path("data/raw/fda_events.jsonl")

if not raw_file.exists():
    print("❌ No raw data file found!")
    print(f"   Expected at: {raw_file}")
    print("\nRun: python main.py --extract-only")
    exit(1)

print("=" * 60)
print("INSPECTING RAW FDA EVENT DATA")
print("=" * 60)

with open(raw_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()
    print(f"\nTotal records: {len(lines)}")

    if lines:
        print("\n" + "=" * 60)
        print("FIRST RECORD STRUCTURE:")
        print("=" * 60)
        first_record = json.loads(lines[0])

        print("\nTop-level keys:")
        for key in first_record.keys():
            print(f"  - {key}")

        # Check for reactions
        print("\n--- Reactions ---")
        reactions = first_record.get('reaction', [])
        print(f"Number of reactions: {len(reactions)}")
        if reactions:
            print(f"First reaction: {reactions[0]}")

        # Check for outcomes
        print("\n--- Outcomes ---")
        outcomes = first_record.get('outcome', [])
        print(f"Number of outcomes: {len(outcomes)}")
        if outcomes:
            print(f"Outcomes: {outcomes}")

        # Check for drugs/ingredients
        print("\n--- Drugs ---")
        drugs = first_record.get('drug', [])
        print(f"Number of drugs: {len(drugs)}")
        if drugs:
            print(f"First drug keys: {drugs[0].keys() if isinstance(drugs[0], dict) else 'not a dict'}")
            if isinstance(drugs[0], dict):
                ingredients = drugs[0].get('active_ingredients', [])
                print(f"Active ingredients field: {ingredients}")

        # Check for animal info
        print("\n--- Animal Info ---")
        animal = first_record.get('animal', {})
        if animal:
            print(f"Animal keys: {animal.keys() if isinstance(animal, dict) else 'not a dict'}")
            if isinstance(animal, dict):
                print(f"  species: {animal.get('species')}")
                print(f"  breed: {animal.get('breed')}")
                print(f"  weight: {animal.get('weight')}")
                print(f"  weight_unit: {animal.get('weight_unit')}")

        print("\n" + "=" * 60)
        print("FULL FIRST RECORD (formatted):")
        print("=" * 60)
        print(json.dumps(first_record, indent=2)[:2000])
        print("...(truncated)")
