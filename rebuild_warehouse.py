"""
Rebuild the warehouse database from scratch
"""
from pathlib import Path
from bi.warehouse import build_warehouse

# Remove old warehouse
warehouse_path = Path("data/warehouse/bi_warehouse.db")
if warehouse_path.exists():
    print(f"Removing old warehouse: {warehouse_path}")
    warehouse_path.unlink()

print("Rebuilding warehouse with fixed extraction logic...")
build_warehouse()

print("\n✓ Warehouse rebuilt successfully!")
print("\nRun 'python check_data.py' to verify the data")
