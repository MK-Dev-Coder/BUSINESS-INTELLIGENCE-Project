# Veterinary BI System Project

## Overview
This project implements a Business Intelligence system to analyze adverse events in animals reported to the FDA. It integrates data from the FDA API and external breed APIs to provide insights for veterinarians.

## Project Structure
- `data/`: Contains raw, staging, and processed data.
    - `raw/`: JSON files fetched from APIs.
    - `processed/`: SQLite Data Warehouse (`warehouse.db`).
- `src/`: Source code.
    - `etl/`: Scripts for Extract, Transform, Load.
        - `extract_fda.py`: Fetches adverse event data from FDA.
        - `extract_dogs.py`: Fetches dog breed info.
        - `extract_cats.py`: Fetches cat breed info.
        - `create_dw.py`: Creates the SQLite schema.
        - `load_dw.py`: Loads data into the warehouse.
    - `analysis/`: Scripts for data analysis and visualization.
        - `analyze.py`: Generates reports and plots.
- `docs/`: Documentation and generated plots.

## System Design

### 1. Data Sources
- **FDA Animal & Veterinary Adverse Event Reports**: Primary source of adverse event data.
- **The Dog API**: Source for dog breed characteristics (Group, Purpose).
- **The Cat API**: Source for cat breed characteristics.

### 2. ETL Process
- **Extract**: Python scripts fetch data from APIs and save as JSON.
- **Transform**:
    - Data is parsed and normalized (dates, units).
    - Breeds are matched to groups (basic matching implemented).
- **Load**: Data is loaded into a Star Schema in SQLite.

### 3. Data Warehouse Schema
- **Fact Table**: `events` (Event details, animal info, outcome).
- **Dimension Tables**:
    - `drugs` (Active ingredients, dosage).
    - `reactions` (Reaction terms).
    - `breed_info` (Breed characteristics).

### 4. BI & Analysis
The `analyze.py` script performs SQL queries to answer key business questions:
- Common reactions by breed.
- Top active ingredients causing side effects.
- Correlations with size, gender, and reproductive status.
- Geographic distribution.
- Time to reaction analysis.

## How to Run
1. Install dependencies: `pip install -r requirements.txt`
2. Run extraction:
   ```bash
   python src/etl/extract_fda.py
   python src/etl/extract_dogs.py
   python src/etl/extract_cats.py
   ```
3. Create DW: `python src/etl/create_dw.py`
4. Load Data: `python src/etl/load_dw.py`
5. Run Analysis: `python src/analysis/analyze.py`

## Results
Plots and charts are generated in `docs/plots/`.
