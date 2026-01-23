# Veterinary Clinic Business Intelligence System

A comprehensive Business Intelligence solution for veterinary clinics to analyze adverse drug events and support evidence-based prescribing decisions.

## 📋 Project Overview

This BI system helps veterinarians make informed decisions about drug prescriptions by providing insights from FDA's 15-year historical adverse event database. The system integrates multiple data sources, applies dimensional modeling, and provides interactive visualizations for decision support.

### Client
Veterinary clinic employing experienced veterinarians who need data-driven insights to avoid prescribing drugs that may cause adverse reactions in animals.

### Business Objectives
- Support safer prescribing decisions
- Identify high-risk active ingredients by breed
- Analyze reaction patterns and timing
- Provide breed-specific risk assessments
- Enable evidence-based veterinary medicine

## 🎯 Key Capabilities

The system answers critical business questions:

✓ **What are the most common reactions for every breed?**
✓ **What active ingredients most commonly cause side effects?**
✓ **How do size (weight) correlate with reactions and outcomes?**
✓ **How do gender and reproductive status affect outcomes?**
✓ **What is the geographic distribution of adverse events?**
✓ **How many days does it take for reactions to appear?**
✓ **What are the patterns by breeding groups and purposes?** (for dogs)

## 🏗️ System Architecture

### Data Sources
1. **FDA Animal & Veterinary Adverse Events API**
   - https://open.fda.gov/apis/animalandveterinary/event/
   - 15 years of adverse event data

2. **TheDogAPI**
   - https://api.thedogapi.com/v1/breeds
   - Dog breed information (groups, purposes)

3. **TheCatAPI**
   - https://api.thecatapi.com/v1/breeds
   - Cat breed information

### ETL Pipeline

```
┌─────────────┐      ┌─────────────┐      ┌──────────────┐
│   Extract   │ ───> │   Staging   │ ───> │  Warehouse   │
│             │      │             │      │              │
│ - FDA API   │      │ - Raw JSON  │      │ - Star       │
│ - Dog API   │      │ - Normalize │      │   Schema     │
│ - Cat API   │      │ - Validate  │      │ - Analytics  │
└─────────────┘      └─────────────┘      └──────────────┘
```

### Data Warehouse Design

**Star Schema** with the following structure:

**Fact Table:**
- `fact_event` - Adverse event facts

**Dimension Tables:**
- `dim_breed` - Animal breed information (with species, group, purpose)
- `dim_reaction` - Adverse reaction types
- `dim_outcome` - Event outcomes
- `dim_active_ingredient` - Active ingredients in medications
- `dim_geo` - Geographic locations (state, country)

**Bridge Tables:**
- `bridge_event_reaction` - Many-to-many: events to reactions
- `bridge_event_outcome` - Many-to-many: events to outcomes
- `bridge_event_ingredient` - Many-to-many: events to ingredients

## 🚀 Installation & Setup

### Prerequisites
- Python 3.10+
- API Keys (optional, but recommended):
  - Dog API key from https://thedogapi.com/

### Installation

1. Clone the repository
```bash
git clone <repository-url>
cd Project
```

2. Create virtual environment
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Set environment variables (optional)
```powershell
# PowerShell
$env:DOG_API_KEY="your_api_key_here"

# Command Prompt
set DOG_API_KEY=your_api_key_here
```

## 📊 Usage

### Run Complete ETL Pipeline

```bash
python main.py
```

This will:
1. Extract data from all sources
2. Load into staging database
3. Transform and load into data warehouse

### Command-Line Options

```bash
# Extract only
python main.py --extract-only

# Custom record limit
python main.py --limit 5000

# Adjust API throttling
python main.py --throttle 0.5

# Stage only (if raw data already extracted)
python main.py --stage-only

# Build warehouse only (if staging already complete)
python main.py --warehouse-only
```

### View Quick Results

```bash
python view_results.py
```

### Generate Business Reports

```bash
python generate_reports.py
```

Reports will be saved to `reports/` directory:
- Executive Summary
- Ingredient Risk Assessment

### Launch Interactive Dashboard

```bash
streamlit run dashboard.py
```

The dashboard provides:
- 📊 **Overview** - System statistics and key metrics
- 🐕 **Breed Analysis** - Breed-specific risk profiles
- 💊 **Ingredient Analysis** - High-risk active ingredients
- 📍 **Geographic Distribution** - Event distribution by location
- ⏱️ **Reaction Timing** - How quickly reactions appear
- 🎯 **Breeding Groups** - Analysis by dog breeding categories

## 📁 Project Structure

```
Project/
├── bi/
│   ├── __init__.py
│   ├── config.py          # Configuration and paths
│   ├── extract.py         # Data extraction from APIs
│   ├── staging.py         # Staging database operations
│   ├── warehouse.py       # Data warehouse operations
│   ├── analytics.py       # Business analytics queries
│   ├── reports.py         # Report generation
│   └── utils.py           # Utility functions
├── data/
│   ├── raw/              # Raw extracted data (JSON/JSONL)
│   ├── staging/          # Staging database
│   └── warehouse/        # Data warehouse database
├── reports/              # Generated business reports
├── main.py               # Main ETL pipeline
├── dashboard.py          # Interactive Streamlit dashboard
├── view_results.py       # Quick data viewer
├── generate_reports.py   # Report generation script
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## 🔍 Analytics Capabilities

### Breed Safety Profile
Query adverse event history for specific breeds including:
- Most common reactions
- Typical outcomes
- High-risk ingredients
- Event frequency

### Ingredient Risk Assessment
Identify active ingredients with:
- Highest adverse event counts
- Diversity of reactions caused
- Species-specific risks

### Correlation Analysis
- Weight vs. reaction severity
- Gender/reproductive status vs. outcomes
- Timing patterns for reaction onset
- Geographic risk patterns

### Breeding Category Analysis (Dogs)
- Risk patterns by breeding group (Herding, Hound, Toy, etc.)
- Analysis by breeding purpose (Guarding, Companion, etc.)

## 📈 Visualization Features

### Interactive Charts
- Bar charts for comparisons
- Pie charts for distributions
- Grouped bars for multi-dimensional analysis
- Sunburst diagrams for hierarchical data

### Filters & Selectors
- Breed-specific analysis
- Time period filtering
- Geographic filtering
- Ingredient lookup

### Export Capabilities
- Generate PDF reports
- Export data tables
- Save visualizations

## 🎓 Learning Outcomes Addressed

This project demonstrates:

- **LO1**: BI concepts and techniques (dimensional modeling, ETL)
- **LO2**: Decision support system framework
- **LO3**: Building, deploying, and managing BI systems
- **LO4**: Business and technical requirements analysis
- **LO5**: BI opportunities for performance management
- **LO6**: Strategic BI parameters and issues
- **LO7**: Data warehouses and databases
- **LO8**: Data integration and visualization
- **LO9**: BI application areas
- **LO10**: Python for BI purposes

## 🔧 Technical Stack

- **Language**: Python 3.10+
- **Database**: SQLite (staging and warehouse)
- **Visualization**: Streamlit, Plotly
- **Data Processing**: Pandas
- **APIs**: FDA openFDA, TheDogAPI, TheCatAPI

## 📝 Data Quality Notes

- Some FDA records contain masked (MSK) values for drug names/manufacturers
- Analysis focuses on active ingredients where available
- Not all events have complete weight, timing, or geographic data
- Breed information enriched from external APIs when possible

## 🚧 Known Limitations

1. **API Rate Limits**: FDA API has rate limiting (240 requests/minute)
2. **Data Completeness**: Not all events have complete information
3. **Breed Matching**: Breed names from FDA may not exactly match breed APIs
4. **Historical Data**: Patterns may evolve over time
5. **Causality**: System shows correlations, not proven causality

## 🔮 Future Enhancements

1. **Machine Learning Models**
   - Predict adverse event risk
   - Recommend safer alternatives

2. **Real-Time Monitoring**
   - Live API integration
   - Alert system for new patterns

3. **Integration**
   - Clinic management system integration
   - Electronic health records (EHR) connection

4. **Advanced Analytics**
   - Drug interaction analysis
   - Multi-drug risk assessment
   - Temporal trend analysis

5. **Collaboration Features**
   - Multi-user access
   - Shared annotations
   - Case studies

## 👥 Contributors

CCP6415 Business Intelligence Module Project

## 📄 License

Educational project - see course guidelines for usage restrictions.

## 🤝 Acknowledgments

- FDA openFDA project for adverse event data
- TheDogAPI for breed information
- TheCatAPI for cat breed data
