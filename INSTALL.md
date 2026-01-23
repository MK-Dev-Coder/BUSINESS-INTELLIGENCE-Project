# Installation Instructions

## Step 1: Install Required Packages

Open PowerShell in the project directory and run:

```powershell
.venv\Scripts\activate
pip install -r requirements.txt
```

## Step 2: Set API Key (if not already done)

```powershell
$env:DOG_API_KEY="live_ItHacPS4us1aWMdI8M5OZqMUCvgJjkuVxjpnzExEBjraJp9JUn2IiYp4lTQEkHYg"
```

## Step 3: Test the System

### Generate Reports
```powershell
python generate_reports.py
```

### Launch Dashboard
```powershell
streamlit run dashboard.py
```

The dashboard will open in your web browser automatically.

## Troubleshooting

If you get import errors, make sure:
1. Virtual environment is activated (`.venv\Scripts\activate`)
2. All packages are installed (`pip install -r requirements.txt`)
3. You're in the project root directory

## Next Steps

For the coursework, you now have:
- ✅ Complete ETL pipeline
- ✅ Data warehouse with star schema
- ✅ Analytics module with all required queries
- ✅ Interactive dashboard with visualizations
- ✅ Business report generation
- ✅ Complete documentation

You still need to complete:
- Task 1: Write the background, problem definition, and project scope document
- Task 4: Write the conclusion document
- Presentation materials
