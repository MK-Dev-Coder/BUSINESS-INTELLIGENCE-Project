$pythonPath = "C:/Users/mikek/AppData/Local/Programs/Python/Python312/python.exe"

Write-Host "1. Installing dependencies..."
& $pythonPath -m pip install -r requirements.txt

Write-Host "`n2. Running Extraction..."
& $pythonPath src/etl/extract_fda.py
& $pythonPath src/etl/extract_dogs.py
& $pythonPath src/etl/extract_cats.py

Write-Host "`n3. Creating Data Warehouse Schema..."
& $pythonPath src/etl/create_dw.py

Write-Host "`n4. Loading Data into Warehouse..."
& $pythonPath src/etl/load_dw.py

Write-Host "`n5. Running Analysis and Generating Plots..."
& $pythonPath src/analysis/analyze.py

Write-Host "`nDone! Check docs/plots/ for the results."
