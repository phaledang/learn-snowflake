# Lab05 Developer Tasks & Shortcuts

Common helper commands for working in Lab05. Assumes you're in the `lab05` directory and the virtual environment is activated.

## Environment
```powershell
# Activate venv (Windows PowerShell)\.\venv\Scripts\Activate.ps1

# Install / sync dependencies
pip install -r python/requirements.txt
```

## Data Seeding
```powershell
# Seed 10k synthetic rows for 2024 with deterministic seed
python python/seed_sample_data.py --rows 10000 --year 2024 --seed 42
```

## Advanced Analysis
```powershell
# Default full analysis
python python/advanced_analysis.py

# Export CSV summaries + SVG figure
python python/advanced_analysis.py --export-csv --format svg --out-file analysis

# Persist aggregate tables back to Snowflake
python python/advanced_analysis.py --persist

# Custom prefix for tables + CSVs
python python/advanced_analysis.py --persist --persist-prefix DEMO_ --export-csv --export-prefix demo
```

## Pytest
```powershell
# Run all tests (offline portions run even without Snowflake)
pytest -q

# Run only visualization/export tests
pytest -k export -q

# Show skipped reasons
pytest -rs
```

## Debug Helpers
```powershell
# Validate environment variables are loaded
python python/diagnose_env_loading.py

# Validate Snowflake account normalization
python python/diagnose_snowflake.py

# Test connection & engine
python python/test_sqlalchemy_integration.py
```

## Cleanup
```powershell
# Remove generated CSVs & images (from current directory)
Remove-Item *.png,*.svg,*.csv -ErrorAction SilentlyContinue
```

## Suggested Next Enhancements
- Add a run script to chain: seed -> analysis -> persist -> tests
- Add a lightweight metadata logging table for analysis runs
- Parameterize seed script to optionally write multiple year ranges
