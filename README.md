## Prozorro milk/dairy fetch (incremental)

### Run locally
pip install -r requirements.txt
python -m src.prozorro_milk_fetch

### What it does
- Reads /tenders feed from Prozorro Public API (no keys)
- Filters milk/dairy (CPV 155* + keywords)
- Fetches details for relevant statuses
- Stores deduplicated results in SQLite
- Exports monthly CSV.GZ + XLSX into data/outputs/YYYY-MM/
- Writes logs to data/logs/
- Saves progress offset in data/state/checkpoint.json

### GitHub Actions
Workflow runs daily and can be started manually (workflow_dispatch).
It stops gracefully before 6h limit and uploads artifacts.
