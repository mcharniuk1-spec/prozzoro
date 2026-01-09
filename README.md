# Prozorro milk/dairy (CPV-filtered)

This repo downloads Prozorro tenders and extracts items strictly by CPV dairy categories:
15500000-3, 15510000-6, 15511000-3, 15511100-4, 15511210-8, 15512000-0, 15530000-2, 15540000-5, 15550000-8.

## How it works
- Reads incremental feed: /tenders (offset/limit)
- Since CPV is usually on item level, the script periodically fetches tender details:
  every SAMPLE_EVERY records (env var)
- Filters only items whose items[].classification.id matches allowed CPV subtree
- Saves per month into data/outputs/<run_id>/<YYYY-MM>/
  - dairy_lots.csv + .csv.gz (+ optional xlsx)
  - dairy_items.csv + .csv.gz
- Saves progress into data/state/checkpoint.json so next run resumes.

## Run locally
pip install -r requirements.txt
python src/prozorro_milk_fetch.py
