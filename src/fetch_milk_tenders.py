import requests
import csv
import time
import os
from datetime import datetime
from openpyxl import Workbook

BASE_URL = "https://public.api.openprocurement.org/api/2.5/tenders"
KEYWORD = "milk"
LIMIT = 100

LOG_PATH = "data/logs/run_log.txt"
CSV_PATH = "data/outputs/tenders_milk.csv"
XLSX_PATH = "data/outputs/tenders_milk.xlsx"


def log(message):
    os.makedirs("data/logs", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {message}\n")
    print(message)


def fetch_tenders(keyword=KEYWORD, limit=LIMIT):
    log(f"Starting fetch for keyword: {keyword}")
    os.makedirs("data/outputs", exist_ok=True)

    params = {"q": keyword, "limit": limit}
    next_url = BASE_URL
    all_records = []

    while next_url:
        resp = requests.get(next_url, params=params)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("data", [])
        all_records.extend(batch)
        log(f"Fetched {len(batch)} records (total: {len(all_records)})")

        next_page = data.get("next_page")
        if not next_page:
            break

        next_url = next_page["uri"]
        params = {}
        time.sleep(0.5)

    log(f"Completed fetch. Total records: {len(all_records)}")
    return all_records


def save_csv(records):
    if not records:
        log("No records to save to CSV")
        return

    keys = sorted(records[0].keys())

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    log(f"Saved CSV -> {CSV_PATH}")


def save_xlsx(records):
    if not records:
        log("No records to save to XLSX")
        return

    wb = Workbook()
    ws = wb.active
    ws.title = "milk_tenders"

    headers = sorted(records[0].keys())
    ws.append(headers)

    for r in records:
        ws.append([r.get(h, "") for h in headers])

    wb.save(XLSX_PATH)
    log(f"Saved XLSX -> {XLSX_PATH}")


if __name__ == "__main__":
    log("===== RUN STARTED =====")
    tenders = fetch_tenders()
    save_csv(tenders)
    save_xlsx(tenders)
    log("===== RUN FINISHED =====")
