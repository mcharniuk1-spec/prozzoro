from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from pathlib import Path

from openpyxl import Workbook

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "data/outputs"))
MONTHLY_DIR = OUTPUT_DIR / "monthly"

XLSX_OUT = OUTPUT_DIR / "prozorro_dairy_monthly.xlsx"
MAX_ROWS_PER_SHEET = int(os.getenv("MAX_ROWS_PER_SHEET", "900000"))  # Excel limit ~1,048,576


def write_sheet_from_csv(wb: Workbook, sheet_name: str, csv_path: Path) -> None:
    ws = wb.create_sheet(title=sheet_name)
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader, start=1):
            if i > MAX_ROWS_PER_SHEET:
                break
            ws.append(row)


def main() -> None:
    files = sorted(MONTHLY_DIR.glob("prozorro_dairy_items_*.csv"))
    if not files:
        print("No monthly CSV files found. Nothing to export.")
        return

    wb = Workbook()
    # remove default sheet
    default = wb.active
    wb.remove(default)

    for p in files:
        # file: prozorro_dairy_items_YYYY_MM.csv -> sheet: YYYY-MM
        key = p.stem.replace("prozorro_dairy_items_", "").replace("_", "-")
        sheet = key[:31]
        write_sheet_from_csv(wb, sheet, p)

    wb.save(XLSX_OUT)
    print(f"Saved: {XLSX_OUT}")


if __name__ == "__main__":
    main()
