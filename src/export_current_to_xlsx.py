import pathlib
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
BY_MONTH = ROOT / "data" / "outputs" / "by_month"
OUT = ROOT / "data" / "outputs" / "current" / "dairy_full_export.xlsx"

def collect(pattern: str):
    files = sorted(BY_MONTH.glob(pattern))
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception:
            pass
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def main():
    tenders = collect("tenders_*.csv")
    lots = collect("lots_*.csv")
    items = collect("items_*.csv")

    with pd.ExcelWriter(OUT, engine="openpyxl") as w:
        tenders.to_excel(w, index=False, sheet_name="tenders")
        lots.to_excel(w, index=False, sheet_name="lots")
        items.to_excel(w, index=False, sheet_name="items")

    print(f"Saved: {OUT}")

if __name__ == "__main__":
    main()
