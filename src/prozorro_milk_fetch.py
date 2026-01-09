import os
import re
import json
import time
import csv
import gzip
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd


# ----------------------------
# Config (env overridable)
# ----------------------------
BASE_URL = os.getenv("PROZORRO_BASE_URL", "https://public-api.prozorro.gov.ua/api/2.5").rstrip("/")
LIMIT = int(os.getenv("PROZORRO_LIMIT", "100"))

MAX_RUNTIME_MINUTES = int(os.getenv("MAX_RUNTIME_MINUTES", "320"))  # stop early to avoid GH 6h kill
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.25"))

SAMPLE_EVERY_DEFAULT = int(os.getenv("SAMPLE_EVERY", "200"))  # <- default, do NOT modify globally

EXPORT_XLSX = os.getenv("EXPORT_XLSX", "1") == "1"
MAX_XLSX_ROWS = int(os.getenv("MAX_XLSX_ROWS", "80000"))

DATA_DIR = Path("data")
OUTPUTS_DIR = DATA_DIR / "outputs"
LOGS_DIR = DATA_DIR / "logs"
STATE_DIR = DATA_DIR / "state"
CHECKPOINT_PATH = STATE_DIR / "checkpoint.json"

# Allowed CPV categories (DK 021:2015 / CPV)
ALLOWED_CPV = [
    "15500000-3",  # Dairy products
    "15510000-6",  # Milk and cream
    "15511000-3",  # Milk
    "15511100-4",  # Pasteurised milk
    "15511210-8",  # UHT milk
    "15512000-0",  # Cream
    "15530000-2",  # Butter
    "15540000-5",  # Cheese products
    "15550000-8",  # Assorted dairy products
]


# ----------------------------
# Helpers
# ----------------------------
def ensure_dirs():
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def setup_logger(rid: str) -> logging.Logger:
    logger = logging.getLogger("prozorro_cpv_fetch")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_path = LOGS_DIR / f"run_{rid}.txt"
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logger.info("Log file: %s", log_path.as_posix())
    return logger


def load_checkpoint() -> Dict:
    if CHECKPOINT_PATH.exists():
        return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
    return {"offset": None, "created_at": utc_now_iso(), "last_run_at": None, "last_run_id": None, "stats": {}}


def save_checkpoint(cp: Dict):
    CHECKPOINT_PATH.write_text(json.dumps(cp, ensure_ascii=False, indent=2), encoding="utf-8")


def request_json(session: requests.Session, url: str, params: Dict, logger: logging.Logger,
                 timeout=(10, 60), max_retries=6) -> Dict:
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                logger.warning("HTTP %s | retry %s/%s | sleep %.1fs | %s",
                               r.status_code, attempt, max_retries, backoff, r.url)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("Request error (%s) | retry %s/%s | sleep %.1fs", str(e), attempt, max_retries, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    raise RuntimeError(f"Failed after retries: {url}")


def fetch_tender_detail(session: requests.Session, tender_id: str, logger: logging.Logger) -> Dict:
    url = f"{BASE_URL}/tenders/{tender_id}"
    return request_json(session, url, {"opt_pretty": "0"}, logger)


def normalize_cpv8(cpv: Optional[str]) -> Optional[str]:
    if not cpv:
        return None
    m = re.match(r"^(\d{8})-\d$", cpv.strip())
    if m:
        return m.group(1)
    m2 = re.match(r"^(\d{8})$", cpv.strip())
    if m2:
        return m2.group(1)
    return None


def prefixes_from_allowed(allowed: List[str]) -> List[str]:
    """
    Перетворюємо 8-значні CPV у “ієрархічні префікси” (обрізаємо нулі справа).
    Це відповідає логіці CPV-дерева.
    """
    prefixes = []
    for cpv in allowed:
        cpv8 = normalize_cpv8(cpv)
        if not cpv8:
            continue
        p = cpv8.rstrip("0")
        prefixes.append(p if p else cpv8)
    return sorted(set(prefixes), key=lambda x: (-len(x), x))


ALLOWED_PREFIXES = prefixes_from_allowed(ALLOWED_CPV)


def cpv_allowed(cpv_id: Optional[str]) -> bool:
    cpv8 = normalize_cpv8(cpv_id)
    if not cpv8:
        return False
    return any(cpv8.startswith(p) for p in ALLOWED_PREFIXES)


def month_key(primary_iso: Optional[str], fallback_iso: Optional[str]) -> str:
    dt = primary_iso or fallback_iso or ""
    return dt[:7] if len(dt) >= 7 else "unknown"


def write_rows_csv(path: Path, rows: List[Dict], logger: logging.Logger):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    fieldnames = list(rows[0].keys())

    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)

    logger.info("Appended %s rows -> %s", len(rows), path.as_posix())


def gzip_file(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(src, "rb") as f_in, gzip.open(dst, "wb") as f_out:
        f_out.writelines(f_in)


def export_xlsx(csv_path: Path, xlsx_path: Path, logger: logging.Logger, max_rows: int):
    if not csv_path.exists():
        return
    df = pd.read_csv(csv_path)
    if len(df) > max_rows:
        logger.warning("Skip XLSX (too many rows: %s) -> %s", len(df), csv_path.name)
        return
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(xlsx_path, index=False)
    logger.info("Saved XLSX -> %s", xlsx_path.as_posix())


def extract_relevant_items(tender: Dict) -> List[Dict]:
    """
    Фільтр строго по CPV items[].classification.id:
    зберігаємо тільки ті items, що належать до ALLOWED_CPV (через префіксне дерево).
    """
    items = tender.get("items") or []
    rel = []
    for it in items:
        cls = it.get("classification") or {}
        if cpv_allowed(cls.get("id")):
            rel.append(it)
    return rel


def build_rows(tender: Dict, relevant_items: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    tender_id = tender.get("id")
    tenderID = tender.get("tenderID")
    dateModified = tender.get("dateModified")
    datePublished = tender.get("datePublished")
    status = tender.get("status")
    pmt = tender.get("procurementMethodType")
    title = tender.get("title")

    pe = tender.get("procuringEntity") or {}
    pe_name = pe.get("name")
    addr = pe.get("address") or {}
    region = addr.get("region")
    locality = addr.get("locality")

    val = tender.get("value") or {}
    value_amount = val.get("amount")
    value_currency = val.get("currency")

    tender_ap = tender.get("auctionPeriod") or {}
    tender_ap_start = tender_ap.get("startDate")
    tender_ap_end = tender_ap.get("endDate")

    lots = tender.get("lots") or []
    if not lots:
        lots = [{
            "id": "single",
            "title": None,
            "status": status,
            "value": val,
            "auctionPeriod": tender_ap
        }]

    # items можуть бути прив’язані до лота через relatedLot
    by_lot = {}
    for it in relevant_items:
        rl = it.get("relatedLot") or "single"
        by_lot.setdefault(rl, []).append(it)

    lot_rows = []
    item_rows = []

    for lot in lots:
        lot_id = lot.get("id") or "single"
        rel_for_lot = by_lot.get(lot_id, [])
        if not rel_for_lot and lot_id != "single":
            rel_for_lot = by_lot.get("single", [])
        if not rel_for_lot:
            continue

        lot_title = lot.get("title")
        lot_status = lot.get("status")
        lot_value = lot.get("value") or {}
        lot_value_amount = lot_value.get("amount")

        lot_ap = lot.get("auctionPeriod") or {}
        auction_start = lot_ap.get("startDate") or tender_ap_start
        auction_end = lot_ap.get("endDate") or tender_ap_end

        lot_cpvs = sorted(set([
            (it.get("classification") or {}).get("id")
            for it in rel_for_lot
            if (it.get("classification") or {}).get("id")
        ]))

        lot_rows.append({
            "tender_id": tender_id,
            "tenderID": tenderID,
            "datePublished": datePublished,
            "dateModified": dateModified,
            "status": status,
            "procurementMethodType": pmt,
            "title": title,
            "procuringEntity_name": pe_name,
            "region": region,
            "locality": locality,
            "tender_value_amount": value_amount,
            "tender_value_currency": value_currency,
            "lot_id": lot_id,
            "lot_title": lot_title,
            "lot_status": lot_status,
            "lot_value_amount": lot_value_amount,
            "auction_startDate": auction_start,
            "auction_endDate": auction_end,
            "cpv_ids_in_lot": "|".join(lot_cpvs),
        })

        for it in rel_for_lot:
            cls = it.get("classification") or {}
            unit = it.get("unit") or {}
            item_rows.append({
                "tender_id": tender_id,
                "lot_id": lot_id,
                "item_id": it.get("id"),
                "description": it.get("description"),
                "classification_id": cls.get("id"),
                "classification_desc": cls.get("description"),
                "quantity": it.get("quantity"),
                "unit_name": unit.get("name"),
            })

    return lot_rows, item_rows


def main():
    ensure_dirs()
    rid = run_id()
    logger = setup_logger(rid)

    # локальна змінна! (виправляє UnboundLocalError)
    sample_every = SAMPLE_EVERY_DEFAULT

    cp = load_checkpoint()
    offset = cp.get("offset")

    logger.info("BASE_URL=%s", BASE_URL)
    logger.info("ALLOWED_PREFIXES=%s", ALLOWED_PREFIXES)
    logger.info("Start offset=%s", offset)
    logger.info("LIMIT=%s | SAMPLE_EVERY=%s | MAX_RUNTIME_MINUTES=%s", LIMIT, sample_every, MAX_RUNTIME_MINUTES)

    out_run_dir = OUTPUTS_DIR / rid
    out_run_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "prozorro-cpv-fetch/1.2 (+github-actions)"})

    started = time.time()
    deadline = started + (MAX_RUNTIME_MINUTES * 60)

    pages = 0
    scanned_records = 0
    detail_checks = 0
    forced_detail_checks = 0
    dairy_tenders = 0
    lots_written = 0
    items_written = 0
    touched_months = set()

    buffer_by_month: Dict[str, Dict[str, List[Dict]]] = {}

    def flush_buffers():
        nonlocal lots_written, items_written
        for month, pack in list(buffer_by_month.items()):
            lot_rows = pack.get("lots", [])
            item_rows = pack.get("items", [])
            if not lot_rows and not item_rows:
                continue

            month_dir = out_run_dir / month
            lots_csv = month_dir / "dairy_lots.csv"
            items_csv = month_dir / "dairy_items.csv"

            write_rows_csv(lots_csv, lot_rows, logger)
            write_rows_csv(items_csv, item_rows, logger)

            lots_written += len(lot_rows)
            items_written += len(item_rows)

            buffer_by_month[month] = {"lots": [], "items": []}

    try:
        while True:
            if time.time() > deadline:
                logger.warning("Time budget reached -> graceful stop.")
                break

            params = {
                "limit": LIMIT,
                # мінімально потрібні поля, щоб не тягти зайве
                "opt_fields": "id,dateModified,tenderID,status",
                "opt_pretty": "0",
            }
            if offset:
                params["offset"] = offset

            feed = request_json(session, f"{BASE_URL}/tenders", params, logger)
            batch = feed.get("data") or []
            if not batch:
                logger.info("No data in feed -> done.")
                break

            pages += 1
            if pages % 200 == 0:
                logger.info(
                    "Progress | pages=%s | scanned=%s | detail_checks=%s (forced=%s) | dairy_tenders=%s | offset=%s",
                    pages, scanned_records, detail_checks, forced_detail_checks, dairy_tenders, offset
                )

            for rec in batch:
                if time.time() > deadline:
                    break

                scanned_records += 1
                tender_id = rec.get("id")
                if not tender_id:
                    continue

                must_check = False
                # строгий CPV-фільтр можливий тільки по items, тому:
                # - кожні sample_every записів форсуємо /tenders/{id} і дивимось items[].classification.id
                if sample_every > 0 and (scanned_records % sample_every == 0):
                    must_check = True
                    forced_detail_checks += 1

                if not must_check:
                    continue

                detail_checks += 1
                detail = fetch_tender_detail(session, tender_id, logger)
                tender = detail.get("data") or {}

                relevant_items = extract_relevant_items(tender)
                if not relevant_items:
                    time.sleep(REQUEST_SLEEP_SECONDS)
                    continue

                dairy_tenders += 1
                lot_rows, item_rows = build_rows(tender, relevant_items)
                if not lot_rows:
                    time.sleep(REQUEST_SLEEP_SECONDS)
                    continue

                for lr in lot_rows:
                    month = month_key(lr.get("auction_startDate"), lr.get("datePublished"))
                    touched_months.add(month)
                    buffer_by_month.setdefault(month, {"lots": [], "items": []})
                    buffer_by_month[month]["lots"].append(lr)

                for ir in item_rows:
                    month = month_key(None, tender.get("datePublished"))
                    buffer_by_month.setdefault(month, {"lots": [], "items": []})
                    buffer_by_month[month]["items"].append(ir)

                if dairy_tenders % 100 == 0:
                    flush_buffers()

                time.sleep(REQUEST_SLEEP_SECONDS)

            next_page = feed.get("next_page") or {}
            new_offset = next_page.get("offset")
            if not new_offset:
                logger.info("No next_page.offset -> done.")
                break
            offset = new_offset

            cp["offset"] = offset
            cp["last_run_at"] = utc_now_iso()
            cp["last_run_id"] = rid
            cp["stats"] = {
                "pages": pages,
                "scanned_records": scanned_records,
                "detail_checks": detail_checks,
                "forced_detail_checks": forced_detail_checks,
                "dairy_tenders": dairy_tenders,
                "lots_written_so_far": lots_written,
                "items_written_so_far": items_written,
                "touched_months": sorted(touched_months),
                "sample_every": sample_every,
            }
            save_checkpoint(cp)

            # якщо довго не знаходимо — робимо sampling частішим
            if pages >= 500 and dairy_tenders == 0 and sample_every > 20:
                new_sample = max(20, sample_every // 2)
                logger.warning("No dairy found yet. Auto-tuning SAMPLE_EVERY: %s -> %s", sample_every, new_sample)
                sample_every = new_sample

    finally:
        flush_buffers()

        for month in sorted(touched_months):
            month_dir = out_run_dir / month
            lots_csv = month_dir / "dairy_lots.csv"
            items_csv = month_dir / "dairy_items.csv"

            if lots_csv.exists():
                gzip_file(lots_csv, month_dir / "dairy_lots.csv.gz")
            if items_csv.exists():
                gzip_file(items_csv, month_dir / "dairy_items.csv.gz")

            if EXPORT_XLSX and lots_csv.exists():
                export_xlsx(lots_csv, month_dir / "dairy_lots.xlsx", logger, MAX_XLSX_ROWS)

        cp["offset"] = offset
        cp["last_run_at"] = utc_now_iso()
        cp["last_run_id"] = rid
        cp["stats"] = {
            "pages": pages,
            "scanned_records": scanned_records,
            "detail_checks": detail_checks,
            "forced_detail_checks": forced_detail_checks,
            "dairy_tenders": dairy_tenders,
            "lots_written": lots_written,
            "items_written": items_written,
            "touched_months": sorted(touched_months),
            "outputs_dir": str(out_run_dir),
            "sample_every_final": sample_every,
        }
        save_checkpoint(cp)

        logger.info(
            "DONE | pages=%s | scanned=%s | detail_checks=%s (forced=%s) | dairy_tenders=%s | lots=%s | items=%s",
            pages, scanned_records, detail_checks, forced_detail_checks, dairy_tenders, lots_written, items_written
        )
        logger.info("Run outputs: %s", out_run_dir.as_posix())


if __name__ == "__main__":
    main()
