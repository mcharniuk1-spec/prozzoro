import os
import re
import json
import time
import csv
import gzip
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import requests
import pandas as pd


# ----------------------------
# Config (env overridable)
# ----------------------------
BASE_URL = os.getenv("PROZORRO_BASE_URL", "https://public-api.prozorro.gov.ua/api/2.5").rstrip("/")
LIMIT = int(os.getenv("PROZORRO_LIMIT", "100"))
MAX_RUNTIME_MINUTES = int(os.getenv("MAX_RUNTIME_MINUTES", "320"))  # < 6h job cap
REQUEST_SLEEP_SECONDS = float(os.getenv("REQUEST_SLEEP_SECONDS", "0.25"))

EXPORT_XLSX = os.getenv("EXPORT_XLSX", "1") == "1"
MAX_XLSX_ROWS = int(os.getenv("MAX_XLSX_ROWS", "80000"))

DATA_DIR = Path("data")
OUTPUTS_DIR = DATA_DIR / "outputs"
LOGS_DIR = DATA_DIR / "logs"
STATE_DIR = DATA_DIR / "state"
CHECKPOINT_PATH = STATE_DIR / "checkpoint.json"

# CPV prefix for dairy products group: 155xxxxxx-x
CPV_PREFIXES = ("155",)

KEYWORDS = [
    r"\bмолок", r"\bмолоч", r"\bвершк", r"\bсметан", r"\bкефір", r"\bйогурт",
    r"\bсир\b", r"\bтворог", r"\bмасло\b", r"\bряжен", r"\bбринз",
    r"\bmilk\b", r"\bdairy\b", r"\bcheese\b", r"\bbutter\b", r"\byogurt\b", r"\bcream\b", r"\bkefir\b",
]
KW_RE = re.compile("|".join(KEYWORDS), re.IGNORECASE | re.UNICODE)

# Стани, де найчастіше вже є auctionPeriod або кінцеві дані
DETAIL_STATUSES = {
    "active.auction", "active.qualification", "active.awarded",
    "complete", "cancelled", "unsuccessful",
}


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
    logger = logging.getLogger("prozorro_milk_dairy")
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
    return {
        "offset": None,
        "created_at": utc_now_iso(),
        "last_run_at": None,
        "last_run_id": None,
        "stats": {},
    }


def save_checkpoint(cp: Dict):
    CHECKPOINT_PATH.write_text(json.dumps(cp, ensure_ascii=False, indent=2), encoding="utf-8")


def safe_get(d: Dict, path: str, default=None):
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(part)
        if cur is None:
            return default
    return cur


def request_json(session: requests.Session, url: str, params: Dict, logger: logging.Logger,
                 timeout=(10, 60), max_retries=6) -> Dict:
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                logger.warning("HTTP %s | %s | retry %s/%s | sleep %.1fs",
                               r.status_code, r.url, attempt, max_retries, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("Request error | %s | retry %s/%s | sleep %.1fs",
                           str(e), attempt, max_retries, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    raise RuntimeError(f"Failed after retries: {url}")


def is_candidate_list_record(rec: Dict) -> bool:
    title = (rec.get("title") or "").strip()
    cls_id = (safe_get(rec, "classification.id", "") or "").strip()
    if cls_id.startswith(CPV_PREFIXES):
        return True
    if title and KW_RE.search(title):
        return True
    return False


def dairy_category(text: str) -> str:
    t = (text or "").lower()
    if re.search(r"\b(молок|milk)\b", t): return "milk"
    if re.search(r"\b(вершк|cream)\b", t): return "cream"
    if re.search(r"\b(масло|butter)\b", t): return "butter"
    if re.search(r"\b(йогурт|yogurt)\b", t): return "yogurt"
    if re.search(r"\b(кефір|kefir)\b", t): return "kefir"
    if re.search(r"\b(сметан)\b", t): return "sour_cream"
    if re.search(r"\b(сир\b|cheese)\b", t): return "cheese"
    if re.search(r"\b(творог)\b", t): return "curd"
    return "dairy_other"


def period_month_key(preferred_iso: Optional[str], fallback_iso: Optional[str]) -> str:
    dt = preferred_iso or fallback_iso or ""
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


# ----------------------------
# Core extraction
# ----------------------------
def fetch_tender_detail(session: requests.Session, tender_id: str, logger: logging.Logger) -> Dict:
    url = f"{BASE_URL}/tenders/{tender_id}"
    return request_json(session, url, {"opt_pretty": "0"}, logger)


def tender_has_dairy_items(tender: Dict) -> Tuple[bool, List[Dict]]:
    """
    Повертає (is_dairy, relevant_items).
    relevant_items: список items, які виглядають як dairy.
    """
    items = tender.get("items") or []
    relevant = []
    for it in items:
        descr = (it.get("description") or "").strip()
        icls = it.get("classification") or {}
        icls_id = (icls.get("id") or "").strip()
        if icls_id.startswith(CPV_PREFIXES) or (descr and KW_RE.search(descr)):
            relevant.append(it)
    return (len(relevant) > 0, relevant)


def build_rows(tender: Dict, relevant_items: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    returns (lot_rows, item_rows)
    """
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

    tcls = tender.get("classification") or {}
    tcls_id = tcls.get("id")
    tcls_desc = tcls.get("description")

    val = tender.get("value") or {}
    value_amount = val.get("amount")
    value_currency = val.get("currency")

    # auctionPeriod може бути на рівні тендера або лота
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

    # Групуємо релевантні items по relatedLot
    by_lot = {}
    for it in relevant_items:
        rl = it.get("relatedLot") or "single"
        by_lot.setdefault(rl, []).append(it)

    lot_rows = []
    item_rows = []

    for lot in lots:
        lot_id = lot.get("id") or "single"
        lot_title = lot.get("title")
        lot_status = lot.get("status")
        lot_value = lot.get("value") or {}
        lot_value_amount = lot_value.get("amount")

        lot_ap = lot.get("auctionPeriod") or {}
        auction_start = lot_ap.get("startDate") or tender_ap_start
        auction_end = lot_ap.get("endDate") or tender_ap_end

        rel_for_lot = by_lot.get(lot_id, [])
        # якщо items без relatedLot — пробуємо “single”
        if not rel_for_lot and lot_id != "single":
            rel_for_lot = by_lot.get("single", [])

        if not rel_for_lot:
            continue

        cat = dairy_category(" ".join([(it.get("description") or "") for it in rel_for_lot]) + " " + (title or ""))

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
            "tender_classification_id": tcls_id,
            "tender_classification_desc": tcls_desc,
            "tender_value_amount": value_amount,
            "tender_value_currency": value_currency,
            "lot_id": lot_id,
            "lot_title": lot_title,
            "lot_status": lot_status,
            "lot_value_amount": lot_value_amount,
            "auction_startDate": auction_start,
            "auction_endDate": auction_end,
            "dairy_category": cat,
        })

        for it in rel_for_lot:
            icls = it.get("classification") or {}
            unit = it.get("unit") or {}
            item_rows.append({
                "tender_id": tender_id,
                "lot_id": lot_id,
                "item_id": it.get("id"),
                "description": it.get("description"),
                "classification_id": icls.get("id"),
                "classification_desc": icls.get("description"),
                "quantity": it.get("quantity"),
                "unit_name": unit.get("name"),
                "dairy_category": dairy_category((it.get("description") or "") + " " + (title or "")),
            })

    return lot_rows, item_rows


# ----------------------------
# Main
# ----------------------------
def main():
    ensure_dirs()
    rid = run_id()
    logger = setup_logger(rid)

    cp = load_checkpoint()
    offset = cp.get("offset")
    logger.info("BASE_URL=%s", BASE_URL)
    logger.info("Start offset=%s", offset)
    logger.info("LIMIT=%s | MAX_RUNTIME_MINUTES=%s", LIMIT, MAX_RUNTIME_MINUTES)

    out_run_dir = OUTPUTS_DIR / rid
    out_run_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "prozorro-milk-dairy-fetch/1.0 (+github-actions)"})

    started = time.time()
    deadline = started + (MAX_RUNTIME_MINUTES * 60)

    pages = 0
    list_candidates = 0
    details_fetched = 0
    lots_written = 0
    items_written = 0

    touched_months = set()

    # Буфер для періодичного flush (щоб не тримати все в RAM)
    buffer_by_month: Dict[str, Dict[str, List[Dict]]] = {}

    def flush_buffers():
        nonlocal lots_written, items_written
        for month, pack in list(buffer_by_month.items()):
            lot_rows = pack.get("lots", [])
            item_rows = pack.get("items", [])
            if not lot_rows and not item_rows:
                continue

            month_dir = out_run_dir / month
            lots_csv = month_dir / "milk_dairy_lots.csv"
            items_csv = month_dir / "milk_dairy_items.csv"

            write_rows_csv(lots_csv, lot_rows, logger)
            write_rows_csv(items_csv, item_rows, logger)

            lots_written += len(lot_rows)
            items_written += len(item_rows)

            buffer_by_month[month] = {"lots": [], "items": []}

    try:
        while True:
            if time.time() > deadline:
                logger.warning("Time budget reached -> stopping gracefully (so upload step can run).")
                break

            params = {
                "limit": LIMIT,
                "opt_fields": "id,dateModified,tenderID,title,status,procurementMethodType,classification",
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
                logger.info("Progress | pages=%s | candidates=%s | details=%s | offset=%s",
                            pages, list_candidates, details_fetched, offset)

            for rec in batch:
                if time.time() > deadline:
                    break

                st = rec.get("status") or ""
                if not is_candidate_list_record(rec):
                    continue

                list_candidates += 1

                # економимо трафік/час: тягнемо деталі лише для релевантних станів
                if st not in DETAIL_STATUSES:
                    continue

                tender_id = rec.get("id")
                if not tender_id:
                    continue

                detail = fetch_tender_detail(session, tender_id, logger)
                tender = detail.get("data") or {}
                ok, relevant_items = tender_has_dairy_items(tender)
                if not ok:
                    continue

                lot_rows, item_rows = build_rows(tender, relevant_items)
                if not lot_rows:
                    continue

                details_fetched += 1

                # період = місяць аукціону (якщо є) інакше datePublished
                for lr in lot_rows:
                    month = period_month_key(lr.get("auction_startDate"), lr.get("datePublished"))
                    touched_months.add(month)
                    buffer_by_month.setdefault(month, {"lots": [], "items": []})
                    buffer_by_month[month]["lots"].append(lr)

                for ir in item_rows:
                    # беремо місяць по tender_id/lot_id через lots (простий підхід)
                    month = period_month_key(None, tender.get("datePublished"))
                    buffer_by_month.setdefault(month, {"lots": [], "items": []})
                    buffer_by_month[month]["items"].append(ir)

                # flush кожні ~200 деталей, щоб не втратити прогрес і не роздувати RAM
                if details_fetched % 200 == 0:
                    flush_buffers()

                time.sleep(REQUEST_SLEEP_SECONDS)

            # update offset
            next_page = feed.get("next_page") or {}
            new_offset = next_page.get("offset")
            if not new_offset:
                logger.info("No next_page.offset -> done.")
                break
            offset = new_offset

            # checkpoint на диск регулярно
            cp["offset"] = offset
            cp["last_run_at"] = utc_now_iso()
            cp["last_run_id"] = rid
            cp["stats"] = {
                "pages": pages,
                "list_candidates": list_candidates,
                "details_fetched": details_fetched,
                "lots_written_so_far": lots_written,
                "items_written_so_far": items_written,
                "touched_months": sorted(touched_months),
            }
            save_checkpoint(cp)

    finally:
        # добиваємо flush, навіть якщо вихід ранній
        flush_buffers()

        # gzip + xlsx по місяцях цього run’а
        for month in sorted(touched_months):
            month_dir = out_run_dir / month
            lots_csv = month_dir / "milk_dairy_lots.csv"
            items_csv = month_dir / "milk_dairy_items.csv"

            if lots_csv.exists():
                gzip_file(lots_csv, month_dir / "milk_dairy_lots.csv.gz")
            if items_csv.exists():
                gzip_file(items_csv, month_dir / "milk_dairy_items.csv.gz")

            # XLSX (обережно, щоб не вбити час/пам’ять)
            if EXPORT_XLSX and lots_csv.exists():
                export_xlsx(lots_csv, month_dir / "milk_dairy_lots.xlsx", logger, MAX_XLSX_ROWS)

        # фінальний checkpoint
        cp["offset"] = offset
        cp["last_run_at"] = utc_now_iso()
        cp["last_run_id"] = rid
        cp["stats"] = {
            "pages": pages,
            "list_candidates": list_candidates,
            "details_fetched": details_fetched,
            "lots_written": lots_written,
            "items_written": items_written,
            "touched_months": sorted(touched_months),
            "outputs_dir": str(out_run_dir),
        }
        save_checkpoint(cp)

        logger.info("DONE | pages=%s | candidates=%s | details=%s | lots=%s | items=%s",
                    pages, list_candidates, details_fetched, lots_written, items_written)
        logger.info("Run outputs: %s", out_run_dir.as_posix())


if __name__ == "__main__":
    main()
