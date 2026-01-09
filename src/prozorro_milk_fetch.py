import os
import re
import json
import time
import gzip
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone

import requests
import pandas as pd

BASE_URL_DEFAULT = "https://public-api.prozorro.gov.ua/api/2.5"

DATA_DIR = Path("data")
OUTPUT_DIR = DATA_DIR / "outputs"
LOG_DIR = DATA_DIR / "logs"
STATE_DIR = DATA_DIR / "state"

CHECKPOINT_PATH = STATE_DIR / "checkpoint.json"
DB_PATH = STATE_DIR / "prozorro_milk.sqlite"

# "ДК 021:2015" CPV група 155xxxxxx-x = Dairy products
CPV_PREFIXES = ("155",)

# На випадок кривої/узагальненої класифікації — доганяємо ключовими словами
KEYWORDS = [
    r"\bмолок", r"\bмолоч", r"\bвершк", r"\bсметан", r"\bкефір", r"\bйогурт",
    r"\bсир\b", r"\bтворог", r"\bмасло\b", r"\bряжен", r"\bбринз",
    r"\bmilk\b", r"\bdairy\b", r"\bcheese\b", r"\bbutter\b", r"\byogurt\b", r"\bcream\b", r"\bkefir\b",
]
KW_RE = re.compile("|".join(KEYWORDS), re.IGNORECASE | re.UNICODE)

# Статуси, коли зазвичай вже є “нормальні” аукціонні дати/структура
FETCH_DETAIL_STATUSES = {
    "active.auction", "active.qualification", "active.awarded",
    "complete", "cancelled", "unsuccessful",
}

def ensure_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def setup_logger() -> logging.Logger:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"run_{ts}.txt"

    logger = logging.getLogger("prozorro_milk")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logger.info("Log file: %s", log_path.as_posix())
    return logger

def load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
    return {
        "offset": None,
        "started_at": utc_now_iso(),
        "last_run_at": None,
        "stats": {},
    }

def save_checkpoint(cp: dict) -> None:
    CHECKPOINT_PATH.write_text(json.dumps(cp, ensure_ascii=False, indent=2), encoding="utf-8")

def request_json(session: requests.Session, url: str, params: dict, logger: logging.Logger, timeout=(10, 60), max_retries=6):
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                logger.warning("HTTP %s on %s (attempt %s/%s) -> sleep %.1fs",
                               r.status_code, r.url, attempt, max_retries, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("Request error (attempt %s/%s): %s -> sleep %.1fs", attempt, max_retries, e, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    raise RuntimeError(f"Failed after {max_retries} retries: {url}")

def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tenders (
            tender_id TEXT NOT NULL,
            lot_id TEXT NOT NULL,
            tenderID TEXT,
            dateModified TEXT,
            datePublished TEXT,
            status TEXT,
            procurementMethodType TEXT,
            title TEXT,
            procuringEntity_name TEXT,
            classification_id TEXT,
            classification_description TEXT,
            value_amount REAL,
            value_currency TEXT,
            lot_title TEXT,
            lot_status TEXT,
            lot_value_amount REAL,
            auction_startDate TEXT,
            auction_endDate TEXT,
            PRIMARY KEY (tender_id, lot_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS items (
            tender_id TEXT NOT NULL,
            lot_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            description TEXT,
            classification_id TEXT,
            classification_description TEXT,
            quantity REAL,
            unit_name TEXT,
            PRIMARY KEY (tender_id, lot_id, item_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            tender_id TEXT PRIMARY KEY,
            last_dateModified TEXT,
            last_status TEXT,
            is_candidate INTEGER,
            last_checked_at TEXT
        )
    """)
    conn.commit()

def is_candidate(list_rec: dict) -> bool:
    cls = (list_rec.get("classification") or {})
    cls_id = (cls.get("id") or "").strip()
    title = (list_rec.get("title") or "")
    # інколи title/classification можуть не прийти — тоді тільки keyword
    if cls_id.startswith(CPV_PREFIXES):
        return True
    if KW_RE.search(title):
        return True
    return False

def month_key(iso_dt: str) -> str:
    # ISO 8601 -> YYYY-MM
    if not iso_dt or len(iso_dt) < 7:
        return "unknown"
    return iso_dt[:7]

def upsert_seen(conn: sqlite3.Connection, tender_id: str, dateModified: str, status: str, candidate: bool):
    conn.execute("""
        INSERT INTO seen(tender_id, last_dateModified, last_status, is_candidate, last_checked_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(tender_id) DO UPDATE SET
            last_dateModified=excluded.last_dateModified,
            last_status=excluded.last_status,
            is_candidate=MAX(seen.is_candidate, excluded.is_candidate),
            last_checked_at=excluded.last_checked_at
    """, (tender_id, dateModified, status, int(candidate), utc_now_iso()))

def fetch_tender_details(session: requests.Session, base_url: str, tender_id: str, logger: logging.Logger) -> dict:
    url = f"{base_url}/tenders/{tender_id}"
    # opt_schema=ocds може бути корисним для OCDS-представлення, але для витягування полів tender'а не критично.
    params = {"opt_pretty": "0"}
    return request_json(session, url, params, logger)

def extract_and_store(conn: sqlite3.Connection, tender: dict) -> set[str]:
    """
    Повертає множину місяців (YYYY-MM), яких торкнулися апдейти.
    """
    touched_months: set[str] = set()

    t_id = tender.get("id") or ""
    tenderID = tender.get("tenderID")
    dateModified = tender.get("dateModified")
    datePublished = tender.get("datePublished")
    status = tender.get("status")
    pmt = tender.get("procurementMethodType")
    title = tender.get("title")

    pe = tender.get("procuringEntity") or {}
    pe_name = pe.get("name")

    cls = tender.get("classification") or {}
    cls_id = cls.get("id")
    cls_desc = cls.get("description")

    val = tender.get("value") or {}
    val_amount = val.get("amount")
    val_currency = val.get("currency")

    # Лоти: якщо лотів нема — робимо псевдо-лот "single"
    lots = tender.get("lots") or []
    if not lots:
        lots = [{
            "id": "single",
            "title": None,
            "status": tender.get("status"),
            "value": tender.get("value") or {}
        }]

    # auctionPeriod може бути на рівні тендера або лота
    tender_ap = tender.get("auctionPeriod") or {}
    tender_ap_start = tender_ap.get("startDate")
    tender_ap_end = tender_ap.get("endDate")

    for lot in lots:
        lot_id = lot.get("id") or "single"
        lot_title = lot.get("title")
        lot_status = lot.get("status")

        lot_value = lot.get("value") or {}
        lot_value_amount = lot_value.get("amount")

        lot_ap = lot.get("auctionPeriod") or {}
        ap_start = lot_ap.get("startDate") or tender_ap_start
        ap_end = lot_ap.get("endDate") or tender_ap_end

        conn.execute("""
            INSERT INTO tenders(
                tender_id, lot_id, tenderID, dateModified, datePublished, status, procurementMethodType,
                title, procuringEntity_name, classification_id, classification_description,
                value_amount, value_currency, lot_title, lot_status, lot_value_amount,
                auction_startDate, auction_endDate
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(tender_id, lot_id) DO UPDATE SET
                tenderID=excluded.tenderID,
                dateModified=excluded.dateModified,
                datePublished=excluded.datePublished,
                status=excluded.status,
                procurementMethodType=excluded.procurementMethodType,
                title=excluded.title,
                procuringEntity_name=excluded.procuringEntity_name,
                classification_id=excluded.classification_id,
                classification_description=excluded.classification_description,
                value_amount=excluded.value_amount,
                value_currency=excluded.value_currency,
                lot_title=excluded.lot_title,
                lot_status=excluded.lot_status,
                lot_value_amount=excluded.lot_value_amount,
                auction_startDate=excluded.auction_startDate,
                auction_endDate=excluded.auction_endDate
        """, (
            t_id, lot_id, tenderID, dateModified, datePublished, status, pmt,
            title, pe_name, cls_id, cls_desc,
            val_amount, val_currency, lot_title, lot_status, lot_value_amount,
            ap_start, ap_end
        ))

        # items: прив’язка item->lot може бути через relatedLot
        items = tender.get("items") or []
        for it in items:
            related_lot = it.get("relatedLot") or "single"
            if related_lot != lot_id:
                continue

            item_id = it.get("id") or ""
            descr = it.get("description")
            icls = it.get("classification") or {}
            icls_id = icls.get("id")
            icls_desc = icls.get("description")
            qty = it.get("quantity")
            unit = it.get("unit") or {}
            unit_name = unit.get("name")

            conn.execute("""
                INSERT INTO items(
                    tender_id, lot_id, item_id, description, classification_id,
                    classification_description, quantity, unit_name
                ) VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(tender_id, lot_id, item_id) DO UPDATE SET
                    description=excluded.description,
                    classification_id=excluded.classification_id,
                    classification_description=excluded.classification_description,
                    quantity=excluded.quantity,
                    unit_name=excluded.unit_name
            """, (t_id, lot_id, item_id, descr, icls_id, icls_desc, qty, unit_name))

        touched_months.add(month_key(dateModified))

    return touched_months

def export_month(conn: sqlite3.Connection, month: str, logger: logging.Logger) -> None:
    out_dir = OUTPUT_DIR / month
    out_dir.mkdir(parents=True, exist_ok=True)

    tenders_csv_gz = out_dir / f"tenders_{month}.csv.gz"
    items_csv_gz = out_dir / f"items_{month}.csv.gz"
    xlsx_path = out_dir / f"milk_dairy_{month}.xlsx"

    tdf = pd.read_sql_query(
        "SELECT * FROM tenders WHERE substr(dateModified,1,7)=? ORDER BY dateModified",
        conn, params=(month,)
    )
    idf = pd.read_sql_query(
        "SELECT i.* FROM items i JOIN tenders t ON t.tender_id=i.tender_id AND t.lot_id=i.lot_id "
        "WHERE substr(t.dateModified,1,7)=?",
        conn, params=(month,)
    )

    # CSV.GZ
    with gzip.open(tenders_csv_gz, "wt", encoding="utf-8", newline="") as f:
        tdf.to_csv(f, index=False)
    with gzip.open(items_csv_gz, "wt", encoding="utf-8", newline="") as f:
        idf.to_csv(f, index=False)

    logger.info("Exported %s: tenders=%s rows, items=%s rows", month, len(tdf), len(idf))

    # XLSX (обмежуємо розмір, щоб не вбити ран)
    max_rows_for_xlsx = int(os.getenv("MAX_XLSX_ROWS", "200000"))
    if len(tdf) <= max_rows_for_xlsx and len(idf) <= max_rows_for_xlsx:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            tdf.to_excel(writer, sheet_name="tenders", index=False)
            idf.to_excel(writer, sheet_name="items", index=False)
        logger.info("Saved XLSX: %s", xlsx_path.as_posix())
    else:
        logger.warning("Skip XLSX for %s (too large). CSV.GZ is saved.", month)

def main():
    ensure_dirs()
    logger = setup_logger()

    base_url = os.getenv("PROZORRO_BASE_URL", BASE_URL_DEFAULT).rstrip("/")
    limit_env = int(os.getenv("PROZORRO_LIMIT", "100"))
    max_runtime_minutes = int(os.getenv("MAX_RUNTIME_MINUTES", "320"))  # < 6h, лишаємо час на export+artifact
    max_runtime_seconds = max_runtime_minutes * 60

    cp = load_checkpoint()
    offset = cp.get("offset")

    logger.info("Base URL: %s", base_url)
    logger.info("Start offset: %s", offset)
    logger.info("Limit: %s", limit_env)
    logger.info("Max runtime: %s minutes", max_runtime_minutes)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "prozorro-milk-fetch/1.0 (+github-actions)"
    })

    start_ts = time.time()
    touched_months: set[str] = set()

    pages = 0
    candidates = 0
    details_fetched = 0

    try:
        while True:
            elapsed = time.time() - start_ts
            if elapsed > max_runtime_seconds:
                logger.warning("Time budget reached (%.1f min). Stopping gracefully...", elapsed / 60)
                break

            params = {
                "limit": limit_env,
                # намагаємось забрати “легкі” поля одразу, щоб не ходити в /tenders/{id} без потреби
                "opt_fields": "id,dateModified,tenderID,title,status,procurementMethodType,classification",
            }
            if offset:
                params["offset"] = offset

            feed = request_json(session, f"{base_url}/tenders", params, logger)
            batch = feed.get("data") or []
            if not batch:
                logger.info("No more data in feed. Done.")
                break

            pages += 1
            # НЕ спамимо лог кожною сторінкою (це теж час)
            if pages % 200 == 0:
                logger.info("Progress: pages=%s, candidates=%s, details=%s, last_offset=%s",
                            pages, candidates, details_fetched, offset)

            for rec in batch:
                t_id = rec.get("id")
                dm = rec.get("dateModified")
                st = rec.get("status") or ""
                if not t_id:
                    continue

                cand = is_candidate(rec)
                upsert_seen(conn, t_id, dm, st, cand)

                if not cand:
                    continue

                candidates += 1

                # економимо час: деталі тягнемо здебільшого для “аукціонних/фінальних” станів
                if st not in FETCH_DETAIL_STATUSES:
                    continue

                detail = fetch_tender_details(session, base_url, t_id, logger)
                tender = (detail.get("data") or {})
                months = extract_and_store(conn, tender)
                touched_months |= months
                details_fetched += 1

                # коміти кожні N деталей
                if details_fetched % 200 == 0:
                    conn.commit()

            conn.commit()

            next_page = feed.get("next_page") or {}
            offset = next_page.get("offset")
            if not offset:
                logger.info("No next_page.offset found. Done.")
                break

    finally:
        # Зберігаємо checkpoint у будь-якому разі
        cp["offset"] = offset
        cp["last_run_at"] = utc_now_iso()
        cp["stats"] = {
            "pages": pages,
            "candidates": candidates,
            "details_fetched": details_fetched,
            "touched_months_count": len(touched_months),
        }
        save_checkpoint(cp)
        logger.info("Checkpoint saved: offset=%s", offset)

        # Експортимо лише місяці, яких торкались (швидко і практично)
        for m in sorted(touched_months):
            export_month(conn, m, logger)

        conn.commit()
        conn.close()

        logger.info("Done. pages=%s candidates=%s details=%s months=%s",
                    pages, candidates, details_fetched, sorted(touched_months))

if __name__ == "__main__":
    main()
