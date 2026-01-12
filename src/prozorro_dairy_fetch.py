#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import logging
import os
import re
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from openpyxl import Workbook
except Exception:
    Workbook = None  # XLSX chunks optional if dependency missing


# -------------------------
# Config (env-overridable)
# -------------------------
BASE_URL = os.getenv("BASE_URL", "https://public-api.prozorro.gov.ua/api/2.5").rstrip("/")
LIMIT = int(os.getenv("LIMIT", "100"))  # API feed batch size
DETAIL_WORKERS = int(os.getenv("DETAIL_WORKERS", "1"))  # keep 1 by default (safer), can raise to 4-8
MAX_RUNTIME_MINUTES = int(os.getenv("MAX_RUNTIME_MINUTES", "70"))
MAX_PAGES_PER_RUN = int(os.getenv("MAX_PAGES_PER_RUN", "2000"))

FLUSH_EVERY_SECONDS = int(os.getenv("FLUSH_EVERY_SECONDS", "60"))      # CSV flush frequency
XLSX_CHUNK_EVERY_SECONDS = int(os.getenv("XLSX_CHUNK_EVERY_SECONDS", "120"))  # Excel chunk frequency (1–3 min)

# CPV prefixes (digits-only)
DEFAULT_ALLOWED_PREFIXES = "155,1551,15511,155111,1551121,15512,1553,1554,1555"
ALLOWED_PREFIXES = tuple(
    p.strip() for p in os.getenv("ALLOWED_PREFIXES", DEFAULT_ALLOWED_PREFIXES).split(",") if p.strip()
)

# Paths
DATA_DIR = Path("data")
STATE_DIR = DATA_DIR / "state"
OUTPUTS_DIR = DATA_DIR / "outputs"
CURRENT_DIR = OUTPUTS_DIR / "current"
RUNS_DIR = OUTPUTS_DIR / "runs"
LOGS_DIR = DATA_DIR / "logs"
CHECKPOINT_PATH = STATE_DIR / "checkpoint.json"

# Output files (cumulative)
ITEMS_CSV = CURRENT_DIR / "dairy_items.csv"
TENDERS_CSV = CURRENT_DIR / "dairy_tenders.csv"
# XLSX chunks dir (small files every 1–3 minutes)
XLSX_CHUNKS_DIR = CURRENT_DIR / "xlsx_chunks"

# Fields
ITEM_FIELDS = [
    "run_ts_utc",
    "tender_id",
    "tenderID",
    "tender_status",
    "tender_datePublished",
    "tender_dateModified",
    "procuringEntity_name",
    "procuringEntity_edrpou",
    "auction_startDate",
    "auction_endDate",
    "item_id",
    "item_description",
    "cpv",
    "cpv_digits",
    "quantity",
    "unit_name",
    "unit_code",
    "unit_value_amount",
    "unit_value_currency",
    "delivery_endDate",
    "delivery_streetAddress",
    "delivery_locality",
    "delivery_region",
    "delivery_postalCode",
    "delivery_countryName",
]

TENDER_FIELDS = [
    "run_ts_utc",
    "tender_id",
    "tenderID",
    "tender_status",
    "tender_datePublished",
    "tender_dateModified",
    "title",
    "procuringEntity_name",
    "procuringEntity_edrpou",
    "value_amount",
    "value_currency",
    "auction_startDate",
    "auction_endDate",
]


STOP_REQUESTED = False


def _sig_handler(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


signal.signal(signal.SIGTERM, _sig_handler)
signal.signal(signal.SIGINT, _sig_handler)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dirs() -> None:
    for p in [DATA_DIR, STATE_DIR, OUTPUTS_DIR, CURRENT_DIR, RUNS_DIR, LOGS_DIR, XLSX_CHUNKS_DIR]:
        p.mkdir(parents=True, exist_ok=True)


def setup_logger() -> logging.Logger:
    ensure_dirs()
    run_stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"run_{run_stamp}.txt"

    logger = logging.getLogger("prozorro_dairy_fetch")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)

    logger.info("Log file: %s", log_file.as_posix())
    logger.info("BASE_URL=%s", BASE_URL)
    logger.info("ALLOWED_PREFIXES=%s", list(ALLOWED_PREFIXES))
    logger.info("LIMIT=%s | MAX_RUNTIME_MINUTES=%s | MAX_PAGES_PER_RUN=%s", LIMIT, MAX_RUNTIME_MINUTES, MAX_PAGES_PER_RUN)
    logger.info("FLUSH_EVERY_SECONDS=%s | XLSX_CHUNK_EVERY_SECONDS=%s", FLUSH_EVERY_SECONDS, XLSX_CHUNK_EVERY_SECONDS)

    return logger


def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=8,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.headers.update(
        {
            "User-Agent": "prozorro-dairy-fetch/1.0 (+github-actions)",
            "Accept": "application/json",
        }
    )
    return s


def digits_only_cpv(cpv: str) -> str:
    # "15511000-3" -> "15511000"
    if not cpv:
        return ""
    m = re.findall(r"\d+", cpv)
    if not m:
        return ""
    joined = "".join(m)
    return joined[:8] if len(joined) >= 8 else joined


def matches_allowed_cpv(cpv_digits: str) -> bool:
    if not cpv_digits:
        return False
    # prefix match on digits (not necessarily 8)
    return any(cpv_digits.startswith(pref) for pref in ALLOWED_PREFIXES)


def read_checkpoint() -> Dict[str, Any]:
    if CHECKPOINT_PATH.exists():
        try:
            return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def write_checkpoint(offset: Optional[str], stats: Dict[str, Any]) -> None:
    payload = {
        "offset": offset,
        "updated_at_utc": utc_now_iso(),
        "stats": stats,
    }
    CHECKPOINT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def append_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def write_xlsx_chunk(path: Path, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
    if Workbook is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.title = "data"
    ws.append(fieldnames)
    for r in rows:
        ws.append([r.get(k, "") for k in fieldnames])
    wb.save(path.as_posix())


def safe_get(d: Dict[str, Any], path: List[str], default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def fetch_tenders_page(session: requests.Session, offset: Optional[str], logger: logging.Logger) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    params = {"limit": LIMIT}
    if offset:
        params["offset"] = offset
    url = f"{BASE_URL}/tenders"
    r = session.get(url, params=params, timeout=60)
    if r.status_code != 200:
        logger.warning("Feed request failed: %s %s", r.status_code, r.text[:200])
        return [], offset
    j = r.json()
    data = j.get("data") or []
    next_offset = safe_get(j, ["next_page", "offset"], None)
    return data, next_offset


def fetch_tender_detail(session: requests.Session, tender_id: str, logger: logging.Logger) -> Optional[Dict[str, Any]]:
    # opt_fields may be ignored by server; safe either way
    params = {
        "opt_fields": ",".join(
            [
                "tenderID",
                "datePublished",
                "dateModified",
                "status",
                "title",
                "procuringEntity",
                "value",
                "auctionPeriod",
                "items",
                "lots",
            ]
        )
    }
    url = f"{BASE_URL}/tenders/{tender_id}"
    r = session.get(url, params=params, timeout=90)
    if r.status_code != 200:
        logger.debug("Detail failed %s: %s", tender_id, r.status_code)
        return None
    j = r.json()
    return j.get("data")


def parse_tender(detail: Dict[str, Any], run_ts: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    tender_id = detail.get("id", "")
    tenderID = detail.get("tenderID", "")
    status = detail.get("status", "")
    datePublished = detail.get("datePublished", "")
    dateModified = detail.get("dateModified", "")
    title = detail.get("title", "")

    pe = detail.get("procuringEntity") or {}
    pe_name = pe.get("name", "")
    pe_edrpou = safe_get(pe, ["identifier", "id"], "")

    value_amount = safe_get(detail, ["value", "amount"], "")
    value_currency = safe_get(detail, ["value", "currency"], "")

    auction_start = safe_get(detail, ["auctionPeriod", "startDate"], "")
    auction_end = safe_get(detail, ["auctionPeriod", "endDate"], "")

    tender_row = {
        "run_ts_utc": run_ts,
        "tender_id": tender_id,
        "tenderID": tenderID,
        "tender_status": status,
        "tender_datePublished": datePublished,
        "tender_dateModified": dateModified,
        "title": title,
        "procuringEntity_name": pe_name,
        "procuringEntity_edrpou": pe_edrpou,
        "value_amount": value_amount,
        "value_currency": value_currency,
        "auction_startDate": auction_start,
        "auction_endDate": auction_end,
    }

    items = detail.get("items") or []
    out_items: List[Dict[str, Any]] = []

    for it in items:
        cls = it.get("classification") or {}
        if (cls.get("scheme") or "").upper() != "CPV":
            continue

        cpv = cls.get("id", "")  # "15511000-3"
        cpv_digits = digits_only_cpv(cpv)
        if not matches_allowed_cpv(cpv_digits):
            continue

        unit = it.get("unit") or {}
        unit_value = unit.get("value") or {}
        delivery = it.get("deliveryAddress") or {}
        delivery_end = safe_get(it, ["deliveryDate", "endDate"], "")

        out_items.append(
            {
                "run_ts_utc": run_ts,
                "tender_id": tender_id,
                "tenderID": tenderID,
                "tender_status": status,
                "tender_datePublished": datePublished,
                "tender_dateModified": dateModified,
                "procuringEntity_name": pe_name,
                "procuringEntity_edrpou": pe_edrpou,
                "auction_startDate": auction_start,
                "auction_endDate": auction_end,
                "item_id": it.get("id", ""),
                "item_description": it.get("description", ""),
                "cpv": cpv,
                "cpv_digits": cpv_digits,
                "quantity": it.get("quantity", ""),
                "unit_name": unit.get("name", ""),
                "unit_code": unit.get("code", ""),
                "unit_value_amount": unit_value.get("amount", ""),
                "unit_value_currency": unit_value.get("currency", ""),
                "delivery_endDate": delivery_end,
                "delivery_streetAddress": delivery.get("streetAddress", ""),
                "delivery_locality": delivery.get("locality", ""),
                "delivery_region": delivery.get("region", ""),
                "delivery_postalCode": delivery.get("postalCode", ""),
                "delivery_countryName": delivery.get("countryName", ""),
            }
        )

    return tender_row, out_items


def main() -> int:
    logger = setup_logger()
    session = build_session()

    cp = read_checkpoint()
    offset = cp.get("offset")
    logger.info("Start offset=%s", offset)

    run_ts = utc_now_iso()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir = RUNS_DIR / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Buffers
    tender_buf: List[Dict[str, Any]] = []
    item_buf: List[Dict[str, Any]] = []

    # For periodic XLSX chunks (only delta rows)
    xlsx_pending_tenders: List[Dict[str, Any]] = []
    xlsx_pending_items: List[Dict[str, Any]] = []

    stats = {
        "pages": 0,
        "tenders_seen": 0,
        "details_ok": 0,
        "details_failed": 0,
        "dairy_tenders": 0,
        "dairy_items": 0,
        "last_offset": offset,
    }

    start = time.monotonic()
    # reserve ~90 sec for final flush so we do NOT get cancelled while uploading artifacts
    deadline = start + max(60, MAX_RUNTIME_MINUTES * 60 - 90)

    next_flush = start + FLUSH_EVERY_SECONDS
    next_xlsx = start + XLSX_CHUNK_EVERY_SECONDS

    def flush(force: bool = False):
        nonlocal tender_buf, item_buf, xlsx_pending_tenders, xlsx_pending_items

        if not force and not tender_buf and not item_buf:
            return

        # write cumulative csv
        if tender_buf:
            append_csv(TENDERS_CSV, TENDER_FIELDS, tender_buf)
            append_csv(run_dir / "dairy_tenders.csv", TENDER_FIELDS, tender_buf)
            xlsx_pending_tenders.extend(tender_buf)
            tender_buf = []

        if item_buf:
            append_csv(ITEMS_CSV, ITEM_FIELDS, item_buf)
            append_csv(run_dir / "dairy_items.csv", ITEM_FIELDS, item_buf)
            xlsx_pending_items.extend(item_buf)
            item_buf = []

        # update checkpoint frequently
        stats["last_offset"] = offset
        write_checkpoint(offset, stats)

    def write_xlsx_chunks():
        nonlocal xlsx_pending_tenders, xlsx_pending_items

        if Workbook is None:
            return

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        if xlsx_pending_tenders:
            write_xlsx_chunk(XLSX_CHUNKS_DIR / f"dairy_tenders_chunk_{ts}.xlsx", TENDER_FIELDS, xlsx_pending_tenders)
            write_xlsx_chunk(run_dir / f"dairy_tenders_chunk_{ts}.xlsx", TENDER_FIELDS, xlsx_pending_tenders)
            xlsx_pending_tenders = []

        if xlsx_pending_items:
            write_xlsx_chunk(XLSX_CHUNKS_DIR / f"dairy_items_chunk_{ts}.xlsx", ITEM_FIELDS, xlsx_pending_items)
            write_xlsx_chunk(run_dir / f"dairy_items_chunk_{ts}.xlsx", ITEM_FIELDS, xlsx_pending_items)
            xlsx_pending_items = []

    while True:
        now = time.monotonic()
        if STOP_REQUESTED:
            logger.warning("Stop requested by signal; finishing gracefully...")
            break
        if now >= deadline:
            logger.info("Time budget reached; finishing gracefully to allow artifact upload.")
            break
        if stats["pages"] >= MAX_PAGES_PER_RUN:
            logger.info("MAX_PAGES_PER_RUN reached; finishing gracefully.")
            break

        # periodic flush/save
        if now >= next_flush:
            flush(force=False)
            next_flush = now + FLUSH_EVERY_SECONDS

        if now >= next_xlsx:
            flush(force=False)
            write_xlsx_chunks()
            next_xlsx = now + XLSX_CHUNK_EVERY_SECONDS

        # fetch feed page
        page, next_offset = fetch_tenders_page(session, offset, logger)
        stats["pages"] += 1

        if not page:
            logger.info("Feed returned empty page; stop.")
            offset = next_offset
            stats["last_offset"] = offset
            break

        stats["tenders_seen"] += len(page)

        # process details sequentially (safe). If you want faster: set DETAIL_WORKERS>1 and implement pool.
        dairy_found_in_page = 0

        for t in page:
            if STOP_REQUESTED or time.monotonic() >= deadline:
                break

            tid = t.get("id")
            if not tid:
                continue

            detail = fetch_tender_detail(session, tid, logger)
            if not detail:
                stats["details_failed"] += 1
                continue

            stats["details_ok"] += 1
            tender_row, dairy_items = parse_tender(detail, run_ts)
            if dairy_items:
                dairy_found_in_page += 1
                stats["dairy_tenders"] += 1
                stats["dairy_items"] += len(dairy_items)
                tender_buf.append(tender_row)
                item_buf.extend(dairy_items)

        offset = next_offset
        stats["last_offset"] = offset

        logger.info(
            "Progress | pages=%s | tenders_seen=%s | details_ok=%s | dairy_tenders=%s | dairy_items=%s | offset=%s",
            stats["pages"],
            stats["tenders_seen"],
            stats["details_ok"],
            stats["dairy_tenders"],
            stats["dairy_items"],
            str(offset)[:80],
        )

    # final flush + xlsx chunks
    flush(force=True)
    write_xlsx_chunks()
    write_checkpoint(offset, stats)

    logger.info("Done. Stats=%s", stats)
    # IMPORTANT: exit 0 so that the workflow continues to upload artifacts/commit checkpoint
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
