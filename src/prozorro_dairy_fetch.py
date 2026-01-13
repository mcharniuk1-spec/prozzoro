from __future__ import annotations

import csv
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


# -----------------------------
# Config
# -----------------------------
BASE_URL = os.getenv("PROZORRO_BASE_URL", "https://public-api.prozorro.gov.ua/api/2.5").rstrip("/")

# Your requested CPV set (UA/EN don't matter here; we match numeric code)
DEFAULT_CPV_CODES = [
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

# You can override via env: CPV_CODES="15500000-3,15511100-4,..."
CPV_CODES = [c.strip() for c in os.getenv("CPV_CODES", ",".join(DEFAULT_CPV_CODES)).split(",") if c.strip()]

# Feed paging
LIMIT = int(os.getenv("LIMIT", "100"))  # feed batch size (docs: default 100) :contentReference[oaicite:7]{index=7}
DETAIL_WORKERS = int(os.getenv("DETAIL_WORKERS", "10"))

# Time-budget: stop BEFORE GH kills job; leave room for upload/commit steps
MAX_RUNTIME_MINUTES = int(os.getenv("MAX_RUNTIME_MINUTES", "320"))

# Flush checkpoint + log progress often
SAVE_EVERY_SECONDS = int(os.getenv("SAVE_EVERY_SECONDS", "120"))

# Backfill start
START_DATE = os.getenv("START_DATE", "2024-01-01")  # ISO date
START_OFFSET = os.getenv("START_OFFSET", "").strip()  # if you already have offset, set it here

# Paths
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "data/outputs"))
MONTHLY_DIR = OUTPUT_DIR / "monthly"
LOG_DIR = Path(os.getenv("LOG_DIR", "data/logs"))
STATE_DIR = Path(os.getenv("STATE_DIR", "data/state"))

CHECKPOINT_PATH = STATE_DIR / "prozorro_checkpoint.json"

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_utc")
LOG_PATH = LOG_DIR / f"run_{RUN_ID}.txt"

ALL_CSV_PATH = OUTPUT_DIR / "prozorro_dairy_items_all.csv"


# -----------------------------
# Helpers
# -----------------------------
def mkdirs() -> None:
    for p in [OUTPUT_DIR, MONTHLY_DIR, LOG_DIR, STATE_DIR]:
        p.mkdir(parents=True, exist_ok=True)


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("prozorro_dairy_fetch")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(logging.INFO)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_checkpoint() -> Dict[str, Any]:
    if CHECKPOINT_PATH.exists():
        try:
            return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def iso_to_month_key(iso_dt: str) -> str:
    # iso like "2026-01-09T14:12:39.175+02:00"
    # month key "2026_01"
    try:
        dt = datetime.fromisoformat(iso_dt.replace("Z", "+00:00"))
        return f"{dt.year:04d}_{dt.month:02d}"
    except Exception:
        return "unknown"


def digits_only(code: str) -> str:
    return "".join(ch for ch in code if ch.isdigit())


def cpv_prefixes(codes: List[str]) -> List[str]:
    """
    Convert CPV codes into numeric prefixes that include subcodes.
    Example:
      15500000-3 -> digits "155000003" -> base "15500000" -> strip trailing zeros -> "155"
      15511100-4 -> base "15511100" -> strip trailing zeros -> "155111"
    """
    prefixes: List[str] = []
    for c in codes:
        d = digits_only(c)
        base = d[:8] if len(d) >= 8 else d
        base = base.rstrip("0") or base
        prefixes.append(base)
    # de-duplicate longest-first (more specific first)
    prefixes = sorted(set(prefixes), key=lambda x: (-len(x), x))
    return prefixes


ALLOWED_PREFIXES = cpv_prefixes(CPV_CODES)

# Output schema (flat, stable)
FIELDNAMES = [
    # tender meta
    "tender_id",
    "tenderID",
    "dateModified",
    "status",
    "title",
    "procurementMethodType",
    "procuringEntity_name",
    "procuringEntity_edrpou",
    # key dates
    "tenderPeriod_startDate",
    "tenderPeriod_endDate",
    "auctionPeriod_startDate",
    # tender value (if present)
    "tender_value_amount",
    "tender_value_currency",
    # item (one row per matched item)
    "item_id",
    "item_description",
    "cpv_id",
    "cpv_description",
    "item_quantity",
    "item_unit",
    "delivery_endDate",
    "delivery_region",
    "delivery_locality",
    # award (best-effort)
    "award_status",
    "award_date",
    "award_value_amount",
    "award_value_currency",
    "supplier_name",
    "supplier_id",
    # partitioning
    "period_key",
]


@dataclass
class Stats:
    pages: int = 0
    tenders_in_feed: int = 0
    tenders_details_ok: int = 0
    tenders_details_fail: int = 0
    matched_rows: int = 0
    last_offset: str = ""


class CsvPartitionWriter:
    def __init__(self, all_path: Path, monthly_dir: Path, fieldnames: List[str]) -> None:
        self.all_path = all_path
        self.monthly_dir = monthly_dir
        self.fieldnames = fieldnames
        self._all_fh = None
        self._all_writer = None
        self._month_fhs: Dict[str, Any] = {}
        self._month_writers: Dict[str, csv.DictWriter] = {}

    def _open_writer(self, path: Path) -> csv.DictWriter:
        is_new = (not path.exists()) or (path.stat().st_size == 0)
        fh = open(path, "a", newline="", encoding="utf-8")
        writer = csv.DictWriter(fh, fieldnames=self.fieldnames)
        if is_new:
            writer.writeheader()
            fh.flush()
        return fh, writer

    def write_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        if self._all_writer is None:
            fh, wr = self._open_writer(self.all_path)
            self._all_fh, self._all_writer = fh, wr

        for r in rows:
            # write to ALL
            self._all_writer.writerow(r)

            # write to month partition
            mk = r.get("period_key") or "unknown"
            if mk not in self._month_writers:
                path = self.monthly_dir / f"prozorro_dairy_items_{mk}.csv"
                fh, wr = self._open_writer(path)
                self._month_fhs[mk] = fh
                self._month_writers[mk] = wr
            self._month_writers[mk].writerow(r)

        self.flush()

    def flush(self) -> None:
        if self._all_fh:
            self._all_fh.flush()
        for fh in self._month_fhs.values():
            fh.flush()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            if self._all_fh:
                self._all_fh.close()
            for fh in self._month_fhs.values():
                fh.close()


def parse_start_offset(logger: logging.Logger) -> str:
    if START_OFFSET:
        return START_OFFSET

    # Start from START_DATE at 00:00 UTC -> feed offset accepts date-time / timestamp-like offsets in practice.
    # We use ISO string (safe) rather than float to avoid formatting issues.
    dt = datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc)
    # Some feeds accept ISO offset as well; if your API expects numeric, replace with dt.timestamp().
    return dt.isoformat()


def request_json(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    max_retries: int = 8,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                sleep_s = float(ra) if ra and ra.isdigit() else backoff
                if logger:
                    logger.warning("429 Too Many Requests. Sleep %.1fs (attempt %d/%d)", sleep_s, attempt, max_retries)
                time.sleep(sleep_s)
                backoff = min(backoff * 2, 60)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == max_retries:
                raise
            if logger:
                logger.warning("Request failed: %s | attempt %d/%d | sleep %.1fs", str(e), attempt, max_retries, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
    raise RuntimeError("Unreachable")


def cpv_matches(cpv_id: str) -> bool:
    d = digits_only(cpv_id)
    base = d[:8] if len(d) >= 8 else d
    for pref in ALLOWED_PREFIXES:
        if base.startswith(pref):
            return True
    return False


def extract_rows(tender: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    t_id = tender.get("id", "")
    tenderID = tender.get("tenderID", "")
    dateModified = tender.get("dateModified", "")
    status = tender.get("status", "")
    title = tender.get("title", "")
    pmt = tender.get("procurementMethodType", "")

    pe = tender.get("procuringEntity") or {}
    pe_name = pe.get("name", "")
    pe_ident = pe.get("identifier") or {}
    pe_edrpou = pe_ident.get("id", "")

    tenderPeriod = tender.get("tenderPeriod") or {}
    tp_start = tenderPeriod.get("startDate", "")
    tp_end = tenderPeriod.get("endDate", "")

    auctionPeriod = tender.get("auctionPeriod") or {}
    auc_start = auctionPeriod.get("startDate", "")

    value = tender.get("value") or {}
    t_val_amount = value.get("amount", "")
    t_val_curr = value.get("currency", "")

    # awards (best effort)
    award_status = ""
    award_date = ""
    award_value_amount = ""
    award_value_currency = ""
    supplier_name = ""
    supplier_id = ""
    awards = tender.get("awards") or []
    if isinstance(awards, list) and awards:
        # choose "active" first, else first
        aw = next((a for a in awards if a.get("status") == "active"), awards[0])
        award_status = aw.get("status", "")
        award_date = aw.get("date", "") or aw.get("dateModified", "")
        aw_val = aw.get("value") or {}
        award_value_amount = aw_val.get("amount", "")
        award_value_currency = aw_val.get("currency", "")
        suppliers = aw.get("suppliers") or []
        if suppliers:
            s0 = suppliers[0]
            supplier_name = s0.get("name", "")
            sid = (s0.get("identifier") or {}).get("id", "")
            supplier_id = sid

    items = tender.get("items") or []
    if not isinstance(items, list) or not items:
        return rows

    for it in items:
        cls = it.get("classification") or {}
        cpv_id = cls.get("id", "")
        cpv_desc = cls.get("description", "")

        # also check additionalClassifications if main is not CPV-like
        ok = False
        if cpv_id and cpv_matches(cpv_id):
            ok = True
        else:
            add_cls = it.get("additionalClassifications") or []
            if isinstance(add_cls, list):
                for ac in add_cls:
                    acid = (ac or {}).get("id", "")
                    if acid and cpv_matches(acid):
                        ok = True
                        cpv_id = acid
                        cpv_desc = (ac or {}).get("description", "")
                        break

        if not ok:
            continue

        item_id = it.get("id", "")
        item_desc = it.get("description", "")
        qty = it.get("quantity", "")
        unit = (it.get("unit") or {}).get("name", "")

        delivery = it.get("deliveryDate") or {}
        delivery_end = delivery.get("endDate", "") or delivery.get("startDate", "")

        addr = it.get("deliveryAddress") or {}
        delivery_region = addr.get("region", "")
        delivery_locality = addr.get("locality", "")

        # period key for partitioning: prioritize auction date, else dateModified
        period_key = iso_to_month_key(auc_start or dateModified or tp_start)

        rows.append({
            "tender_id": t_id,
            "tenderID": tenderID,
            "dateModified": dateModified,
            "status": status,
            "title": title,
            "procurementMethodType": pmt,
            "procuringEntity_name": pe_name,
            "procuringEntity_edrpou": pe_edrpou,
            "tenderPeriod_startDate": tp_start,
            "tenderPeriod_endDate": tp_end,
            "auctionPeriod_startDate": auc_start,
            "tender_value_amount": t_val_amount,
            "tender_value_currency": t_val_curr,
            "item_id": item_id,
            "item_description": item_desc,
            "cpv_id": cpv_id,
            "cpv_description": cpv_desc,
            "item_quantity": qty,
            "item_unit": unit,
            "delivery_endDate": delivery_end,
            "delivery_region": delivery_region,
            "delivery_locality": delivery_locality,
            "award_status": award_status,
            "award_date": award_date,
            "award_value_amount": award_value_amount,
            "award_value_currency": award_value_currency,
            "supplier_name": supplier_name,
            "supplier_id": supplier_id,
            "period_key": period_key,
        })

    return rows


def fetch_tender_detail(session: requests.Session, tender_id: str, logger: logging.Logger) -> Tuple[str, Optional[List[Dict[str, Any]]], Optional[str]]:
    url = f"{BASE_URL}/tenders/{tender_id}"
    # opt_fields helps reduce payload; supported by API :contentReference[oaicite:8]{index=8}
    params = {
        "opt_fields": ",".join([
            "id", "tenderID", "dateModified", "status", "title", "procurementMethodType",
            "procuringEntity", "tenderPeriod", "auctionPeriod", "value",
            "items", "awards"
        ])
    }
    try:
        data = request_json(session, url, params=params, logger=logger)
        tender = data.get("data") or {}
        rows = extract_rows(tender)
        return tender_id, rows, None
    except Exception as e:
        return tender_id, None, str(e)


def main() -> None:
    mkdirs()
    logger = setup_logger()

    logger.info("Log file: %s", str(LOG_PATH))
    logger.info("BASE_URL=%s", BASE_URL)
    logger.info("ALLOWED_PREFIXES=%s", ALLOWED_PREFIXES)
    logger.info("LIMIT=%s | DETAIL_WORKERS=%s | MAX_RUNTIME_MINUTES=%s | SAVE_EVERY_SECONDS=%s",
                LIMIT, DETAIL_WORKERS, MAX_RUNTIME_MINUTES, SAVE_EVERY_SECONDS)

    checkpoint = load_checkpoint()
    offset = checkpoint.get("offset") or parse_start_offset(logger)
    stats = Stats(last_offset=str(offset))

    logger.info("Start offset=%s", offset)

    session = requests.Session()
    session.headers.update({"User-Agent": f"prozzoro-dairy-fetch/{RUN_ID}"})

    writer = CsvPartitionWriter(ALL_CSV_PATH, MONTHLY_DIR, FIELDNAMES)

    start_mon = time.monotonic()
    deadline = start_mon + MAX_RUNTIME_MINUTES * 60
    last_save = start_mon

    try:
        while True:
            now = time.monotonic()
            if now >= deadline - 90:  # leave a bit of time buffer
                logger.warning("Time budget reached. Stopping gracefully to allow upload/commit steps.")
                break

            feed_url = f"{BASE_URL}/tenders"
            params = {"offset": offset, "limit": LIMIT}
            feed = request_json(session, feed_url, params=params, logger=logger)

            tenders = feed.get("data") or []
            next_page = feed.get("next_page") or {}
            next_offset = next_page.get("offset") or ""

            if not tenders:
                logger.info("No tenders returned. offset=%s. Done.", offset)
                break

            stats.pages += 1
            stats.tenders_in_feed += len(tenders)

            ids = [t.get("id") for t in tenders if t.get("id")]
            logger.info("Progress | pages=%d | batch=%d | offset=%s", stats.pages, len(ids), offset)

            batch_rows: List[Dict[str, Any]] = []
            with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
                futs = [ex.submit(fetch_tender_detail, session, tid, logger) for tid in ids]
                for fut in as_completed(futs):
                    tid, rows, err = fut.result()
                    if err:
                        stats.tenders_details_fail += 1
                        logger.warning("Detail fail tender_id=%s err=%s", tid, err)
                        continue
                    stats.tenders_details_ok += 1
                    if rows:
                        batch_rows.extend(rows)

            if batch_rows:
                writer.write_rows(batch_rows)
                stats.matched_rows += len(batch_rows)

            # Advance offset only after processing & writing this page
            if not next_offset or str(next_offset) == str(offset):
                logger.info("No next_offset or unchanged. Stopping. next_offset=%s", str(next_offset))
                break

            offset = str(next_offset)
            stats.last_offset = offset

            # periodic checkpoint + summary
            now = time.monotonic()
            if now - last_save >= SAVE_EVERY_SECONDS:
                ck = {
                    "offset": offset,
                    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "stats": {
                        "pages": stats.pages,
                        "tenders_in_feed": stats.tenders_in_feed,
                        "tenders_details_ok": stats.tenders_details_ok,
                        "tenders_details_fail": stats.tenders_details_fail,
                        "matched_rows": stats.matched_rows,
                    },
                }
                atomic_write_json(CHECKPOINT_PATH, ck)
                logger.info("Checkpoint saved: %s", str(CHECKPOINT_PATH))
                last_save = now

        # final checkpoint
        ck = {
            "offset": offset,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "stats": {
                "pages": stats.pages,
                "tenders_in_feed": stats.tenders_in_feed,
                "tenders_details_ok": stats.tenders_details_ok,
                "tenders_details_fail": stats.tenders_details_fail,
                "matched_rows": stats.matched_rows,
            },
            "run_id": RUN_ID,
        }
        atomic_write_json(CHECKPOINT_PATH, ck)
        logger.info("Final checkpoint saved: %s", str(CHECKPOINT_PATH))
        logger.info("DONE | pages=%d | tenders_in_feed=%d | details_ok=%d | details_fail=%d | matched_rows=%d",
                    stats.pages, stats.tenders_in_feed, stats.tenders_details_ok, stats.tenders_details_fail, stats.matched_rows)

    finally:
        writer.close()


if __name__ == "__main__":
    main()
