"""
tedp.vn AQI Downloader — crawl.py
==================================
Mirrors the VIIRS/MODIS crawl pattern. For each station in
Masterdata/envisoft_station_map.csv:
  1. List records via /api/data_hour/search/findByStationId... ([start, end])
  2. Diff vs existing per-station CSV (resume-safe by Record_ID)
  3. Fetch /api/data_hour/{record_id} per new ID (parallel, rate-limited)
  4. Parse with AQI.detail_parser → append to {output_dir}/<cleanName>.csv

Output: /home/slow_data/Air_Quality/AQI/<cleanName>.csv
"""

import sys
import csv as csv_mod
import time
import logging
import concurrent.futures
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Optional

import requests
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from detail_parser import parse_detail, OUTPUT_COLUMNS

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LIST_URL = (
    "https://tedp.vn/api/data_hour/search/"
    "findByStationIdAndGetTimeBetweenOrderByGetTimeDesc"
)
DETAIL_URL = "https://tedp.vn/api/data_hour/{}"

STATION_CSV = Path("/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv")
OUTPUT_DIR = Path("/home/slow_data/Air_Quality/AQI")

HEADERS = {"User-Agent": "Mozilla/5.0 (aqi-crawl)"}

DEFAULT_WORKERS = 16          # per-station detail-fetch threads
DEFAULT_STATION_WORKERS = 4   # stations crawled concurrently
DEFAULT_RATE_PER_SEC = 30.0   # total req/s across all worker threads
DEFAULT_TIMEOUT = 30
LIST_PAGE_SIZE = 100000       # request a large page to avoid pagination


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def clean_station_name(name: str) -> str:
    return (
        name.replace(":", "")
        .replace("/", "_")
        .replace("\\", "_")
        .strip()
    )


def load_stations(csv_path: Path = STATION_CSV) -> list[dict]:
    df = pd.read_csv(csv_path)
    df["stationId"] = df["stationId"].astype(str)
    return df.to_dict("records")


def extract_record_id(item: dict) -> str:
    href = item.get("_links", {}).get("self", {}).get("href", "")
    return href.split("/")[-1] if href else ""


# ---------------------------------------------------------------------------
# HTTPS client (shared rate limiter across threads)
# ---------------------------------------------------------------------------
class TedpClient:
    def __init__(self, rate_per_sec: float = 0.0):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        self._last = 0.0
        self._lock = Lock()

    def _throttle(self):
        if self.interval <= 0:
            return
        with self._lock:
            now = time.time()
            wait = self.interval - (now - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()

    def list_records(
        self,
        station_id: str,
        start_iso: str,
        end_iso: str,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> list[dict]:
        params = {
            "stationId": station_id,
            "from": start_iso,
            "to": end_iso,
            "size": LIST_PAGE_SIZE,
        }
        self._throttle()
        r = self.session.get(LIST_URL, params=params, timeout=timeout)
        r.raise_for_status()
        return (r.json() or {}).get("_embedded", {}).get("data_hour", [])

    def fetch_detail(
        self,
        record_id: str,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> tuple[Optional[dict], Optional[str]]:
        """Returns (row_dict, error_str). row_dict is None on failure."""
        self._throttle()
        try:
            r = self.session.get(DETAIL_URL.format(record_id), timeout=timeout)
        except requests.exceptions.Timeout:
            return None, "Timeout"
        except Exception as e:
            return None, f"Exc:{e}"

        if r.status_code == 200:
            try:
                j = r.json()
            except Exception:
                return None, "ParseErr"
            row, _unknown = parse_detail(j)
            row["Detailed_Status"] = "Done"
            return row, None
        if r.status_code == 429:
            return None, "429"
        return None, f"Error_{r.status_code}"


# ---------------------------------------------------------------------------
# Per-station CSV writer with resume-by-Record_ID
# ---------------------------------------------------------------------------
class StationCSVWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._existing_ids: dict[Path, set] = {}
        self._latest_ts: dict[Path, str] = {}
        self._cache_lock = Lock()

    def path_for(self, clean_name: str) -> Path:
        return self.output_dir / f"{clean_name}.csv"

    def _load_cache(self, path: Path):
        ids: set = set()
        latest = ""
        if path.exists() and path.stat().st_size > 0:
            try:
                with open(path, "r", encoding="utf-8", newline="") as f:
                    reader = csv_mod.DictReader(f)
                    for r in reader:
                        rid = (r.get("Record_ID") or "").strip()
                        if rid:
                            ids.add(rid)
                        ts = (r.get("Timestamp") or "").strip()
                        if ts and ts > latest:
                            latest = ts
            except Exception as e:
                logger.warning("Failed reading %s: %s", path, e)
        self._existing_ids[path] = ids
        self._latest_ts[path] = latest

    def existing_record_ids(self, path: Path) -> set:
        with self._cache_lock:
            if path not in self._existing_ids:
                self._load_cache(path)
            return self._existing_ids[path]

    def latest_timestamp(self, path: Path) -> str:
        with self._cache_lock:
            if path not in self._existing_ids:
                self._load_cache(path)
            return self._latest_ts.get(path, "")

    def append_rows(self, path: Path, rows: list[dict]) -> int:
        if not rows:
            return 0
        write_header = not (path.exists() and path.stat().st_size > 0)
        norm = [{c: r.get(c, "") for c in OUTPUT_COLUMNS} for r in rows]
        with open(path, "a", encoding="utf-8", newline="") as f:
            w = csv_mod.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
            if write_header:
                w.writeheader()
            w.writerows(norm)
        with self._cache_lock:
            cache = self._existing_ids.setdefault(path, set())
            latest = self._latest_ts.get(path, "")
            for r in rows:
                rid = (r.get("Record_ID") or "").strip()
                if rid:
                    cache.add(rid)
                ts = (r.get("Timestamp") or "").strip()
                if ts and ts > latest:
                    latest = ts
            self._latest_ts[path] = latest
        return len(rows)


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------
def run_aqi_crawl(
    start: str,
    end: str = None,
    output_dir: str = str(OUTPUT_DIR),
    station_csv: str = str(STATION_CSV),
    workers: int = DEFAULT_WORKERS,
    station_workers: int = DEFAULT_STATION_WORKERS,
    rate_per_sec: float = DEFAULT_RATE_PER_SEC,
    timeout: int = DEFAULT_TIMEOUT,
    incremental: bool = True,
) -> dict:
    """
    Crawl tedp.vn AQI data for all stations from `start` (YYYY-MM-DD) to
    `end` (YYYY-MM-DD, default = now).

    Writes one CSV per station under `output_dir`. Resume-safe: existing
    Record_IDs in the output CSV are skipped on re-runs.

    Speed knobs:
      * `rate_per_sec`     — global API request rate (shared cap, all threads).
      * `workers`          — concurrent detail fetches per station.
      * `station_workers`  — number of stations crawled in parallel.
      * `incremental=True` — re-runs query only since the latest on-disk
                             Timestamp - 1 day, avoiding 30k-row list calls.
    """
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d") if end else datetime.now()
    start_iso = start_dt.strftime("%Y-%m-%dT00:00:00")
    end_iso = end_dt.strftime("%Y-%m-%dT23:59:59")
    logger.info("Period: %s → %s", start_iso, end_iso)

    stations = load_stations(Path(station_csv))
    logger.info(
        "Loaded %d stations  |  rate=%.1f req/s  detail_workers=%d  station_workers=%d",
        len(stations), rate_per_sec, workers, station_workers,
    )

    writer = StationCSVWriter(Path(output_dir))
    client = TedpClient(rate_per_sec=rate_per_sec)

    run_stats = {
        "stations": len(stations),
        "records_listed": 0,
        "records_new": 0,
        "records_written": 0,
        "list_errors": [],
        "detail_errors": 0,
    }
    stats_lock = Lock()

    def _effective_start(path: Path) -> str:
        if not incremental:
            return start_iso
        latest = writer.latest_timestamp(path)
        if not latest:
            return start_iso
        try:
            # Timestamp is written as ISO "YYYY-MM-DDTHH:MM:SS"
            dt = datetime.fromisoformat(latest.replace("Z", ""))
        except Exception:
            return start_iso
        eff = (dt - timedelta(days=1))
        return max(eff, start_dt).strftime("%Y-%m-%dT%H:%M:%S")

    def _process_station(st: dict) -> None:
        sid = str(st["stationId"])
        sname = str(st["stationName"])
        clean = clean_station_name(sname)
        path = writer.path_for(clean)
        existing = writer.existing_record_ids(path)
        eff_start = _effective_start(path)

        logger.info("[%s] listing from %s …", sname, eff_start)
        try:
            items = client.list_records(sid, eff_start, end_iso, timeout=timeout)
        except Exception as e:
            logger.error("[%s] list failed: %s", sname, e)
            with stats_lock:
                run_stats["list_errors"].append({"station": sname, "reason": str(e)})
            return

        new_ids = []
        for it in items:
            rid = extract_record_id(it)
            if rid and rid not in existing:
                new_ids.append(rid)

        with stats_lock:
            run_stats["records_listed"] += len(items)
            run_stats["records_new"] += len(new_ids)

        logger.info(
            "[%s] %d listed, %d new (skipping %d already on disk)",
            sname, len(items), len(new_ids), len(items) - len(new_ids),
        )
        if not new_ids:
            return

        rows: list[dict] = []
        local_errors = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {
                ex.submit(client.fetch_detail, rid, timeout): rid
                for rid in new_ids
            }
            for fut in concurrent.futures.as_completed(futures):
                rid = futures[fut]
                row, err = fut.result()
                if err is not None:
                    local_errors += 1
                    if err == "429":
                        logger.warning("[%s] 429 on %s — backing off", sname, rid)
                        time.sleep(3)
                    continue
                if row is not None:
                    rows.append(row)

        written = writer.append_rows(path, rows)
        with stats_lock:
            run_stats["records_written"] += written
            run_stats["detail_errors"] += local_errors
        logger.info("[%s] wrote %d rows → %s", sname, written, path)

    if station_workers <= 1:
        for st in stations:
            _process_station(st)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=station_workers) as ex:
            futures = [ex.submit(_process_station, st) for st in stations]
            for f in concurrent.futures.as_completed(futures):
                exc = f.exception()
                if exc is not None:
                    logger.error("station task crashed: %s", exc)

    logger.info(
        "Done. listed=%d new=%d written=%d list_err=%d detail_err=%d",
        run_stats["records_listed"], run_stats["records_new"],
        run_stats["records_written"], len(run_stats["list_errors"]),
        run_stats["detail_errors"],
    )
    return run_stats


# ===========================================================================
# RUN
# ===========================================================================
if __name__ == "__main__":
    run_aqi_crawl(
        start="2022-01-01",
        end=None,
        output_dir=str(OUTPUT_DIR),
    )
