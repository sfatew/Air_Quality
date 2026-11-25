"""
China national-network (CNEMC 国控站点) hourly PM2.5 — china_crawl.py
=====================================================================
Source: https://quotsoft.net/air/ — daily mirror of the CNEMC national
real-time platform (the same network GTWNN / GTW-Tree / ASTMNN / STLG use),
one file per day since 2014-05-13, wide format
    date, hour, type, <station_code_1>, <station_code_2>, ...
with 15 `type` rows per hour (PM2.5, PM2.5_24h, PM10, SO2, ... AQI).

  1. Download china_sites_YYYYMMDD.csv → raw/YYYY/china_sites_YYYYMMDD.csv.gz
     (all pollutants kept; resume-safe: existing days skipped, the last
     REFRESH_DAYS re-fetched because the newest file may still be partial)
  2. Extract type == "PM2.5" → pm25/china_pm25_YYYY.csv.gz, long format
         time_bjt, station_code, pm25        (µg/m³, NaN rows dropped)

time_bjt is the CNEMC time point in Beijing time (UTC+8), i.e. the end of the
1-h averaging window. Station coordinates are NOT in these files; build them
with china_stations.py after the 2023 PM2.5 table exists.

Usage:
    python china_crawl.py --start 2020-01-01            # end = yesterday (BJT)
    python china_crawl.py --start 2020-01-01 --end 2020-12-31 --workers 4
"""

import argparse
import concurrent.futures
import csv
import gzip
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

URL = "https://quotsoft.net/air/data/china_sites_{}.csv"
OUTPUT_DIR = Path("/home/slow_data/Air_Quality/AQI/china")
HEADERS = {"User-Agent": "Mozilla/5.0 (aqi-china-crawl)"}

DEFAULT_WORKERS = 4           # single personal server — keep it polite
DEFAULT_TIMEOUT = 60
REFRESH_DAYS = 2
BJT = timezone(timedelta(hours=8))


def make_session(workers: int) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=5, backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=workers))
    return s


def raw_path(raw_dir: Path, day: date) -> Path:
    return raw_dir / f"{day:%Y}" / f"china_sites_{day:%Y%m%d}.csv.gz"


def download_day(session: requests.Session, day: date, raw_dir: Path,
                 timeout: int) -> str:
    dst = raw_path(raw_dir, day)
    try:
        r = session.get(URL.format(f"{day:%Y%m%d}"), timeout=timeout)
    except requests.RequestException as e:
        logger.error("%s: %s", day, e)
        return "error"
    if r.status_code == 404:
        return "missing"
    if r.status_code != 200:
        logger.error("%s: HTTP %d", day, r.status_code)
        return "error"
    if not r.content.lstrip(b"\xef\xbb\xbf").startswith(b"date,hour,type"):
        logger.error("%s: unexpected payload (%d bytes)", day, len(r.content))
        return "error"

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    with gzip.open(tmp, "wb", compresslevel=6) as f:
        f.write(r.content)
    tmp.replace(dst)
    return "ok"


def download_range(start: date, end: date, raw_dir: Path, workers: int,
                   timeout: int) -> set[int]:
    """Download every missing day in [start, end]. Returns years that changed."""
    days = [start + timedelta(d) for d in range((end - start).days + 1)]
    refresh_from = end - timedelta(days=REFRESH_DAYS - 1)
    todo = [d for d in days
            if d >= refresh_from or not raw_path(raw_dir, d).exists()]
    logger.info("%d days in range, %d to download", len(days), len(todo))

    session = make_session(workers)
    stats = {"ok": 0, "missing": 0, "error": 0}
    missing: list[date] = []
    changed_years: set[int] = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(download_day, session, d, raw_dir, timeout): d
                   for d in todo}
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            d = futures[fut]
            status = fut.result()
            stats[status] += 1
            if status == "ok":
                changed_years.add(d.year)
            elif status == "missing":
                missing.append(d)
            if i % 100 == 0 or i == len(todo):
                logger.info("download %d/%d  %s", i, len(todo), stats)

    if missing:
        logger.warning("%d days not on server: %s", len(missing),
                       ", ".join(f"{d:%Y-%m-%d}" for d in sorted(missing)))
    return changed_years


def read_pm25_day(path: Path) -> pd.DataFrame:
    """One raw day → wide PM2.5 frame (index time_bjt, columns station codes)."""
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        header = next(csv.reader(fh))
    # Some days list a code twice (identical values); pandas would mangle the
    # copy to "1961A.1", so keep only the first occurrence.
    first: dict[str, int] = {}
    for i, c in enumerate(header):
        first.setdefault(c, i)
    df = pd.read_csv(path, dtype={"date": str}, usecols=sorted(first.values()))
    df = df[df["type"] == "PM2.5"]
    idx = (pd.to_datetime(df["date"], format="%Y%m%d")
           + pd.to_timedelta(df["hour"], unit="h"))
    wide = df.drop(columns=["date", "hour", "type"]).set_axis(idx, axis=0)
    wide.index.name = "time_bjt"
    wide.columns.name = "station_code"
    return wide


def build_pm25_year(year: int, raw_dir: Path, pm25_dir: Path) -> None:
    files = sorted((raw_dir / str(year)).glob("china_sites_*.csv.gz"))
    if not files:
        return
    wide = pd.concat([read_pm25_day(f) for f in files], axis=0)
    long = (wide.stack(future_stack=True).dropna()
            .rename("pm25").reset_index())

    pm25_dir.mkdir(parents=True, exist_ok=True)
    dst = pm25_dir / f"china_pm25_{year}.csv.gz"
    tmp = dst.with_name(dst.name + ".tmp")
    long.to_csv(tmp, index=False, float_format="%g", compression="gzip")
    tmp.replace(dst)
    logger.info("%d: %d days, %d stations, %d rows → %s",
                year, len(files), wide.shape[1], len(long), dst)


def run_china_crawl(start: str, end: str | None = None,
                    output_dir: Path = OUTPUT_DIR,
                    workers: int = DEFAULT_WORKERS,
                    timeout: int = DEFAULT_TIMEOUT) -> None:
    start_d = datetime.strptime(start, "%Y-%m-%d").date()
    end_d = (datetime.strptime(end, "%Y-%m-%d").date() if end
             else datetime.now(BJT).date() - timedelta(days=1))
    raw_dir, pm25_dir = output_dir / "raw", output_dir / "pm25"
    logger.info("Period: %s → %s  |  output %s", start_d, end_d, output_dir)

    changed = download_range(start_d, end_d, raw_dir, workers, timeout)
    years = sorted(y for y in range(start_d.year, end_d.year + 1)
                   if y in changed
                   or not (pm25_dir / f"china_pm25_{y}.csv.gz").exists())
    for y in years:
        build_pm25_year(y, raw_dir, pm25_dir)
    logger.info("Done. PM2.5 tables rebuilt for %s", years or "none")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--start", default="2020-01-01")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD, default yesterday (BJT)")
    ap.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    a = ap.parse_args()
    run_china_crawl(a.start, a.end, a.output_dir, a.workers, a.timeout)
