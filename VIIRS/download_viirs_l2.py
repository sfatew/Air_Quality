"""
VIIRS AOD L2 Downloader — Vietnam
===================================
Downloads AERDB_L2_VIIRS_NOAA20 and AERDB_L2_VIIRS_SNPP granules that
intersect Vietnam from NASA LAADS DAAC using CMR spatial filtering.

Folder structure mirrors the MODIS convention:
    /home/slow_data/Air_Quality/VIIRS/{SHORT_NAME}/{year}/{doy}/*.nc

  e.g.
    …/AERDB_L2_VIIRS_NOAA20/2024/092/AERDB_L2_VIIRS_NOAA20.A2024092.0330.002.nc
    …/AERDB_L2_VIIRS_SNPP/2024/092/AERDB_L2_VIIRS_SNPP.A2024092.0318.002.nc

Usage:
    # Download both products from a start date until today (minus lag):
    python download_viirs_l2.py --start 2022-01-01

    # Download a specific date range for one product:
    python download_viirs_l2.py --start 2024-01-01 --end 2024-03-31 --product NOAA20

    # Historical fill + continuous daily updates (runs forever):
    python download_viirs_l2.py --start 2022-01-01 --continuous
"""

import argparse
import concurrent.futures
import csv
import os
import sys
import time
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import yaml

# ---------------------------------------------------------------------------
# Silence SSL warnings for servers that need verify=False
# ---------------------------------------------------------------------------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Load token from shared config
# ---------------------------------------------------------------------------
_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
with open(_CONFIG_PATH, "r") as _f:
    _cfg = yaml.safe_load(_f)
TOKEN = _cfg["VIIRS"]["TOKEN"]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LAADS_BASE_URL = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData"
CMR_SEARCH_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"

# Vietnam bounding box (min_lon, min_lat, max_lon, max_lat)
# Slightly expanded to catch swaths that clip the border
VIETNAM_BBOX = (102.170435826, 8.59975962975, 109.33526981, 23.3520633001)

PRODUCTS = {
    "NOAA20": {
        "short_name": "AERDB_L2_VIIRS_NOAA20",
        "collection": "5200",
    },
    "SNPP": {
        "short_name": "AERDB_L2_VIIRS_SNPP",
        "collection": "5200",
    },
}

DOWNLOAD_BASE = Path("/home/slow_data/Air_Quality/VIIRS/L2")

# LAADS typically takes 1–3 days to publish; stay this far behind today
DAILY_UPDATE_LAG_DAYS = 3

# Seconds between scheduled wake-ups in continuous mode
CHECK_INTERVAL = 24 * 60 * 60  # 24 hours

MAX_CONCURRENT_DOWNLOADS = 8
SSL_VERIFY = False

USERAGENT = "viirs_downloader/1.0--" + sys.version.replace("\n", "").replace("\r", "")

# ---------------------------------------------------------------------------
# HTTP session with retry + auth
# ---------------------------------------------------------------------------
http_session = requests.Session()
http_session.headers.update({
    "user-agent": USERAGENT,
    "Authorization": f"Bearer {TOKEN}",
})
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(
    max_retries=retries,
    pool_connections=MAX_CONCURRENT_DOWNLOADS,
    pool_maxsize=MAX_CONCURRENT_DOWNLOADS,
)
http_session.mount("https://", adapter)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------
def make_date_url(short_name: str, collection: str, date_obj: datetime) -> str:
    """Build the LAADS DAAC folder URL for a given product + date."""
    year = date_obj.strftime("%Y")
    doy  = date_obj.strftime("%j")   # zero-padded 3-digit day-of-year
    return f"{LAADS_BASE_URL}/{collection}/{short_name}/{year}/{doy}"


def local_dir(short_name: str, date_obj: datetime) -> Path:
    """Return (and create) the local directory for a given product + date."""
    year = date_obj.strftime("%Y")
    doy  = date_obj.strftime("%j")
    path = DOWNLOAD_BASE / short_name / year / doy
    path.mkdir(parents=True, exist_ok=True)
    return path


# ---------------------------------------------------------------------------
# File listing
# ---------------------------------------------------------------------------
def list_remote_files(url: str) -> list[dict]:
    """
    Retrieve the file listing for a LAADS DAAC date folder.
    Tries CSV first, falls back to JSON.
    Returns a list of dicts with at least a 'name' key.
    """
    print(f"🔎 Listing files from {url}")
    try:
        resp = http_session.get(f"{url}.csv", verify=SSL_VERIFY, timeout=30)
        resp.raise_for_status()
        return list(csv.DictReader(StringIO(resp.text), skipinitialspace=True))
    except requests.exceptions.HTTPError:
        pass
    try:
        resp = http_session.get(f"{url}.json", verify=SSL_VERIFY, timeout=30)
        resp.raise_for_status()
        return resp.json().get("content", [])
    except Exception as exc:
        print(f"❌ Failed to list {url}: {exc}")
        return []


# ---------------------------------------------------------------------------
# CMR spatial filter
# ---------------------------------------------------------------------------
def get_vietnam_granule_names(short_name: str, date_obj: datetime) -> set[str] | None:
    """
    Query NASA CMR for granule names whose footprint intersects Vietnam.
    Returns a set of .nc filenames, or None on failure (caller should
    fall back to downloading all files for that day).
    """
    min_lon, min_lat, max_lon, max_lat = VIETNAM_BBOX
    date_str      = date_obj.strftime("%Y-%m-%d")
    next_date_str = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

    params = {
        "short_name"   : short_name,
        "temporal[]"   : f"{date_str}T00:00:00Z,{next_date_str}T00:00:00Z",
        "bounding_box" : f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "page_size"    : 2000,
        "downloadable" : "true",
    }

    try:
        # CMR is a public API — no Bearer token needed
        resp = requests.get(CMR_SEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()
        entries = resp.json().get("feed", {}).get("entry", [])

        names = set()
        for entry in entries:
            pgid = entry.get("producer_granule_id", "").strip()
            if pgid:
                names.add(pgid if pgid.endswith(".nc") else pgid + ".nc")

        print(
            f"🌏 CMR: {len(names)} {short_name} granules intersect Vietnam "
            f"on {date_obj.strftime('%Y-%m-%d')}"
        )
        return names

    except Exception as exc:
        print(f"⚠️  CMR query failed ({exc}). Will download all granules for this day.")
        return None


def filter_nc_files(
    all_files: list[dict],
    vietnam_names: set[str] | None,
) -> list[dict]:
    """Keep only .nc files, optionally restricted to those in `vietnam_names`."""
    nc_files = [f for f in all_files if f.get("name", "").endswith(".nc")]

    if vietnam_names is None:
        print(f"⚠️  No spatial filter — downloading all {len(nc_files)} .nc files.")
        return nc_files

    filtered = [f for f in nc_files if f.get("name", "") in vietnam_names]
    print(f"🔍 Spatial filter: {len(filtered)} / {len(nc_files)} granules cover Vietnam.")
    return filtered


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
def _download_one(file_info: dict, dest_dir: Path, base_url: str) -> bool:
    """Download a single granule; return True on success."""
    filename = file_info["name"]
    dest     = dest_dir / filename

    if dest.exists() and dest.stat().st_size > 0:
        return True   # already have it

    url = f"{base_url}/{filename}"
    try:
        with http_session.get(url, verify=SSL_VERIFY, stream=True, timeout=300) as resp:
            resp.raise_for_status()

            # Guard against accidentally saving an HTML error page
            ctype = resp.headers.get("Content-Type", "")
            if "html" in ctype or "text/plain" in ctype:
                print(f"  ❌ {filename}: server returned HTML/text — check token")
                return False

            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)

        # Sanity-check: NetCDF4/HDF5 magic bytes
        with open(dest, "rb") as fh:
            magic = fh.read(4)
        if magic[:3] not in (b"CDF", b"\x89HD"):
            print(f"  ❌ {filename}: not a valid NetCDF/HDF file — deleting")
            dest.unlink(missing_ok=True)
            return False

        print(f"  ✅ {filename}  ({dest.stat().st_size / 1e6:.1f} MB)")
        return True

    except Exception as exc:
        print(f"  ❌ {filename}: {exc}", file=sys.stderr)
        dest.unlink(missing_ok=True)
        return False


def download_for_date(short_name: str, collection: str, date_obj: datetime) -> int:
    """
    Download all Vietnam-intersecting granules for one product on one date.
    Returns the number of files successfully downloaded (0 = nothing new).
    Raises FileNotFoundError when LAADS has no data at all for that day.
    """
    base_url    = make_date_url(short_name, collection, date_obj)
    all_files   = list_remote_files(base_url)

    if not all_files:
        raise FileNotFoundError(f"No files listed for {short_name} on {date_obj:%Y-%m-%d}")

    viet_names  = get_vietnam_granule_names(short_name, date_obj)
    wanted      = filter_nc_files(all_files, viet_names)

    if not wanted:
        print(f"ℹ️  No granules cover Vietnam for {short_name} on {date_obj:%Y-%m-%d}")
        return 0

    dest_dir = local_dir(short_name, date_obj)

    # Skip files already downloaded
    to_fetch = [f for f in wanted if not (dest_dir / f["name"]).exists()
                                  or (dest_dir / f["name"]).stat().st_size == 0]

    already = len(wanted) - len(to_fetch)
    if already:
        print(f"  ⏭  {already} file(s) already on disk — skipping")

    if not to_fetch:
        return 0

    print(f"⚡ Downloading {len(to_fetch)} file(s) with {MAX_CONCURRENT_DOWNLOADS} threads …")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as pool:
        futures = [pool.submit(_download_one, f, dest_dir, base_url) for f in to_fetch]
        results = [fut.result() for fut in concurrent.futures.as_completed(futures)]

    ok = sum(results)
    print(f"✅ {date_obj:%Y-%m-%d} {short_name}: {ok}/{len(to_fetch)} downloaded")
    return ok


# ---------------------------------------------------------------------------
# Historical + continuous update loop
# ---------------------------------------------------------------------------
def run_historical(
    short_name: str,
    collection: str,
    start_date: datetime,
    end_date: datetime,
) -> datetime:
    """
    Download all dates from start_date to end_date (inclusive).
    Returns the last date attempted.
    """
    date = start_date
    last = start_date - timedelta(days=1)

    while date <= end_date:
        try:
            download_for_date(short_name, collection, date)
        except FileNotFoundError:
            print(f"⚠️  No data for {date:%Y-%m-%d} — skipping.")
        last = date
        date += timedelta(days=1)

    return last


def run_continuous(
    short_name: str,
    collection: str,
    last_checked: datetime,
) -> None:
    """
    Wake up every CHECK_INTERVAL seconds and download any new dates since
    last_checked up to (today − DAILY_UPDATE_LAG_DAYS).
    Runs forever until interrupted.
    """
    while True:
        print(f"\n⏰ Scheduled check at {datetime.now():%Y-%m-%d %H:%M:%S}")
        today  = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff = today - timedelta(days=DAILY_UPDATE_LAG_DAYS)

        date = last_checked + timedelta(days=1)
        while date <= cutoff:
            try:
                download_for_date(short_name, collection, date)
            except FileNotFoundError:
                print(f"⚠️  No data for {date:%Y-%m-%d} — skipping.")
            last_checked = date
            date += timedelta(days=1)

        print(f"✅ Up to date through {last_checked:%Y-%m-%d}")
        print(f"🕐 Sleeping {CHECK_INTERVAL / 3600:.0f} h …\n")
        time.sleep(CHECK_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download VIIRS AERDB L2 (NOAA-20 / SNPP) for Vietnam."
    )
    parser.add_argument(
        "--start",
        default="2022-09-01",
        help="Start date (YYYY-MM-DD). Default: 2022-09-01",
    )
    parser.add_argument(
        "--end",
        default=None,
        help=(
            "End date (YYYY-MM-DD). Default: today minus "
            f"{DAILY_UPDATE_LAG_DAYS}-day processing lag."
        ),
    )
    parser.add_argument(
        "--product",
        choices=list(PRODUCTS.keys()),
        default=None,
        help="Restrict to one platform (default: both NOAA20 and SNPP).",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help=(
            "After the historical fill, keep running and check for new data "
            "every 24 h (runs forever)."
        ),
    )
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    today      = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_date   = (
        datetime.strptime(args.end, "%Y-%m-%d")
        if args.end
        else today - timedelta(days=DAILY_UPDATE_LAG_DAYS)
    )

    products_to_run = (
        {args.product: PRODUCTS[args.product]}
        if args.product
        else PRODUCTS
    )

    for platform, cfg in products_to_run.items():
        short_name = cfg["short_name"]
        collection = cfg["collection"]

        print(
            f"\n🔥 {short_name}: historical download "
            f"{start_date:%Y-%m-%d} → {end_date:%Y-%m-%d}"
        )
        last = run_historical(short_name, collection, start_date, end_date)

        if args.continuous:
            print(f"\n🔄 Switching to continuous update mode for {short_name} …")
            run_continuous(short_name, collection, last)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(0)
