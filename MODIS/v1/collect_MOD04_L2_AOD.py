import os
import sys
# Get the absolute path of the current script's directory
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory's path
parent_dir = os.path.join(current_script_dir, os.pardir)
# Add the parent directory to sys.path
sys.path.append(parent_dir)
# print(sys.path)

import shutil
from datetime import datetime, timedelta
import time
import concurrent.futures
import csv
import json
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Replace your 'from config.config import modis_config' with this: ---
import yaml

yaml_path = os.path.join(parent_dir, 'config', 'config.yaml')
with open(yaml_path, 'r') as file:
    config_data = yaml.safe_load(file)

# Access your modis configuration from the YAML structure
# (Adjust the key 'modis_config' based on how your YAML file is structured)
modis_config = config_data.get('MODIS')

try:
    from StringIO import StringIO   # python2
except ImportError:
    from io import StringIO         # python3

# Suppress the insecure request warning if SSL verification fails and we fallback
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

################################################################################
# CONFIGURATION
################################################################################

PRODUCT = "MOD04_L2"  # Terra MODIS Dark Target / Deep Blue Aerosol (Level 2, swath)
SERVER = f'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/{PRODUCT}'
DOWNLOAD_DIR = r'/home/slow_data/Air_Quality/MODIS_MOD04_L2'
TOKEN = modis_config['TOKEN']

START_DATE_STR = "2022-09-01"
print(f"📅 Using start date: {START_DATE_STR}")

CHECK_INTERVAL = 24 * 60 * 60  # 24 hours in seconds
MAX_CONCURRENT_DOWNLOADS = 8

# Switch from historical to daily-update mode when we reach within this many
# days of today. LAADS DAAC typically takes 1–3 days to process and publish data,
# so staying 3 days behind avoids chasing files that don't exist yet.
DAILY_UPDATE_LAG_DAYS = 3

# Vietnam bounding box (min_lon, min_lat, max_lon, max_lat)
# Slightly expanded to catch swaths that clip the border
VIETNAM_BBOX = (102.170435826, 8.59975962975, 109.33526981, 23.3520633001)

# NASA CMR search endpoint (no auth required for searching)
CMR_SEARCH_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"

################################################################################
# ROBUST HTTP SESSION SETUP
################################################################################

USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n', '').replace('\r', '')

# Create a global session to utilize Connection Pooling (Keep-Alive)
http_session = requests.Session()
http_session.headers.update({
    'user-agent': USERAGENT,
    'Authorization': f'Bearer {TOKEN}'
})

# Add retry logic so network blips don't crash the script
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(
    max_retries=retries,
    pool_connections=MAX_CONCURRENT_DOWNLOADS,
    pool_maxsize=MAX_CONCURRENT_DOWNLOADS
)
http_session.mount('https://', adapter)

SSL_VERIFY = False

################################################################################
# DOWNLOAD LOGIC
################################################################################

def make_url(date_obj):
    """Build the URL for a specific date folder."""
    year = date_obj.strftime("%Y")
    day_of_year = date_obj.strftime("%j")  # 001–366
    return f"{SERVER}/{year}/{day_of_year}"

def list_files(url):
    """
    Get the list of .hdf files at the given URL.
    Returns list of file dictionaries with 'name' and 'size'.
    """
    print(f"🔎 Listing files from {url}")

    try:
        # Try CSV first
        response = http_session.get(f'{url}.csv', verify=SSL_VERIFY, timeout=30)
        response.raise_for_status()

        files = []
        for f in csv.DictReader(StringIO(response.text), skipinitialspace=True):
            files.append(f)
        return files

    except requests.exceptions.HTTPError:
        # Fallback to JSON
        try:
            response = http_session.get(f'{url}.json', verify=SSL_VERIFY, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get('content', [])
        except Exception as e:
            print(f"❌ Failed to retrieve file list from {url}: {e}")
            return []
    except Exception as e:
        print(f"❌ Failed to retrieve file list from {url}: {e}")
        return []

def get_vietnam_granule_names(date_obj):
    """
    Query NASA CMR for MOD04_L2 granule names whose spatial footprint
    intersects the Vietnam bounding box.

    CMR does NOT require authentication for searching — it's a public API.
    Returns a set of HDF filenames (without path) that we should download.
    Returns None on failure so the caller can fall back to downloading all files.
    """
    min_lon, min_lat, max_lon, max_lat = VIETNAM_BBOX
    date_str      = date_obj.strftime("%Y-%m-%d")
    next_date_str = (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

    params = {
        'short_name'   : PRODUCT,
        # Do NOT pass 'version' — it causes CMR to return 0 results for MOD04_L2.
        # The short_name alone resolves to the current Collection 061 correctly.
        'temporal[]'   : f'{date_str}T00:00:00Z,{next_date_str}T00:00:00Z',
        'bounding_box' : f'{min_lon},{min_lat},{max_lon},{max_lat}',
        'page_size'    : 2000,          # MOD04_L2 has ~300 granules/day globally
        'downloadable' : 'true',
    }

    try:
        # Use a plain session (no LADSWEB Bearer token) — CMR is a public API
        resp = requests.get(CMR_SEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()
        entries = resp.json().get('feed', {}).get('entry', [])

        # 'producer_granule_id' is the HDF filename; 'title' is an internal CMR ID
        names = set()
        for entry in entries:
            pgid = entry.get('producer_granule_id', '').strip()
            if pgid:
                names.add(pgid if pgid.endswith('.hdf') else pgid + '.hdf')

        print(f"🌏 CMR found {len(names)} granules intersecting Vietnam "
              f"for {date_obj.strftime('%Y-%m-%d')}")
        return names

    except Exception as e:
        print(f"⚠️  CMR query failed ({e}). Falling back to downloading all granules.")
        return None


def filter_hdf(files, vietnam_names=None):
    """
    Keep only .hdf files.
    MOD04_L2 swath files follow the naming pattern:
      MOD04_L2.AYYYYDDD.HHMM.CCC.YYYYDDDHHMMSS.hdf

    If `vietnam_names` is provided (from CMR), keep only granules in that set.
    If CMR lookup failed (vietnam_names is None), fall back to all .hdf files.
    """
    hdf_files = [f for f in files if f.get('name', '').endswith('.hdf')]

    if vietnam_names is None:
        # CMR unavailable — download everything (safe fallback)
        print(f"⚠️  Spatial filter skipped — downloading all {len(hdf_files)} HDF files.")
        return hdf_files

    filtered = [f for f in hdf_files if f.get('name', '') in vietnam_names]
    print(f"🔍 Spatial filter: {len(filtered)} / {len(hdf_files)} granules cover Vietnam.")
    return filtered

def download_single_file(f, local_path, base_url):
    """Downloads a single file using a stream to save RAM."""
    filename = f['name']
    url = f"{base_url}/{filename}"
    path = os.path.join(local_path, filename)

    try:
        with http_session.get(url, verify=SSL_VERIFY, stream=True, timeout=120) as response:
            response.raise_for_status()
            with open(path, 'wb') as fh:
                for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                    if chunk:
                        fh.write(chunk)
        print(f'  ✅ Downloaded: {filename}')
        return True
    except Exception as e:
        print(f"  ❌ Failed to download {filename}: {str(e)}", file=sys.stderr)
        # Cleanup partial / corrupt files
        if os.path.exists(path):
            os.remove(path)
        return False

def download_files(files, date_obj, base_url):
    """Download the given list of files concurrently, skipping files that already exist."""
    if not files:
        print(f"ℹ️ No {PRODUCT} files found for {date_obj.strftime('%Y-%m-%d')}")
        raise FileNotFoundError("No files found")

    year = date_obj.strftime("%Y")
    day = date_obj.strftime("%j")
    local_path = os.path.join(DOWNLOAD_DIR, year, day)
    os.makedirs(local_path, exist_ok=True)

    files_to_download = []
    for f in files:
        filename = f['name']
        local_file = os.path.join(local_path, filename)
        if os.path.exists(local_file) and os.path.getsize(local_file) > 0:
            pass  # Skip existing
        else:
            files_to_download.append(f)

    if not files_to_download:
        print(f"✅ All {len(files)} files already exist for {date_obj.strftime('%Y-%m-%d')}")
        return

    print(f"⚡ Downloading {len(files_to_download)} new files concurrently using {MAX_CONCURRENT_DOWNLOADS} threads...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as executor:
        futures = [
            executor.submit(download_single_file, f, local_path, base_url)
            for f in files_to_download
        ]
        concurrent.futures.wait(futures)

    print(f"✅ Finished downloads for {date_obj.strftime('%Y-%m-%d')}")

def download_for_date(date_obj):
    """Download MOD04_L2 swath files that cover Vietnam for a specific date."""
    url = make_url(date_obj)

    try:
        all_files     = list_files(url)
        # Ask CMR which granules intersect Vietnam BEFORE touching the HDF files
        vietnam_names = get_vietnam_granule_names(date_obj)
        wanted_files  = filter_hdf(all_files, vietnam_names)
        download_files(wanted_files, date_obj, url)
    except FileNotFoundError:
        raise FileNotFoundError("No files found for this date")
    except Exception as e:
        print(f"❌ Error processing {date_obj.strftime('%Y-%m-%d')}: {e}", file=sys.stderr)

def check_for_updates(last_date):
    """Periodically check for new data since the last known date.

    Runs forever, waking every CHECK_INTERVAL seconds. Each wake-up sweeps
    from (last_checked + 1 day) up to (today - DAILY_UPDATE_LAG_DAYS), skipping
    any individual days that have no data rather than stopping the loop.
    """
    last_checked_date = last_date.replace(hour=0, minute=0, second=0, microsecond=0)

    while True:
        print(f"\n⏰ Scheduled check at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        today  = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff = today - timedelta(days=DAILY_UPDATE_LAG_DAYS)

        check_date = last_checked_date + timedelta(days=1)
        while check_date <= cutoff:
            try:
                download_for_date(check_date)
            except FileNotFoundError:
                print(f"⚠️  No data for {check_date.strftime('%Y-%m-%d')} — skipping.")
            # Always advance: don't retry missing days on every wake-up
            last_checked_date = check_date
            check_date += timedelta(days=1)

        print(f"✅ Up to date through {last_checked_date.strftime('%Y-%m-%d')}")
        print(f"🕐 Sleeping for {CHECK_INTERVAL / 3600:.0f} hours before next check...\n")
        time.sleep(CHECK_INTERVAL)

def main():
    start_date = datetime.strptime(START_DATE_STR, "%Y-%m-%d")
    today      = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Stop historical crawl DAILY_UPDATE_LAG_DAYS before today — data that recent
    # may not be published yet. Daily-update mode will pick it up later.
    historical_end = today - timedelta(days=DAILY_UPDATE_LAG_DAYS)

    print(f"🔥 Starting {PRODUCT} historical download "
          f"from {start_date.strftime('%Y-%m-%d')} "
          f"to   {historical_end.strftime('%Y-%m-%d')} "
          f"(holding back {DAILY_UPDATE_LAG_DAYS} days for processing lag)")

    last_attempted_date = start_date - timedelta(days=1)

    date = start_date
    while date <= historical_end:
        try:
            download_for_date(date)
        except FileNotFoundError:
            print(f"⚠️  No data for {date.strftime('%Y-%m-%d')} — skipping.")
        # Always advance regardless of whether data existed
        last_attempted_date = date
        date += timedelta(days=1)

    print(f"\n🔄 Switching to daily update mode "
          f"(last date checked: {last_attempted_date.strftime('%Y-%m-%d')})...")
    check_for_updates(last_attempted_date)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Download interrupted by user")
        sys.exit(-1)
