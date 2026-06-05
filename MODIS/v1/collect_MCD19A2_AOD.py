import os
import sys
# Get the absolute path of the current script's directory
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory's path
parent_dir = os.path.join(current_script_dir, os.pardir)
# Add the parent directory to sys.path
sys.path.append(parent_dir)

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
import yaml

yaml_path = os.path.join(parent_dir, 'config', 'config.yaml')
with open(yaml_path, 'r') as file:
    config_data = yaml.safe_load(file)

# Access your modis configuration from the YAML structure
# (Adjust the key 'modis_config' based on how your YAML file is structured)
modis_config = config_data.get('MODIS')

# print(modis_config)

try:
    from StringIO import StringIO   # python2
except ImportError:
    from io import StringIO         # python3

# Suppress the insecure request warning if SSL verification fails and we fallback
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

################################################################################
# CONFIGURATION
################################################################################

SERVER = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MCD19A2'
DOWNLOAD_DIR = r'/home/slow_data/Air_Quality/MODIS_MCD19A2/raw'
TOKEN = modis_config.get('TOKEN')

START_DATE_STR = "2026-03-01"
print(f"📅 Using date: {START_DATE_STR}")

TILES = ["h27v06", "h28v06", "h27v07", "h28v07", "h28v08"]

CHECK_INTERVAL = 24 * 60 * 60  # 24 hours in seconds
MAX_CONCURRENT_DOWNLOADS = 8  

################################################################################
# ROBUST HTTP SESSION SETUP
################################################################################

USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')

# Create a global session to utilize Connection Pooling (Keep-Alive)
# This is drastically faster than opening a new connection for every file.
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

# To bypass your local SSL certificate issues entirely, we set verify=False. 
# Since we are just downloading public NASA data, this is perfectly safe.
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

def filter_tiles(files):
    """Filter files by Vietnam-related tiles (only .hdf files)."""
    filtered = []
    for f in files:
        name = f.get('name', '')
        # Only include .hdf files that match our tiles
        if name.endswith('.hdf') and any(tile in name for tile in TILES):
            filtered.append(f)
    return filtered

def download_single_file(f, local_path, base_url):
    """Downloads a single file using a stream to save RAM."""
    filename = f['name']
    url = f"{base_url}/{filename}"
    path = os.path.join(local_path, filename)
    
    try:
        # print(f'  ⬇️ Starting: {filename}')
        with http_session.get(url, verify=SSL_VERIFY, stream=True, timeout=60) as response:
            response.raise_for_status()
            with open(path, 'wb') as fh:
                for chunk in response.iter_content(chunk_size=1024 * 1024): # 1MB chunks
                    if chunk:
                        fh.write(chunk)
        print(f'  ✅ Downloaded: {filename}')
        return True
    except Exception as e:
        print(f"  ❌ Failed to download {filename}: {str(e)}", file=sys.stderr)
        # Cleanup partial/corrupt files
        if os.path.exists(path):
            os.remove(path)
        return False

def download_files(files, date_obj, base_url):
    """Download the given list of files concurrently, skipping files that already exist."""
    if not files:
        print(f"ℹ️ No matching tiles found for {date_obj.strftime('%Y-%m-%d')}")
        raise FileNotFoundError("No matching tiles found")
    
    year = date_obj.strftime("%Y")
    day = date_obj.strftime("%j")
    local_path = os.path.join(DOWNLOAD_DIR, year, day)
    os.makedirs(local_path, exist_ok=True)

    files_to_download = []
    
    for f in files:
        filename = f['name']
        local_file = os.path.join(local_path, filename)
        
        # Check if file exists AND isn't empty
        if os.path.exists(local_file) and os.path.getsize(local_file) > 0:
            pass # Skip existing
        else:
            files_to_download.append(f)

    if not files_to_download:
        print(f"✅ All {len(files)} files already exist for {date_obj.strftime('%Y-%m-%d')}")
        return

    print(f"⚡ Downloading {len(files_to_download)} new files concurrently using {MAX_CONCURRENT_DOWNLOADS} threads...")

    # Execute downloads in parallel using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as executor:
        futures = [
            executor.submit(download_single_file, f, local_path, base_url) 
            for f in files_to_download
        ]
        concurrent.futures.wait(futures)

    print(f"✅ Finished downloads for {date_obj.strftime('%Y-%m-%d')}")

def download_for_date(date_obj):
    """Download all matching tiles for a specific date."""
    url = make_url(date_obj)
    
    try:
        all_files = list_files(url)
        wanted_files = filter_tiles(all_files)
        download_files(wanted_files, date_obj, url)
    except FileNotFoundError:
        raise FileNotFoundError("No matching tiles found for this date")
    except Exception as e:
        print(f"❌ Error processing {date_obj.strftime('%Y-%m-%d')}: {e}", file=sys.stderr)

def check_for_updates(last_date):
    """Periodically check for new data since last known date."""
    last_checked_date = last_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    while True:
        print(f"\n⏰ Scheduled check at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        current_date_eod = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        check_date = last_checked_date + timedelta(days=1)
        
        dates_downloaded = 0
        while check_date <= current_date_eod:
            try:
                download_for_date(check_date)
            except FileNotFoundError:
                print(f"🚫 No data found for {check_date.strftime('%Y-%m-%d')}. Stopping checks for now.")
                break
            last_checked_date = check_date
            check_date += timedelta(days=1)
            dates_downloaded += 1
            
        print(f"✅ Up to date through {last_checked_date.strftime('%Y-%m-%d')}")
        print(f"🕐 Sleeping for {CHECK_INTERVAL / 3600} hours before next check...\n")
        time.sleep(CHECK_INTERVAL)

def main():
    start_date = datetime.strptime(START_DATE_STR, "%Y-%m-%d")
    current_date_eod = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # First, download historical data and track the last date successfully checked
    date = start_date
    last_downloaded_date = start_date - timedelta(days=1)  # Initialize to day before start
    
    print(f"🔥 Starting MCD19A2 (MAIAC) historical download from {start_date.strftime('%Y-%m-%d')} to {current_date_eod.strftime('%Y-%m-%d')}")
    
    while date <= current_date_eod:
        try:
            download_for_date(date)
        except FileNotFoundError:
            break
        last_downloaded_date = date  # Update the tracker
        date += timedelta(days=1)
    
    # Then switch to update mode, starting from the last date processed
    print(f"\n🔄 Switching to daily update mode...")
    check_for_updates(last_downloaded_date)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Download interrupted by user")
        sys.exit(-1)