import os
import sys
current_script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(current_script_dir, os.pardir)
sys.path.append(parent_dir)

import csv
import concurrent.futures
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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

################################################################################
# CONFIGURATION
################################################################################

SERVER = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MCD12Q1'
DOWNLOAD_DIR = r'/home/slow_data/Air_Quality/MODIS_MCD12Q1'
TOKEN = modis_config.get('TOKEN')

# MCD12Q1 is a yearly product; data for a given year is typically released
# the following year. Adjust START_YEAR as needed.
START_YEAR = 2022
# Day-of-year is always 001 for this annual product.
DAY_OF_YEAR = '001'

TILES = ["h27v06", "h28v06", "h27v07", "h28v07", "h28v08"]

MAX_CONCURRENT_DOWNLOADS = 8

################################################################################
# HTTP SESSION
################################################################################

USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n', '').replace('\r', '')

http_session = requests.Session()
http_session.headers.update({
    'user-agent': USERAGENT,
    'Authorization': f'Bearer {TOKEN}'
})

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

def make_url(year):
    """Build the URL for a specific year folder (day is always 001)."""
    return f"{SERVER}/{year}/{DAY_OF_YEAR}"

def list_files(url):
    """Get the list of .hdf files at the given URL."""
    print(f"Listing files from {url}")
    try:
        response = http_session.get(f'{url}.csv', verify=SSL_VERIFY, timeout=30)
        response.raise_for_status()
        return list(csv.DictReader(StringIO(response.text), skipinitialspace=True))
    except requests.exceptions.HTTPError:
        try:
            response = http_session.get(f'{url}.json', verify=SSL_VERIFY, timeout=30)
            response.raise_for_status()
            return response.json().get('content', [])
        except Exception as e:
            print(f"Failed to retrieve file list from {url}: {e}")
            return []
    except Exception as e:
        print(f"Failed to retrieve file list from {url}: {e}")
        return []

def filter_tiles(files):
    """Keep only .hdf files for the Vietnam tiles."""
    return [
        f for f in files
        if f.get('name', '').endswith('.hdf') and any(tile in f['name'] for tile in TILES)
    ]

def download_single_file(f, local_path, base_url):
    """Stream-download a single file."""
    filename = f['name']
    url = f"{base_url}/{filename}"
    path = os.path.join(local_path, filename)
    try:
        with http_session.get(url, verify=SSL_VERIFY, stream=True, timeout=120) as response:
            response.raise_for_status()
            with open(path, 'wb') as fh:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)
        print(f"  Downloaded: {filename}")
        return True
    except Exception as e:
        print(f"  Failed to download {filename}: {e}", file=sys.stderr)
        if os.path.exists(path):
            os.remove(path)
        return False

def download_files_for_year(year, files, base_url):
    """Download matched tiles for a year, skipping files that already exist."""
    if not files:
        print(f"No matching tiles found for {year}")
        return

    local_path = os.path.join(DOWNLOAD_DIR, str(year), DAY_OF_YEAR)
    os.makedirs(local_path, exist_ok=True)

    to_download = [
        f for f in files
        if not (
            os.path.exists(os.path.join(local_path, f['name'])) and
            os.path.getsize(os.path.join(local_path, f['name'])) > 0
        )
    ]

    if not to_download:
        print(f"All {len(files)} files already exist for {year}")
        return

    print(f"Downloading {len(to_download)} file(s) for {year} using {MAX_CONCURRENT_DOWNLOADS} threads...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as executor:
        futures = [
            executor.submit(download_single_file, f, local_path, base_url)
            for f in to_download
        ]
        concurrent.futures.wait(futures)
    print(f"Finished downloads for {year}")

def download_for_year(year):
    """Download all matching tiles for a specific year."""
    url = make_url(year)
    all_files = list_files(url)
    wanted_files = filter_tiles(all_files)
    if not wanted_files:
        raise FileNotFoundError(f"No matching tiles found for {year}")
    download_files_for_year(year, wanted_files, url)

def main():
    from datetime import datetime
    # MCD12Q1 for year Y is typically published mid-year Y+1, so cap at last year.
    current_year = datetime.now().year
    end_year = current_year - 1

    print(f"Starting MCD12Q1 (Land Cover) download from {START_YEAR} to {end_year}")

    for year in range(START_YEAR, end_year + 1):
        try:
            download_for_year(year)
        except FileNotFoundError as e:
            print(f"Skipping {year}: {e}")
        except Exception as e:
            print(f"Error processing {year}: {e}", file=sys.stderr)

    print("\nAll available years processed.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nDownload interrupted by user")
        sys.exit(-1)
