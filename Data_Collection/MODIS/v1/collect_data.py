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
from config.config import modis_config

try:
    from StringIO import StringIO   # python2
except ImportError:
    from io import StringIO         # python3

################################################################################
# NASA LAADS DAAC Download Functions (from official script)
################################################################################

USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')

def getcURL(url, headers=None, out=None):
    """Fallback to cURL when Python SSL doesn't support TLSv1.1+"""
    import subprocess
    try:
        print('  Using cURL fallback', file=sys.stderr)
        args = ['curl', '--fail', '-sS', '-L', '-b', 'session', '--get', url]
        for (k,v) in headers.items():
            args.extend(['-H', ': '.join([k, v])])
        if out is None:
            result = subprocess.check_output(args)
            return result.decode('utf-8') if isinstance(result, bytes) else result
        else:
            subprocess.call(args, stdout=out)
    except subprocess.CalledProcessError as e:
        print('cURL GET error: %s' % (e.message if hasattr(e, 'message') else str(e)), file=sys.stderr)
    return None

def geturl(url, token=None, out=None):
    """Read the specified URL and output to a file or return content"""
    headers = {'user-agent': USERAGENT}
    if token is not None:
        headers['Authorization'] = 'Bearer ' + token
    
    try:
        import ssl
        try:
            CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            CTX.minimum_version = ssl.TLSVersion.TLSv1_2
        except AttributeError:
            # Fallback for older Python versions
            CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        
        if sys.version_info.major == 2:
            import urllib2
            try:
                fh = urllib2.urlopen(urllib2.Request(url, headers=headers), context=CTX)
                if out is None:
                    return fh.read()
                else:
                    shutil.copyfileobj(fh, out)
            except urllib2.HTTPError as e:
                print('HTTP GET error code: %d' % e.code, file=sys.stderr)
                return getcURL(url, headers, out)
            except urllib2.URLError as e:
                print('Failed to make request: %s' % e.reason, file=sys.stderr)
                return getcURL(url, headers, out)
            return None
        else:
            from urllib.request import urlopen, Request, URLError, HTTPError
            try:
                fh = urlopen(Request(url, headers=headers), context=CTX)
                if out is None:
                    return fh.read().decode('utf-8')
                else:
                    shutil.copyfileobj(fh, out)
            except HTTPError as e:
                print('HTTP GET error code: %d' % e.code, file=sys.stderr)
                return getcURL(url, headers, out)
            except URLError as e:
                print('Failed to make request: %s' % e.reason, file=sys.stderr)
                return getcURL(url, headers, out)
            return None
    
    except AttributeError:
        return getcURL(url, headers, out)

################################################################################

SERVER = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD11A1'
DOWNLOAD_DIR = r'/home/slow_data/Air_Quality/MODIS'
TOKEN = modis_config.TOKEN

START_DATE_STR = "2025-11-07"
print(f"📅 Using date: {START_DATE_STR}")

TILES = ["h27v06", "h28v06", "h27v07", "h28v07", "h28v08"]

CHECK_INTERVAL = 24 * 60 * 60  # 24 hours in seconds
DOWNLOAD_SLEEP_SEC = 0.5

def make_url(date_obj):
    """Build the URL for a specific date folder."""
    year = date_obj.strftime("%Y")
    day_of_year = date_obj.strftime("%j")  # 001–366
    return f"{SERVER}/{year}/{day_of_year}"

def list_files(url, token):
    """
    Get the list of .hdf files at the given URL using NASA's method.
    Returns list of file dictionaries with 'name' and 'size'.
    """
    print(f"🔎 Listing files from {url}")
    
    try:
        import csv
        content = geturl(f'{url}.csv', token)
        if content is None:
            print(f"❌ Failed to retrieve file list from {url}")
            return []
        
        files = []
        for f in csv.DictReader(StringIO(content), skipinitialspace=True):
            files.append(f)
        return files
    
    except ImportError:
        # Fallback to JSON if csv module not available
        import json
        content = geturl(f'{url}.json', token)
        if content is None:
            print(f"❌ Failed to retrieve file list from {url}")
            return []
        
        data = json.loads(content)
        return data.get('content', [])

def filter_tiles(files):
    """Filter files by Vietnam-related tiles (only .hdf files)."""
    filtered = []
    for f in files:
        name = f['name']
        # Only include .hdf files that match our tiles
        if name.endswith('.hdf') and any(tile in name for tile in TILES):
            filtered.append(f)
    return filtered

def download_files(files, date_obj, base_url):
    """Download the given list of files, skipping files that already exist."""
    if not files:
        print(f"ℹ️ No matching tiles found for {date_obj.strftime('%Y-%m-%d')}")
        raise FileNotFoundError("No matching tiles found")
    
    year = date_obj.strftime("%Y")
    day = date_obj.strftime("%j")
    local_path = os.path.join(DOWNLOAD_DIR, year, day)
    os.makedirs(local_path, exist_ok=True)

    print(f"🔍 Checking {len(files)} files for {local_path}")
    files_to_download = []
    
    for f in files:
        filename = f['name']
        local_file = os.path.join(local_path, filename)
        
        if os.path.exists(local_file) and os.path.getsize(local_file) > 0:
            print(f"✓ Skipping existing file: {filename}")
        else:
            files_to_download.append(f)

    if not files_to_download:
        print(f"✅ All files already exist for {date_obj.strftime('%Y-%m-%d')}\n")
        return

    print(f"⬇️ Downloading {len(files_to_download)} new files to {local_path}")

    for f in files_to_download:
        filename = f['name']
        url = f"{base_url}/{filename}"
        path = os.path.join(local_path, filename)
        
        try:
            print(f'  Downloading: {filename}')
            with open(path, 'w+b') as fh:
                geturl(url, TOKEN, fh)
            print(f'  ✓ Downloaded: {filename}')
        except IOError as e:
            print(f"  ❌ Failed to download {filename}: {e.strerror}", file=sys.stderr)
        
        time.sleep(DOWNLOAD_SLEEP_SEC)

    print(f"✅ Finished downloads for {date_obj.strftime('%Y-%m-%d')}\n")

def download_for_date(date_obj):
    """Download all matching tiles for a specific date."""
    url = make_url(date_obj)
    
    try:
        all_files = list_files(url, TOKEN)
        wanted_files = filter_tiles(all_files)
        download_files(wanted_files, date_obj, url)
    except FileNotFoundError:
        raise FileNotFoundError("No matching tiles found for this date")
    
    except Exception as e:
        print(f"❌ Error processing {date_obj.strftime('%Y-%m-%d')}: {e}", file=sys.stderr)

def check_for_updates(last_date):
    """Periodically check for new data since last known date."""
    
    # Ensure last_date is only the date component for comparison
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
        print(f"🕒 Sleeping for {CHECK_INTERVAL / 3600} hours before next check...\n")
        time.sleep(CHECK_INTERVAL)

def main():
    start_date = datetime.strptime(START_DATE_STR, "%Y-%m-%d")
    current_date_eod = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # First, download historical data and track the last date successfully checked
    date = start_date
    last_downloaded_date = start_date - timedelta(days=1)  # Initialize to day before start
    
    print(f"📥 Starting historical download from {start_date.strftime('%Y-%m-%d')} to {current_date_eod.strftime('%Y-%m-%d')}")
    
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