import os
import sys
# Get the absolute path of the current script's directory
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory's path
parent_dir = os.path.join(current_script_dir, os.pardir)
# Add the parent directory to sys.path
sys.path.append(parent_dir)
import subprocess
import re
from datetime import datetime, timedelta
import time
from config.config import modis_config


SERVER = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD11A1'
DOWNLOAD_DIR = r'E:\Air Quality\MODIS'
TOKEN = modis_config.TOKEN

START_DATE_STR = "2023-03-16"
print(f"📅 Using date: {START_DATE_STR}")

TILES = ["h27v06", "h28v06", "h27v07", "h28v07", "h28v08"]

CHECK_INTERVAL = 24 * 60 * 60  # 24 hours in seconds
DOWNLOAD_SLEEP_SEC = 0.5 # New constant for avoiding server overload

def make_url(date_obj):
    """Build the URL for a specific date folder."""
    year = date_obj.strftime("%Y")
    day_of_year = date_obj.strftime("%j")  # 001–366
    return f"{SERVER}/{year}/{day_of_year}/"

def list_files(url):
    """
    Get the list of .hdf files at the given URL using wget --spider (no download).
    Returns list of file URLs.
    """
    print(f"🔎 Listing files from {url}")
    args = [
        "wget",
        "--spider",
        "--no-check-certificate",
        "--header", f"Authorization: Bearer {TOKEN}",
        "-r", "-l1", "-nd",
        "-A", ".hdf",
        url
    ]

    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    
    # ---  Check for wget exit code and authentication errors ---
    if process.returncode != 0:
        error_output = (stdout + stderr).decode().strip()
        # Common pattern for auth failure
        if "401 Unauthorized" in error_output or "403 Forbidden" in error_output:
            print("❌ Authentication failed. Check your MODIS token.")
        else:
            print(f"❌ Wget failed to list files for {url}. Error: {error_output}")
        return []

    # The file URLs appear in stderr because wget prints them as "Spider mode enabled..."
    text = (stdout + stderr).decode()

    # Extract URLs of .hdf files
    file_urls = re.findall(r"https://[^\s]+\.hdf", text)
    return file_urls

def filter_tiles(file_urls):
    """Filter files by Vietnam-related tiles."""
    filtered = [url for url in file_urls if any(tile in url for tile in TILES)]
    return filtered

def download_files(file_urls, date_obj):
    """Download the given list of file URLs."""
    if not file_urls:
        print(f"ℹ️ No matching tiles found for {date_obj.strftime('%Y-%m-%d')}")
        return
    
    year = date_obj.strftime("%Y")
    day = date_obj.strftime("%j")
    local_path = os.path.join(DOWNLOAD_DIR, year, day)
    os.makedirs(local_path, exist_ok=True)

    print(f"⬇️ Downloading {len(file_urls)} files to {local_path}")

    for url in file_urls:
        args = [
            "wget",
            "--header", f"Authorization: Bearer {TOKEN}",
            "-P", local_path,
            url
        ]
        
        # --- Check for wget download status ---
        result = subprocess.run(args, capture_output=True)
        if result.returncode != 0:
             print(f"🛑 Failed to download {url}. Wget error output:\n{result.stderr.decode().strip()}")
        # -----------------------------------------------
        
        time.sleep(DOWNLOAD_SLEEP_SEC)

    print(f"✅ Finished downloads for {date_obj.strftime('%Y-%m-%d')}\n")

def download_for_date(date_obj):
    url = make_url(date_obj)
    all_files = list_files(url)
    wanted_files = filter_tiles(all_files)
    download_files(wanted_files, date_obj)

# --- check_for_updates now returns the last date successfully checked ---
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
            download_for_date(check_date)
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
    last_downloaded_date = start_date - timedelta(days=1) # Initialize to day before start
    
    while date <= current_date_eod:
        download_for_date(date)
        last_downloaded_date = date # Update the tracker
        date += timedelta(days=1)
    
    # Then switch to update mode, starting from the last date processed
    print(f"🔄 Switching to daily update mode...")
    check_for_updates(last_downloaded_date)
    
if __name__ == "__main__":
    main()