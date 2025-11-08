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
from ftplib import FTP_TLS
import ssl
from config.config import gis_config

FTP_TLS.ssl_version = ssl.PROTOCOL_TLSv1_2

SERVER = gis_config.SERVER
DOWNLOAD_DIR = r'/home/slow_data/Air_Quality/GIS'
USER = gis_config.GIS_USERNAME
PASSWORD = gis_config.GIS_PASSWORD

# DATE_STR = "2025-09-01"
START_DATE_STR = "2023-03-16"
print(f"📅 Using date: {START_DATE_STR}")

CHECK_INTERVAL = 6 * 60 * 60  # every 6 hours

# --- Helper Function for FTPS Connection ---
def connect_ftps():
    """Establishes and returns an Explicit FTPS connection."""
    if not all([SERVER, USER, PASSWORD]):
        print("Error: FTPS credentials/server not configured.")
        sys.exit(1)

    print(f"🔗 Connecting to {SERVER} via Explicit FTPS (Port 21)...")
    ftps = None
    try:
        ftps = FTP_TLS()
        # Connect to port 21 (like normal FTP for Explicit FTPS) 
        ftps.connect(SERVER, 21)
        ftps.login(USER, PASSWORD)
        # Secure the command channel (STARTTLS is implicit here)
        ftps.prot_p() 
        
        print("✅ FTPS connection successful.")
        return ftps
    except Exception as e:
        print(f"❌ FTPS Connection Failed: {e}")
        if ftps:
            ftps.quit()
        sys.exit(1)


def download_for_date(ftps, date_obj):
    year = date_obj .strftime('%Y')
    month = date_obj .strftime('%m')
    day = date_obj .strftime('%d')

    # Define the base remote path
    remote_path = f'/gpmdata/{year}/{month}/{day}/gis/'

    # Get the list of files (basenames)
    file_list = get_file_list(ftps, remote_path)
    
    if not file_list:
        print("ℹ️ No matching HHR .zip files found.")
        return False

    local_path = os.path.join(DOWNLOAD_DIR, year, month, day)
    os.makedirs(local_path, exist_ok=True)

    # Loop through and download each file
    for filename in file_list:
        get_file(ftps, filename, local_path)
    
    return True

def get_file_list(ftps: FTP_TLS, remote_path: str):
    """
    Get the file listing for the given remote path using the FTPS connection.
    Returns a list of filenames (basenames) or an empty list.
    """
    print(f"📡 Fetching file list from {remote_path}")
    
    # 1. Change to the remote directory
    try:
        ftps.cwd(remote_path)
    except Exception as e:
        print(f"⚠️ Could not change directory to {remote_path}. Error: {e}")
        return []

    # 2. Get a list of all files/directories (nlst returns basenames)
    # ftps.nlst() automatically returns a list in modern ftplib versions
    try:
        raw_listing = ftps.nlst()
    except Exception as e:
        print(f"⚠️ Failed to get file listing from {remote_path}. Error: {e}")
        return []

    # 3. Filter for .zip links containing 'HHR'
    pattern = re.compile(r'.*HHR.*\.zip', re.IGNORECASE)
    matches = [fname for fname in raw_listing if pattern.search(fname)]

    if not matches:
        print("ℹ️ No matching HHR .zip files found in listing.")
        return []

    print(f"✅ Found {len(matches)} HHR .zip files.")
    return matches

def get_file(ftps: FTP_TLS, filename: str, local_path: str):
    """
    Get the given file from the current FTPS directory.
    'filename' is the basename (e.g., 'file.zip').
    """
    output_path = os.path.join(local_path, filename)

    if os.path.exists(output_path):
        print(f"⏭️ File already exists, skipping: {filename}")
        return

    print(f"⬇️ Downloading: {filename} to {output_path}")

    # Use a binary file handle for writing the downloaded data
    with open(output_path, 'wb') as local_file:
        try:
            # ftps.retrbinary is used for downloading files in binary mode
            ftps.retrbinary(f'RETR {filename}', local_file.write)
            print(f"✅ Downloaded: {filename}")
        except Exception as e:
            print(f"❌ Failed to download {filename}. Error: {e}")
            # Clean up potentially incomplete file
            if os.path.exists(output_path):
                os.remove(output_path)

def download_historical_data():
    try:
        current_date = datetime.strptime(START_DATE_STR, '%Y-%m-%d')
    except ValueError:
        print("Error: Invalid date format. Please use YYYY-MM-DD.")
        sys.exit(1)
    ftps = connect_ftps()
    connected_since = None
    reconnect_interval_days = 30  # reconnect every 30 days or upon failure

    while True:
        if ftps is None or (connected_since and (current_date - connected_since).days >= reconnect_interval_days):
            if ftps:
                ftps.quit()
            ftps = connect_ftps()
            connected_since = current_date
            print(f"🔄 Reconnected FTPS at {current_date.date()}")

        found = download_for_date(ftps, current_date)

        if not found:
            print(f"🚫 No more GIS data found. Reached latest date.")
            break
        current_date += timedelta(days=1)
    if ftps:
        ftps.quit()
    return current_date  # The last checked date

def check_for_updates(start_from_date):
    """Periodically check for new data since last known date."""
    last_checked_date = start_from_date
    while True:
        print(f"\n⏰ Scheduled check at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        ftps = connect_ftps()
        next_date = last_checked_date
        new_data_found = False

        # Keep checking sequential dates until no data found
        while True:
            found = download_for_date(ftps, next_date)
            if not found:
                break
            new_data_found = True
            last_checked_date = next_date
            next_date += timedelta(days=1)

        ftps.quit()

        if new_data_found:
            print(f"✅ Updated through {last_checked_date.date()}")
        else:
            print("ℹ️ No new updates found.")

        print(f"🕒 Sleeping for {CHECK_INTERVAL / 3600:.1f} hours before next check...\n")
        time.sleep(CHECK_INTERVAL)

if __name__ == '__main__':
    print(f"📅 Starting historical download from {START_DATE_STR}")
    latest_date = download_historical_data()
    print(f"🔁 Switching to scheduled update mode after {latest_date.date()}")
    check_for_updates(latest_date)
