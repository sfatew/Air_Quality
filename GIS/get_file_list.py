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
from ftplib import FTP_TLS, error_temp, error_perm
import ssl
from config.config import gis_config

FTP_TLS.ssl_version = ssl.PROTOCOL_TLSv1_2

SERVER = gis_config.SERVER
NEAR_REALTIME_SERVER = gis_config.NEAR_REALTIME_SERVER
DOWNLOAD_DIR = r'/home/slow_data/Air_Quality/GIS'
USER = gis_config.GIS_USERNAME
PASSWORD = gis_config.GIS_PASSWORD

NRT_PATTERN = re.compile(r'.*HHR.*\.30min\.zip', re.IGNORECASE)
NRT_DATE_PATTERN = re.compile(r'3IMERG\.(\d{8})-')

# CHANGED: Updated the start date to pick up where your folders left off
START_DATE_STR = "2025-10-01"
# ADDED: Created a hard end date so the script doesn't download 2026 data
END_DATE_STR = "2026-04-09" 
print(f"📅 Using date range: {START_DATE_STR} to {END_DATE_STR}")

CHECK_INTERVAL = 6 * 60 * 60  # every 6 hours
RECONNECT_AFTER_FILES = 50  # Reconnect after every N files
MAX_RETRIES = 3  # Maximum retry attempts for operations

# --- Helper Function for FTPS Connection ---
def connect_ftps():
    """Establishes and returns an Explicit FTPS connection."""
    if not all([SERVER, USER, PASSWORD]):
        print("Error: FTPS credentials/server not configured.")
        sys.exit(1)

    print(f"🔗 Connecting to {SERVER} via Explicit FTPS (Port 21)...")
    ftps = None
    try:
        ftps = FTP_TLS(timeout=60)  # Add timeout
        ftps.connect(SERVER, 21)
        ftps.login(USER, PASSWORD)
        ftps.prot_p()
        
        # Send NOOP to verify connection
        ftps.voidcmd("NOOP")
        
        print("✅ FTPS connection successful.")
        return ftps
    except Exception as e:
        print(f"❌ FTPS Connection Failed: {e}")
        if ftps:
            try:
                ftps.quit()
            except:
                pass
        return None


def connect_ftps_nrt():
    """Establishes and returns an Explicit FTPS connection to the Near-Realtime server."""
    if not all([USER, PASSWORD]):
        print("Error: FTPS credentials not configured.")
        return None

    print(f"🔗 Connecting to NRT server {NEAR_REALTIME_SERVER} via Explicit FTPS (Port 21)...")
    ftps = None
    try:
        ftps = FTP_TLS(timeout=60)
        ftps.connect(NEAR_REALTIME_SERVER, 21)
        ftps.login(USER, PASSWORD)
        ftps.prot_p()
        ftps.voidcmd("NOOP")
        print("✅ NRT FTPS connection successful.")
        return ftps
    except Exception as e:
        print(f"❌ NRT FTPS Connection Failed: {e}")
        if ftps:
            try:
                ftps.quit()
            except:
                pass
        return None


def is_connection_alive(ftps):
    """Check if FTPS connection is still alive."""
    try:
        ftps.voidcmd("NOOP")
        return True
    except:
        return False


def get_nrt_file_list_for_date(ftps_nrt, year, month, date_str):
    """Get NRT file listing for a specific date from the monthly directory."""
    remote_path = f'/data/imerg/gis/{year}/{month}/'
    print(f"📡 Fetching NRT file list from {remote_path} for date {date_str}")

    for attempt in range(MAX_RETRIES):
        try:
            ftps_nrt.cwd('/')
            ftps_nrt.cwd(remote_path)
            raw_listing = ftps_nrt.nlst()

            matches = [f for f in raw_listing if NRT_PATTERN.search(f) and date_str in f]

            if not matches:
                print(f"ℹ️ No NRT HHR .30min.zip files found for {date_str}.")
                return []

            print(f"✅ Found {len(matches)} NRT files for {date_str}.")
            return matches

        except error_perm as e:
            if "550" in str(e):
                print(f"ℹ️ NRT directory doesn't exist: {remote_path}")
                return []
            print(f"⚠️ NRT permission error on attempt {attempt + 1}: {e}")
            if attempt == MAX_RETRIES - 1:
                return []

        except Exception as e:
            print(f"⚠️ Error fetching NRT file list (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt == MAX_RETRIES - 1:
                return []
            time.sleep(2 ** attempt)

    return []


def download_for_date(ftps, date_obj, ftps_nrt=None):
    """Download files for a specific date. Returns (final_found, files_downloaded).

    Logic:
    1. If local NRT files (3B-HHR-L prefix) exist, attempt to upgrade to Final first.
    2. Try Final server (/gpmdata/...) regardless of local state.
       - If Final files found: download and return (True, count).
    3. If Final not available:
       - If had local NRT files: check NRT server for any missing files and fill gaps.
       - If no local files: download all NRT files for this date as fallback.
       Returns (False, count) — signals that Final data is still not available.
    """
    year = date_obj.strftime('%Y')
    month = date_obj.strftime('%m')
    day = date_obj.strftime('%d')
    date_str = date_obj.strftime('%Y%m%d')

    local_path = os.path.join(DOWNLOAD_DIR, year, month, day)

    # Check if we already have NRT (near-realtime) files locally
    has_nrt_files = False
    if os.path.exists(local_path):
        local_files = os.listdir(local_path)
        has_nrt_files = any(f.startswith('3B-HHR-L') for f in local_files)
        if has_nrt_files:
            print(f"🔄 NRT files detected for {date_obj.date()}, attempting upgrade to Final...")

    # --- Try Final server ---
    remote_path = f'/gpmdata/{year}/{month}/{day}/gis/'
    file_list = get_file_list(ftps, remote_path)

    if file_list:
        os.makedirs(local_path, exist_ok=True)
        files_downloaded = 0
        for filename in file_list:
            success = get_file(ftps, remote_path, filename, local_path)
            if success:
                files_downloaded += 1
        if has_nrt_files:
            print(f"✅ Upgraded to Final data for {date_obj.date()}")
        return True, files_downloaded

    # --- Final data not available ---
    if has_nrt_files:
        print(f"⚠️ Final data not yet available for {date_obj.date()}, checking NRT completeness...")
    else:
        print(f"⚠️ No Final data for {date_obj.date()}, falling back to NRT server...")

    if ftps_nrt is None:
        print("ℹ️ NRT connection not available, skipping NRT fallback.")
        return False, 0

    nrt_files = get_nrt_file_list_for_date(ftps_nrt, year, month, date_str)
    if not nrt_files:
        print(f"ℹ️ No NRT data found for {date_obj.date()} either.")
        return False, 0

    os.makedirs(local_path, exist_ok=True)
    nrt_remote_path = f'/data/imerg/gis/{year}/{month}/'
    files_downloaded = 0
    for filename in nrt_files:
        success = get_file(ftps_nrt, nrt_remote_path, filename, local_path)
        if success:
            files_downloaded += 1

    return False, files_downloaded


def get_file_list(ftps: FTP_TLS, remote_path: str):
    """Get the file listing for the given remote path using the FTPS connection."""
    print(f"📡 Fetching file list from {remote_path}")
    
    for attempt in range(MAX_RETRIES):
        try:
            # Always use absolute path and change directory fresh
            ftps.cwd('/')  # Reset to root first
            ftps.cwd(remote_path)
            
            raw_listing = ftps.nlst()
            
            # Filter for .zip links containing 'HHR'
            pattern = re.compile(r'.*HHR.*\.zip', re.IGNORECASE)
            matches = [fname for fname in raw_listing if pattern.search(fname)]

            if not matches:
                print("ℹ️ No matching HHR .zip files found in listing.")
                return []

            print(f"✅ Found {len(matches)} HHR .zip files.")
            return matches
            
        except error_perm as e:
            # Directory doesn't exist - not an error, just no data
            if "550" in str(e):
                print(f"ℹ️ Directory doesn't exist: {remote_path}")
                return []
            print(f"⚠️ Permission error on attempt {attempt + 1}: {e}")
            if attempt == MAX_RETRIES - 1:
                return []
                
        except Exception as e:
            print(f"⚠️ Error fetching file list (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt == MAX_RETRIES - 1:
                raise  # Re-raise on final attempt
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return []


def get_file(ftps: FTP_TLS, remote_path: str, filename: str, local_path: str):
    """Download a file from the FTPS server with retry logic."""
    output_path = os.path.join(local_path, filename)

    if os.path.exists(output_path):
        print(f"⏭️ File already exists, skipping: {filename}")
        return True

    print(f"⬇️ Downloading: {filename} to {output_path}")

    for attempt in range(MAX_RETRIES):
        try:
            # Ensure we're in the right directory
            ftps.cwd('/')
            ftps.cwd(remote_path)
            
            with open(output_path, 'wb') as local_file:
                ftps.retrbinary(f'RETR {filename}', local_file.write)
                
            print(f"✅ Downloaded: {filename}")
            return True
            
        except Exception as e:
            print(f"❌ Download failed (attempt {attempt + 1}/{MAX_RETRIES}): {filename}. Error: {e}")
            
            # Clean up potentially incomplete file
            if os.path.exists(output_path):
                os.remove(output_path)
            
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"⛔ Failed to download {filename} after {MAX_RETRIES} attempts")
                raise  # Re-raise on final attempt
    
    return False


def download_historical_data():
    """Download historical data with automatic reconnection."""
    try:
        current_date = datetime.strptime(START_DATE_STR, '%Y-%m-%d')
        end_date = datetime.strptime(END_DATE_STR, '%Y-%m-%d')
    except ValueError:
        print("Error: Invalid date format. Please use YYYY-MM-DD.")
        sys.exit(1)

    ftps = connect_ftps()
    if not ftps:
        print("❌ Failed to establish initial connection.")
        sys.exit(1)

    # Connect to NRT server as fallback (non-fatal if unavailable)
    ftps_nrt = connect_ftps_nrt()
    if not ftps_nrt:
        print("⚠️ NRT server connection failed. Will proceed without NRT fallback.")

    last_connection_time = time.time()
    files_since_reconnect = 0
    reconnect_interval_seconds = 30 * 60  # 30 minutes

    while True:
        if current_date > end_date:
            print(f"\n🎯 Reached target end date: {END_DATE_STR}. Stopping download.")
            break

        # Reconnect Final server if needed
        current_time = time.time()
        time_since_connection = current_time - last_connection_time

        if (files_since_reconnect >= RECONNECT_AFTER_FILES or
                time_since_connection >= reconnect_interval_seconds or
                not is_connection_alive(ftps)):

            print(f"🔄 Reconnecting Final server (files: {files_since_reconnect}, time: {time_since_connection/60:.1f}min)")
            try:
                ftps.quit()
            except:
                pass

            ftps = connect_ftps()
            if not ftps:
                print("❌ Reconnection failed. Retrying in 60 seconds...")
                time.sleep(60)
                continue

            last_connection_time = time.time()
            files_since_reconnect = 0

        # Reconnect NRT server if needed
        if ftps_nrt is not None and not is_connection_alive(ftps_nrt):
            print("🔄 Reconnecting NRT server...")
            try:
                ftps_nrt.quit()
            except:
                pass
            ftps_nrt = connect_ftps_nrt()
            if not ftps_nrt:
                print("⚠️ NRT server reconnection failed. Continuing without NRT fallback for now.")

        # Try to download for current date
        try:
            final_found, num_files = download_for_date(ftps, current_date, ftps_nrt)

            if not final_found:
                print(f"🚫 No Final GIS data available for: {current_date.date()}")

            files_since_reconnect += num_files
            current_date += timedelta(days=1)

        except Exception as e:
            print(f"❌ Error processing {current_date.date()}: {e}")
            print("🔄 Will reconnect and retry...")

            try:
                ftps.quit()
            except:
                pass

            ftps = connect_ftps()
            if not ftps:
                print("❌ Reconnection failed. Retrying in 60 seconds...")
                time.sleep(60)
            else:
                last_connection_time = time.time()
                files_since_reconnect = 0

            continue  # Retry same date

    for conn in (ftps, ftps_nrt):
        if conn:
            try:
                conn.quit()
            except:
                pass

    return current_date


def check_for_updates(start_from_date):
    """Periodically check for new data since last known date."""
    last_checked_date = start_from_date

    while True:
        print(f"\n⏰ Scheduled check at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

        ftps = connect_ftps()
        if not ftps:
            print("❌ Failed to connect. Retrying in 10 minutes...")
            time.sleep(600)
            continue

        ftps_nrt = connect_ftps_nrt()
        if not ftps_nrt:
            print("⚠️ NRT server connection failed. Proceeding without NRT fallback.")

        next_date = last_checked_date
        new_data_found = False

        try:
            while True:
                final_found, _ = download_for_date(ftps, next_date, ftps_nrt)
                if not final_found:
                    break
                new_data_found = True
                last_checked_date = next_date
                next_date += timedelta(days=1)

        except Exception as e:
            print(f"⚠️ Error during update check: {e}")

        finally:
            for conn in (ftps, ftps_nrt):
                if conn:
                    try:
                        conn.quit()
                    except:
                        pass

        if new_data_found:
            print(f"✅ Updated through {last_checked_date.date()}")
        else:
            print("ℹ️ No new updates found.")

        print(f"🕒 Sleeping for {CHECK_INTERVAL / 3600:.1f} hours before next check...\n")
        time.sleep(CHECK_INTERVAL)


if __name__ == '__main__':
    # CHANGED: Print statement updated to show the end date
    print(f"📅 Starting historical download from {START_DATE_STR} to {END_DATE_STR}")
    latest_date = download_historical_data()
    
    # ADDED: Simple completion message
    print("✅ Finished downloading requested date range. Shutting down script.")
    
    # COMMENTED OUT: We no longer want the script to switch to the infinite update loop
    # print(f"🔁 Switching to scheduled update mode after {latest_date.date()}")
    # check_for_updates(latest_date)