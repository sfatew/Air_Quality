import os
import sys

# --- Configuration for import paths ---
# Get the absolute path of the current script's directory
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory's path
parent_dir = os.path.join(current_script_dir, os.pardir)
# Add the parent directory to sys.path
sys.path.append(parent_dir)

import time
from ftplib import FTP
import subprocess
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from config.config import aod_config


# --- FTP and Directory Configuration ---
FTP_HOST = "ftp.ptree.jaxa.jp"
FTP_USER = aod_config.FTP_USER
FTP_PASS = aod_config.FTP_PASS
BASE_DIR = "/pub/himawari/L3/ARP/031"

# Cấu hình thư mục local
LOCAL_BASE = "/home/slow_data/Air_Quality/AOD/L3_AOD"
PROCESS_SCRIPT = "/home/work1/projects/Air_Quality/AOD data/process_aod_data.py"
MISSING_LOG_FILE = "/home/work1/projects/Air_Quality/AOD data/missing_data.log"

# Thời gian bắt đầu lịch sử để tải về (resume from where Dec 2025 left off)
start_time_holder = datetime(2022, 9, 26, 0, 0)

# Stop after this point — do not download 2026 data yet
END_TIME = datetime(2026, 3, 23, 23, 0)

# Maximum number of consecutive missing hours before considering we've caught up
MAX_CONSECUTIVE_MISSING = 24  # If 24 in a row are missing, assume we're caught up


# --- Missing Data Log Management ---
def load_missing_data_log():
    """Load missing data entries from log file into a dictionary"""
    missing_data = {}
    if not os.path.exists(MISSING_LOG_FILE):
        return missing_data
    
    try:
        with open(MISSING_LOG_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    # Parse log format: timestamp | data_timestamp | remote_path | reason
                    parts = line.split(" | ")
                    if len(parts) >= 3:
                        data_timestamp_str = parts[1]
                        remote_path = parts[2]
                        # Parse the data timestamp
                        data_timestamp = datetime.strptime(data_timestamp_str, '%Y-%m-%d %H:%M')
                        # Store with timestamp as key for quick lookup
                        missing_data[data_timestamp] = line
                except Exception as e:
                    print(f"⚠️ Could not parse log line: {line[:50]}... Error: {e}")
                    continue
        
        if missing_data:
            print(f"📋 Loaded {len(missing_data)} missing data entries from log")
    except Exception as e:
        print(f"⚠️ Error loading missing data log: {e}")
    
    return missing_data


def remove_from_missing_log(timestamp):
    """Remove a timestamp entry from the missing data log file"""
    if not os.path.exists(MISSING_LOG_FILE):
        return
    
    try:
        # Read all lines
        with open(MISSING_LOG_FILE, "r") as f:
            lines = f.readlines()
        
        # Filter out the line with this timestamp
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M')
        filtered_lines = [line for line in lines if f" | {timestamp_str} | " not in line]
        
        # Write back
        with open(MISSING_LOG_FILE, "w") as f:
            f.writelines(filtered_lines)
        
        if len(filtered_lines) < len(lines):
            print(f"✅ Removed {timestamp_str} from missing data log")
    except Exception as e:
        print(f"⚠️ Error removing from missing data log: {e}")


def log_missing_data(timestamp, remote_path, reason="Directory not found"):
    """Log missing data to file"""
    try:
        with open(MISSING_LOG_FILE, "a") as f:
            log_entry = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {timestamp.strftime('%Y-%m-%d %H:%M')} | {remote_path} | {reason}\n"
            f.write(log_entry)
        print(f"📝 Logged missing data: {timestamp.strftime('%Y-%m-%d %H:%M')}")
    except Exception as e:
        print(f"⚠️ Could not write to missing data log: {e}")


def get_local_files(local_path):
    """Get set of all .nc files in local directory"""
    if not os.path.exists(local_path):
        return set()
    try:
        return set([f.removeprefix("aod_vietnam_").removesuffix(".tif") for f in os.listdir(local_path)])
    except Exception as e:
        print(f"⚠️ Error reading local directory {local_path}: {e}")
        return set()


def fetch_file(file, local_path, ftp):
    local_file = os.path.join(local_path, file)
    try:
        with open(local_file, "wb") as f:
            # Retrieve the file with retry logic
            for attempt in range(3):
                try:
                    ftp.retrbinary(f"RETR {file}", f.write)
                    break
                except Exception as e:
                    if attempt < 2:
                        print(f"⚠️ Retry {attempt+1} for {file}")
                        time.sleep(5)
                    else:
                        raise
        print(f"✅ Downloaded: {file}")
        
        # Process the .nc file
        subprocess.run(["python", PROCESS_SCRIPT, local_file], check=True, timeout=300)
        
    except subprocess.TimeoutExpired:
        print(f"❌ Lỗi: Xử lý file {file} đã hết thời gian (5 phút).")
    except Exception as file_error:
        print(f"❌ Lỗi khi tải hoặc xử lý file {file}: {file_error}")
        # Clean up partially downloaded file
        if os.path.exists(local_file):
            os.remove(local_file)


# --- Core FTP and Processing Logic ---
def download_and_process(ftp, remote_path, local_path, timestamp, missing_data_log):
    """
    Attempts to connect to a remote directory, downloads new files, and processes them.
    
    Returns:
        bool: True if the remote directory was successfully accessed (even if empty/no new files).
              False if accessing the remote path failed (e.g., directory does not exist).
    """
    os.makedirs(local_path, exist_ok=True)
    
    try:
        # Change to the remote directory
        ftp.cwd(remote_path)
        
        # Get list of files in the current remote directory
        remote_files = ftp.nlst()
        remote_nc_files = [f for f in remote_files if f.endswith('.nc')]
        
        # Get list of files already downloaded locally
        local_files = get_local_files(local_path)
        
        # Filter files that need to be downloaded
        files_to_download = [f for f in remote_nc_files if f.removesuffix(".nc") not in local_files]

        if not files_to_download:
            if remote_nc_files:
                print(f"✔️ Đã kiểm tra {remote_path}. Tất cả {len(remote_nc_files)} file .nc đã tồn tại.")
            else:
                print(f"✔️ Đã kiểm tra {remote_path}. Không có file .nc.")
            
            # Check if this timestamp was in missing log and remove it if data exists
            if timestamp in missing_data_log and remote_nc_files:
                print(f"🔄 Data now available for previously missing timestamp")
                remove_from_missing_log(timestamp)
            
            return True

        print(f"📥 Bắt đầu tải {len(files_to_download)} file mới từ {remote_path}...")
        
        for file in files_to_download:
            fetch_file(file, local_path, ftp)

        # After successful download, check if this was in missing log
        if timestamp in missing_data_log:
            print(f"🎉 Successfully recovered previously missing data!")
            remove_from_missing_log(timestamp)

        return True  # Successfully accessed the directory

    except Exception as e:
        error_msg = str(e)
        print(f"⛔ Không truy cập được thư mục {remote_path}: {error_msg}")
        
        # Only log if not already in missing data log
        if timestamp not in missing_data_log:
            log_missing_data(timestamp, remote_path, error_msg)
        
        return False


def historical_mode(missing_data_log):
    """Download historical data with gap tracking, stopping at END_TIME"""
    global start_time_holder
    
    consecutive_missing_count = 0
    first_missing_time = None  # Track where the gap started
    
    while True:
        current_time = start_time_holder

        # --- Stop condition: do not go past END_TIME ---
        if current_time > END_TIME:
            print(f"🛑 Reached end date ({END_TIME.strftime('%Y-%m-%d %H:%M')}). Stopping.")
            return

        # Check if we've caught up to present (leaving 2 hours buffer for data availability)
        if current_time >= datetime.now() - timedelta(hours=2):
            print("🏁 Đã hoàn thành tải lịch sử.")
            print("🔄 Chuyển sang chế độ theo dõi thời gian thực.")
            return  # Exit to switch to real-time mode
        
        try:
            print(f"\n🚀 [HISTORICAL] Đang kiểm tra: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Check if this timestamp is in missing log
            if current_time in missing_data_log:
                print(f"🔍 This timestamp was previously logged as missing - attempting recovery...")
            
            with FTP(FTP_HOST, timeout=30) as ftp:
                ftp.login(FTP_USER, FTP_PASS)
                
                # Build paths
                ymd = current_time.strftime("%Y%m")
                dd = current_time.strftime("%d")
                # hh = current_time.strftime("%H")
                remote_path = f"{BASE_DIR}/{ymd}/{dd}/"
                local_path = os.path.join(LOCAL_BASE, ymd, dd)

                # Download and process
                directory_found = download_and_process(ftp, remote_path, local_path, current_time, missing_data_log)

            if directory_found:
                # Reset missing counter when data is found
                consecutive_missing_count = 0
                first_missing_time = None
                
                # Move to next hour
                start_time_holder += timedelta(days=1)
                time.sleep(1)
                
            else:
                # Data not found
                if first_missing_time is None:
                    first_missing_time = current_time
                
                consecutive_missing_count += 1
                
                # Check if we've exceeded the threshold
                if consecutive_missing_count >= MAX_CONSECUTIVE_MISSING:
                    print(f"⚠️ {MAX_CONSECUTIVE_MISSING} giờ liên tiếp không có dữ liệu từ {first_missing_time.strftime('%Y-%m-%d %H:%M')}.")
                    print("🔄 Giả định đã đuổi kịp dữ liệu mới nhất. Chuyển sang chế độ real-time.")
                    
                    start_time_holder = first_missing_time
                    return  # Exit to switch to real-time mode
                else:
                    # Continue to next hour to find where data resumes
                    print(f"⏩ Bỏ qua giờ này ({consecutive_missing_count}/{MAX_CONSECUTIVE_MISSING} missing). Tiếp tục...")
                    start_time_holder += timedelta(days=1)
                    time.sleep(1)

        except Exception as e:
            print(f"⚠️ Lỗi khi kết nối FTP: {e}")
            time.sleep(30)


def realtime_mode():
    """Monitor and download real-time data"""
    global start_time_holder
    
    missing_data_log = {}
    
    while True:
        current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)

        # --- Stop condition: do not download beyond END_TIME ---
        if current_hour > END_TIME:
            print(f"🛑 Current time is past end date ({END_TIME.strftime('%Y-%m-%d %H:%M')}). Real-time mode disabled.")
            print("ℹ️  Update END_TIME in the script when you are ready to download 2026 data.")
            return

        check_times = [current_hour - timedelta(days=6), current_hour - timedelta(days=1)]
        # Only check timestamps within the allowed range
        check_times = [t for t in check_times if t <= END_TIME]

        try:
            for check_time in check_times:
                print(f"\n🚀 [REAL-TIME] Đang kiểm tra: {check_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                with FTP(FTP_HOST, timeout=30) as ftp:
                    ftp.login(FTP_USER, FTP_PASS)
                    
                    # Build paths
                    ymd = check_time.strftime("%Y%m")
                    dd = check_time.strftime("%d")
                    # hh = check_time.strftime("%H")
                    remote_path = f"{BASE_DIR}/{ymd}/{dd}/"
                    local_path = os.path.join(LOCAL_BASE, ymd, dd)

                    download_and_process(ftp, remote_path, local_path, check_time, missing_data_log)
                    
                    time.sleep(1)
            
            print(f"\n⏳ [REAL-TIME] Chờ 10 phút trước khi kiểm tra lại...")
            time.sleep(600)

        except Exception as e:
            print(f"⚠️ Lỗi khi kết nối FTP: {e}")
            time.sleep(30)


def main():
    """Main loop for managing historical and real-time modes"""
    global start_time_holder
    
    print("📖 Loading missing data log...")
    missing_data_log = load_missing_data_log()
    
    while True:
        # Check if we should start in real-time mode
        if start_time_holder >= datetime.now() - timedelta(hours=6):
            print("🔄 Bắt đầu ở chế độ real-time.")
            realtime_mode()
        else:
            print("📚 Bắt đầu tải dữ liệu lịch sử.")
            historical_mode(missing_data_log)
            
            # After historical mode completes, switch to real-time
            realtime_mode()


if __name__ == "__main__":
    main()