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

from util import Limit_download_log

from config.config import aod_config


# --- FTP and Directory Configuration ---
FTP_HOST = "ftp.ptree.jaxa.jp"
FTP_USER = aod_config.FTP_USER
FTP_PASS = aod_config.FTP_PASS
BASE_DIR = "/pub/himawari/L2/ARP/031"

# Cấu hình thư mục local
LOCAL_BASE = "/home/slow_data/Air_Quality/AOD/full_aod"
PROCESS_SCRIPT = "/home/work1/projects/Air_Quality/AOD data/process_aod_data.py"
LOG_FILE = "/home/work1/projects/Air_Quality/AOD data/downloaded_files.log"

# Thời gian bắt đầu lịch sử để tải về (Starting time for historical download)
start_time_holder = datetime(2022, 9, 27, 0, 0)

# Biến cờ để theo dõi chế độ hoạt động
is_real_time_mode = False

# --- Log Management ---
# Global set to track downloaded files.
downloaded = Limit_download_log.Limit_download_log(max_size=1000, log_path=LOG_FILE, log_limit_lines=10000)

def fetch_file(file, local_path, ftp):
    local_file = os.path.join(local_path, file)
    try:
        with open(local_file, "wb") as f:
            # Retrieve the file
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
        # Gọi xử lý file .nc 
        subprocess.run(["python", PROCESS_SCRIPT, local_file], check=True, timeout=300) 
        downloaded.add(file)
    except subprocess.TimeoutExpired:
                print(f"❌ Lỗi: Xử lý file {file} đã hết thời gian (5 phút).")
    except Exception as file_error:
        print(f"❌ Lỗi khi tải hoặc xử lý file {file}: {file_error}")
        # Clean up partially downloaded file
        if os.path.exists(local_file):
            os.remove(local_file)

# --- Core FTP and Processing Logic ---
def download_and_process(ftp, remote_path, local_path):
    # ... (same as before) ...
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
        files = ftp.nlst()
        files_to_download = [f for f in files if f not in downloaded and f.endswith('.nc')]
        
        if not files_to_download:
            print(f"✔️ Đã kiểm tra {remote_path}. Không có file .nc mới để tải.")
            # Return True because the directory was successfully accessed
            return True 

        print(f"📥 Bắt đầu tải {len(files_to_download)} file từ {remote_path}...")
        
        for file in files_to_download:
            fetch_file(file, local_path, ftp)

        return True # Successfully accessed the directory

    except Exception as e:
        # This catches errors like 550 (directory not found) or connection issues
        print(f"⛔ Không truy cập được thư mục {remote_path}: {e}")
        return False

def main():
    """Main loop for connecting, downloading, and advancing the time cursor."""
    global start_time_holder
    global downloaded # Declare global to modify the set directly
    global is_real_time_mode

    while True:
        current_time = start_time_holder
        
        try:
            print(f"\n🚀 Đang kết nối và kiểm tra dữ liệu cho: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # --- FTP Connection Setup (REMAINS PER-LOOP FOR ROBUSTNESS) ---
            with FTP(FTP_HOST, timeout=30) as ftp:
                ftp.login(FTP_USER, FTP_PASS)
                
                # Build paths
                ymd = current_time.strftime("%Y%m")
                dd = current_time.strftime("%d")
                hh = current_time.strftime("%H")
                remote_path = f"{BASE_DIR}/{ymd}/{dd}/{hh}/"
                local_path = os.path.join(LOCAL_BASE, ymd, dd, hh)

                # --- Download and Process ---
                directory_found = download_and_process(ftp, remote_path, local_path)

            # --- Time Cursor Advancement Logic ---
            if directory_found:

                # Directory was successfully checked. Move to the next hour.
                start_time_holder += timedelta(hours=1)
                if is_real_time_mode:
                    time.sleep(1) 
                
                # Check if we have caught up to the present
                if start_time_holder > datetime.now():
                    if not is_real_time_mode:
                        print("🏁 Đã hoàn thành tải lịch sử.")
                        
                        print("🔄 Chuyển sang chế độ theo dõi thời gian thực.")
                        is_real_time_mode = True
                        
                    # Trong chế độ Real-time, chúng ta kiểm tra lại cùng một giờ (hoặc giờ trước đó)
                    # để đảm bảo không bỏ sót dữ liệu vừa xuất hiện.
                    current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
                    start_time_holder = current_hour - timedelta(hours=1)
                    
            else:
                print(f"⏳ Không có dữ liệu, thử lại sau 10 phút...\n")
                time.sleep(600)  # 10 phút

        except Exception as e:
            print(f"⚠️ Lỗi khi kết nối FTP: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()