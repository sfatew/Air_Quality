import os
import argparse
import time
from ftplib import FTP
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import sys

current_path = Path(__file__).resolve()

root_dir = current_path.parents[2]

sys.path.append(str(root_dir))


# --- FTP Configuration ---
import yaml

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

FTP_HOST = config['HIMAWARI']['FTP_HOST']
FTP_USER = config['HIMAWARI']['FTP_USER']
FTP_PASS = config['HIMAWARI']['FTP_PASS']

MAX_CONSECUTIVE_MISSING = 24
LOG_DIR = "/home/slow_data/Air_Quality/Himawari/logs"
_log_tag = ""

# --- Level-specific configuration ---
LEVEL_CONFIG = {
    "L2": {
        "base_dir": "/pub/himawari/L2/ARP/031",
        "local_base": "/home/slow_data/Air_Quality/Himawari/L2_AOD",
        "process_script": "/opt/airflow/dags/Himawari/process_nc_to_tif/process_aod_data_L2.py",
        "time_step": timedelta(hours=1),
        "realtime_lookback": [timedelta(hours=6), timedelta(hours=1)],
        "realtime_threshold": timedelta(hours=6),
    },
    "L3": {
        "base_dir": "/pub/himawari/L3/ARP/031",
        "local_base": "/home/slow_data/Air_Quality/Himawari/L3_AOD",
        "process_script": "/opt/airflow/dags/Himawari/process_nc_to_tif/process_aod_data_L3.py",
        "time_step": timedelta(days=1),
        "realtime_lookback": [timedelta(days=6), timedelta(days=1)],
        "realtime_threshold": timedelta(hours=6),
    },
}


def _wlog(message):
    os.makedirs(LOG_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_path = os.path.join(LOG_DIR, datetime.now().strftime("%Y-%m-%d") + ".log")
    line = f"[{ts}] [{_log_tag}] {message}\n"
    try:
        with open(log_path, "a") as f:
            f.write(line)
    except Exception as e:
        print(f"Warning: Could not write to log: {e}")


def build_remote_path(base_dir, current_time, level):
    ymd = current_time.strftime("%Y%m")
    dd = current_time.strftime("%d")
    if level == "L2":
        hh = current_time.strftime("%H")
        return f"{base_dir}/{ymd}/{dd}/{hh}/"
    return f"{base_dir}/{ymd}/{dd}/"


def build_local_path(local_base, current_time, level):
    ymd = current_time.strftime("%Y%m")
    dd = current_time.strftime("%d")
    if level == "L2":
        hh = current_time.strftime("%H")
        return os.path.join(local_base, ymd, dd, hh)
    return os.path.join(local_base, ymd, dd)


def get_local_files(local_path):
    """Return (tif_stems, nc_stems) from local directory.
    tif_stems: set of stems derived from aod_vietnam_*.tif files
    nc_stems: set of stems derived from *.nc files
    """
    tif_stems = set()
    nc_stems = set()
    if not os.path.exists(local_path):
        return tif_stems, nc_stems
    try:
        for f in os.listdir(local_path):
            if f.endswith(".tif"):
                tif_stems.add(f.removeprefix("aod_vietnam_").removesuffix(".tif"))
            elif f.endswith(".nc"):
                nc_stems.add(f.removesuffix(".nc"))
    except Exception as e:
        print(f"Warning: Error reading local directory {local_path}: {e}")
    return tif_stems, nc_stems


def fetch_file(file, local_path, ftp, process_script) -> bool:
    local_file = os.path.join(local_path, file)
    try:
        with open(local_file, "wb") as f:
            for attempt in range(3):
                try:
                    ftp.retrbinary(f"RETR {file}", f.write)
                    break
                except Exception:
                    if attempt < 2:
                        print(f"Retry {attempt+1} for {file}")
                        time.sleep(5)
                    else:
                        raise
        print(f"Downloaded: {file}")
        subprocess.run(["python", process_script, local_file], check=True, timeout=300)
        return True
    except subprocess.TimeoutExpired:
        reason = "Processing timed out (5 min)"
        print(f"Error: {reason}: {file}")
        _wlog(f"FILE_ERROR   {file}  reason={reason}")
        if os.path.exists(local_file):
            os.remove(local_file)
        return False
    except Exception as file_error:
        reason = str(file_error)
        print(f"Error downloading/processing {file}: {reason}")
        _wlog(f"FILE_ERROR   {file}  reason={reason}")
        if os.path.exists(local_file):
            os.remove(local_file)
        return False


def download_and_process(ftp, remote_path, local_path, timestamp, process_script, stats):
    os.makedirs(local_path, exist_ok=True)
    ts_str = timestamp.strftime('%Y-%m-%d %H:%M')
    stats['checked'] += 1
    try:
        ftp.cwd(remote_path)
        remote_files = ftp.nlst()
        remote_nc_files = [f for f in remote_files if f.endswith('.nc')]
        tif_stems, nc_stems = get_local_files(local_path)

        files_to_download = []
        for f in remote_nc_files:
            stem = f.removesuffix(".nc")
            if stem in tif_stems:
                continue  # already processed to tif
            if stem in nc_stems:
                continue  # already downloaded as nc
            files_to_download.append(f)

        if not files_to_download:
            if remote_nc_files:
                print(f"Checked {remote_path}. All {len(remote_nc_files)} files already exist (tif or nc).")
                _wlog(f"SKIPPED      {ts_str}  all {len(remote_nc_files)} files already exist (tif or nc)")
            else:
                print(f"Checked {remote_path}. No .nc files.")
                _wlog(f"SKIPPED      {ts_str}  no .nc files found on remote")
            stats['skipped'] += 1
            return True

        print(f"Downloading {len(files_to_download)} new files from {remote_path}...")
        results = [fetch_file(f, local_path, ftp, process_script) for f in files_to_download]
        n_ok = sum(results)
        n_err = len(results) - n_ok
        _wlog(f"DOWNLOADED   {ts_str}  files={n_ok}  file_errors={n_err}  path={remote_path}")
        stats['downloaded'] += 1
        stats['file_errors'] += n_err
        return True

    except Exception as e:
        error_msg = str(e)
        print(f"Cannot access directory {remote_path}: {error_msg}")
        _wlog(f"ERROR        {ts_str}  path={remote_path}  reason={error_msg}")
        stats['errors'] += 1
        return False


def historical_mode(cfg, level, start_time, end_time, stats):
    consecutive_missing_count = 0
    first_missing_time = None
    current_time = start_time

    while current_time <= end_time:
        if current_time >= datetime.now() - timedelta(hours=2):
            print("Finished historical download. Switching to real-time mode.")
            return current_time

        try:
            print(f"\n[HISTORICAL] Checking: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

            with FTP(FTP_HOST, timeout=30) as ftp:
                ftp.login(FTP_USER, FTP_PASS)
                remote_path = build_remote_path(cfg["base_dir"], current_time, level)
                local_path = build_local_path(cfg["local_base"], current_time, level)
                directory_found = download_and_process(
                    ftp, remote_path, local_path, current_time,
                    cfg["process_script"], stats
                )

            if directory_found:
                consecutive_missing_count = 0
                first_missing_time = None
                current_time += cfg["time_step"]
                time.sleep(1)
            else:
                if first_missing_time is None:
                    first_missing_time = current_time
                consecutive_missing_count += 1
                if consecutive_missing_count >= MAX_CONSECUTIVE_MISSING:
                    print(f"{MAX_CONSECUTIVE_MISSING} consecutive missing entries from {first_missing_time.strftime('%Y-%m-%d %H:%M')}.")
                    print("Assuming caught up. Switching to real-time mode.")
                    return first_missing_time
                print(f"Skipping ({consecutive_missing_count}/{MAX_CONSECUTIVE_MISSING} missing). Continuing...")
                current_time += cfg["time_step"]
                time.sleep(1)

        except Exception as e:
            print(f"FTP connection error: {e}")
            time.sleep(30)

    print(f"Reached end date ({end_time.strftime('%Y-%m-%d %H:%M')}). Done.")
    return current_time


def realtime_mode(cfg, level, end_time, stats):
    while True:
        current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
        if current_hour > end_time:
            print(f"Current time is past end date ({end_time.strftime('%Y-%m-%d %H:%M')}). Real-time mode disabled.")
            return

        check_times = [current_hour - lb for lb in cfg["realtime_lookback"]]
        check_times = [t for t in check_times if t <= end_time]

        try:
            for check_time in check_times:
                print(f"\n[REAL-TIME] Checking: {check_time.strftime('%Y-%m-%d %H:%M:%S')}")
                with FTP(FTP_HOST, timeout=30) as ftp:
                    ftp.login(FTP_USER, FTP_PASS)
                    remote_path = build_remote_path(cfg["base_dir"], check_time, level)
                    local_path = build_local_path(cfg["local_base"], check_time, level)
                    download_and_process(
                        ftp, remote_path, local_path, check_time,
                        cfg["process_script"], stats
                    )
                    time.sleep(1)

            print(f"\n[REAL-TIME] Waiting 10 minutes before next check...")
            time.sleep(600)

        except Exception as e:
            print(f"FTP connection error: {e}")
            time.sleep(30)


def parse_args():
    parser = argparse.ArgumentParser(description="Download Himawari AOD data (L2/L3)")
    parser.add_argument(
        "--level", required=True, choices=["L2", "L3"],
        help="Data level to download: L2 (hourly) or L3 (daily)"
    )
    parser.add_argument(
        "--years", required=True, nargs="+", type=int,
        help="List of years to download, e.g. --years 2022 2023 2024 2025 2026"
    )
    return parser.parse_args()


def main():
    global _log_tag
    args = parse_args()
    level = args.level
    years = sorted(args.years)
    _log_tag = f"download_himawari_{level.lower()}_status"

    cfg = LEVEL_CONFIG[level]
    start_time = datetime(years[0], 1, 1, 0, 0)
    end_time = datetime(years[-1], 12, 31, 23, 0)

    years_str = f"{years[0]}-{years[-1]}" if len(years) > 1 else str(years[0])
    stats = {'checked': 0, 'downloaded': 0, 'skipped': 0, 'errors': 0, 'file_errors': 0}

    _wlog("=" * 60)
    _wlog(f"START  level={level}  years={years_str}")

    print(f"Level: {level}")
    print(f"Date range: {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}")

    current_time = start_time
    if current_time >= datetime.now() - cfg["realtime_threshold"]:
        print("Starting in real-time mode.")
        _wlog("MODE=REALTIME")
        realtime_mode(cfg, level, end_time, stats)
    else:
        print("Starting historical download.")
        _wlog("MODE=HISTORICAL")
        current_time = historical_mode(cfg, level, current_time, end_time, stats)
        if current_time <= end_time:
            _wlog("MODE=REALTIME  (switching from historical)")
            realtime_mode(cfg, level, end_time, stats)

    _wlog("-" * 60)
    _wlog(
        f"SUMMARY  checked={stats['checked']}  downloaded={stats['downloaded']}  "
        f"skipped={stats['skipped']}  errors={stats['errors']}  file_errors={stats['file_errors']}"
    )
    _wlog("END")
    _wlog("=" * 60)


if __name__ == "__main__":

    # Examples Usages:
    # Download L2 data for 2022-2026
        # python download_himawari.py --level L2 --years 2022 2023 2024 2025 2026
    # Download L3 data for 2024-2025
        # python download_himawari.py --level L3 --years 2024 2025

    main()
