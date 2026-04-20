import os
import glob
import numpy as np
import pandas as pd
import rasterio
from pathlib import Path
from tqdm import tqdm
from pprint import pprint
import argparse
from datetime import datetime

# ── Paths ──────────────────────────────────────────────────────────────
STATION_CSV = "/opt/airflow/dags/Masterdata/envisoft_stations.csv"

L2_AOD_DIR = "/home/slow_data/Air_Quality/Himawari/L2_AOD"
L2_OUTPUT_DIR = "/home/slow_data/Air_Quality/Himawari/station_aod/envisoft_stations"

L3_AOD_DIR = "/home/slow_data/Air_Quality/Himawari/L3_AOD"
L3_OUTPUT_DIR = "/home/slow_data/Air_Quality/Himawari/station_aod/L3_envisoft_stations"

LOG_DIR = "/home/slow_data/Air_Quality/Himawari/logs"
_log_tag = ""

# ── Band definitions ──────────────────────────────────────────────────
L2_BANDS = ['AOT', 'Uncertainty', 'AE', 'QA_flag', 'SSA', 'RF']
L3_BANDS = [
    'AOT_Merged', 'AOT_Pure', 'AOT_Merged_uncertainty', 'AOT_Pure_uncertainty',
    'AE_Merged', 'AE_Pure', 'QA_flag_Merged', 'QA_flag_Pure',
    'AOT_L2_Mean', 'AOT_L2_SDV', 'AOT_L2_Num',
    'AE_L2_Mean', 'AE_L2_SDV', 'AE_L2_Num',
]


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


# ── Shared helpers ─────────────────────────────────────────────────────
def get_existing_timestamps(station_csv_path: str) -> set:
    if not os.path.isfile(station_csv_path):
        return set()
    try:
        df = pd.read_csv(station_csv_path, usecols=['timestamp'])
        return set(df['timestamp'].astype(str).tolist())
    except Exception:
        return set()


def build_station_info(stations_df: pd.DataFrame, output_dir: str) -> list[dict]:
    os.makedirs(output_dir, exist_ok=True)
    info = []
    for _, row in stations_df.iterrows():
        name = str(row['station_name'])
        csv_path = os.path.join(output_dir, f"{name}.csv")
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        info.append({
            'station_name': name,
            'lon': row['longitude'],
            'lat': row['latitude'],
            'csv_path': csv_path,
        })
    return info


def parse_timestamp(filename: str, level: str) -> str:
    """Parse raw timestamp string from filename (UTC)."""
    parts = filename.split("_")
    if level == "L2":
        return parts[4] + "_" + parts[5]
    else:  # L3
        return parts[3] + "_" + parts[4]


def to_gmt7_timestamp(raw_ts: str) -> str:
    """Convert raw UTC timestamp string (YYYYMMDD_HHMM) to GMT+7 formatted string."""
    dt = pd.to_datetime(raw_ts, format='%Y%m%d_%H%M')
    dt = dt + pd.Timedelta(hours=7)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def replace_nan(value):
    """Replace NaN/None with -9999."""
    if value is None:
        return -9999
    try:
        if np.isnan(value):
            return -9999
    except (TypeError, ValueError):
        pass
    return value


def extract_aod(
    years: list[int],
    aod_dir: str,
    output_dir: str,
    band_names: list[str],
    band_start: int,
    glob_pattern_template: str,
    level: str,
) -> dict:
    """
    Generic AOD extraction for both L2 and L3.

    Parameters
    ----------
    band_start : int
        First rasterio band index to read (1 for L2, 2 for L3).
    glob_pattern_template : str
        Pattern with {year} placeholder, relative to aod_dir.
    level : str
        'L2' or 'L3' – controls timestamp parsing.
    """
    stations_df = pd.read_csv(STATION_CSV)
    station_info = build_station_info(stations_df, output_dir)

    n_bands = len(band_names)
    band_end = band_start + n_bands  # exclusive

    # ── Load existing timestamps ONCE into memory per station ──
    ts_cache: dict[str, set] = {}
    for s in station_info:
        ts_cache[s['csv_path']] = get_existing_timestamps(s['csv_path'])
    print(f"[{level}] Loaded timestamp cache for {len(station_info)} stations.")

    stats = {'total': 0, 'skipped': 0, 'processed': 0, 'error': 0}

    years_str = f"{years[0]}-{years[-1]}" if len(years) > 1 else str(years[0])
    _wlog("=" * 60)
    _wlog(f"START  level={level}  years={years_str}")

    for year in years:
        pattern = os.path.join(aod_dir, glob_pattern_template.format(year=year))
        files = sorted(glob.glob(pattern))
        print(f"[{level}] Year {year}: found {len(files)} files.")
        _wlog(f"YEAR={year}  tif_files_found={len(files)}")
        stats['total'] += len(files)

        for aod_file in tqdm(files, desc=f"{level}-{year}", unit="file"):
            try:
                filename = os.path.basename(aod_file)
                raw_ts = parse_timestamp(filename, level)
                timestamp = to_gmt7_timestamp(raw_ts)

                # Quick check: skip if ALL stations already have this timestamp
                if all(timestamp in ts_cache[s['csv_path']] for s in station_info):
                    stats['skipped'] += 1
                    continue

                with rasterio.open(aod_file) as src:
                    bands_data = [src.read(i) for i in range(band_start, band_end)]

                    for s in station_info:
                        if timestamp in ts_cache[s['csv_path']]:
                            continue

                        extracted_values: dict = {'timestamp': timestamp}
                        try:
                            row_idx, col_idx = src.index(s['lon'], s['lat'])
                            for i, name in enumerate(band_names):
                                extracted_values[name] = replace_nan(bands_data[i][row_idx, col_idx])
                        except Exception:
                            for name in band_names:
                                extracted_values[name] = -9999

                        new_df = pd.DataFrame([extracted_values])
                        file_exists = os.path.isfile(s['csv_path'])
                        new_df.to_csv(s['csv_path'], mode='a', header=not file_exists, index=False)

                        # ── Update cache so we don't re-process ──
                        ts_cache[s['csv_path']].add(timestamp)

                stats['processed'] += 1
                if level == "L2":
                    tqdm.write(f"  Processed {timestamp}")

            except Exception as e:
                stats['error'] += 1
                err_msg = str(e)
                tqdm.write(f"Error: {aod_file}: {err_msg}")
                _wlog(f"ERROR  file={os.path.basename(aod_file)}  reason={err_msg}")

    _wlog("-" * 60)
    _wlog(
        f"SUMMARY  total={stats['total']}  processed={stats['processed']}  "
        f"skipped={stats['skipped']}  errors={stats['error']}"
    )
    _wlog("END")
    _wlog("=" * 60)

    pprint(stats)
    return stats


# ── L2 / L3 wrappers ──────────────────────────────────────────────────
def extract_l2_aod(years: list[int]) -> dict:
    return extract_aod(
        years=years,
        aod_dir=L2_AOD_DIR,
        output_dir=L2_OUTPUT_DIR,
        band_names=L2_BANDS,
        band_start=1,
        glob_pattern_template=r'{year}??/??/??/aod_vietnam_NC_H??_*_L2ARP031_FLDK.*.tif',
        level="L2",
    )


def extract_l3_aod(years: list[int]) -> dict:
    return extract_aod(
        years=years,
        aod_dir=L3_AOD_DIR,
        output_dir=L3_OUTPUT_DIR,
        band_names=L3_BANDS,
        band_start=2,
        glob_pattern_template=r'{year}??/*/aod_vietnam_H??_*_1HARP031_FLDK.*.tif',
        level="L3",
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Extract stations AOD")
    parser.add_argument(
        "--level", required=True, choices=["L2", "L3"],
        help="Data level to extract: L2 (hourly) or L3 (daily)"
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
    _log_tag = f"extract_aod_{level.lower()}_status"

    if level == 'L2':
        print("=" * 60)
        print("Extracting L2 AOD ...")
        print("=" * 60)
        extract_l2_aod(years)

    if level == 'L3':
        print("=" * 60)
        print("Extracting L3 AOD ...")
        print("=" * 60)
        extract_l3_aod(years)


if __name__ == "__main__":
    # python extract_stations_aod.py --level L2 --years 2022 2023 2024 2025 2026
    # python extract_stations_aod.py --level L3 --years 2022 2023 2024 2025 2026

    main()
