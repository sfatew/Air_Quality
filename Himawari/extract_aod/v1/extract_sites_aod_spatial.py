"""
Extract Himawari L2/L3 AOD for all target stations.

L2 (10-min cadence, ~48 files/day during daylight hours):
- 5×5 pixel grid for AOT (25 raw values + stats for spatial gradient features)
  At ~5 km/pixel resolution, 5×5 covers ~25×25 km around each station
- Exact pixel for metadata bands (Uncertainty, AE, QA_flag, SSA, RF)

L3 (hourly composite):
- 5×5 pixel grid for AOT_Merged and AOT_Pure
- Exact pixel for uncertainty, AE, QA_flag, and L2-aggregated statistics

- All timestamps converted UTC → GMT+7 (Vietnam local time)
- NaN for missing data (no -9999 sentinels)
- Incremental: re-running skips already-processed timestamps (safe to resume)

Usage:
    python extract_stations_aod_spatial.py --level L2 --years 2024
    python extract_stations_aod_spatial.py --level L2 --years 2022 2023 2024 2025 2026
    python extract_stations_aod_spatial.py --level L3 --years 2024
    python extract_stations_aod_spatial.py --level L3 --years 2022 2023 2024 2025 2026
    python extract_stations_aod_spatial.py --level L2 --years 2024 --station-csv ./my.csv
"""

import os
import glob
import argparse
from datetime import datetime

import numpy as np
import pandas as pd
import rasterio
from tqdm import tqdm


# ══════════════════════════════════════════════════════════════════════
#  Configuration — adjust these paths to your workstation
# ══════════════════════════════════════════════════════════════════════

STATION_CSV   = "/home/work1/projects/Air_Quality/Masterdata/AERONET_sites.csv"
LOG_DIR       = "/home/slow_data/Air_Quality/Himawari/logs"

# ── L2 configuration ───────────────────────────────────────────────────
L2_AOD_DIR    = "/home/slow_data/Air_Quality/Himawari/L2_AOD"
L2_OUTPUT_DIR = "/home/slow_data/Air_Quality/Himawari/sites_aod_v3/L2"
L2_GLOB_TEMPLATE = r'{year}??/??/??/aod_vietnam_NC_H??_*_L2ARP031_FLDK.*.tif'

# 5×5 for primary AOD; exact pixel for retrieval metadata
L2_GRID_BANDS  = {'AOT': 1}
L2_EXACT_BANDS = {'Uncertainty': 2, 'AE': 3, 'QA_flag': 4, 'SSA': 5, 'RF': 6}

# ── L3 configuration ───────────────────────────────────────────────────
L3_AOD_DIR    = "/home/slow_data/Air_Quality/Himawari/L3_AOD"
L3_OUTPUT_DIR = "/home/slow_data/Air_Quality/Himawari/sites_aod_v3/L3"
L3_GLOB_TEMPLATE = r'{year}??/*/aod_vietnam_H??_*_1HARP031_FLDK.*.tif'

# Band index starts at 2 for L3 (band 1 is unused / a different variable)
# 5×5 for primary AOD composites; exact pixel for uncertainty/QA/AE/L2-stats
L3_GRID_BANDS  = {'AOT_Merged': 2, 'AOT_Pure': 3}
L3_EXACT_BANDS = {
    'AOT_Merged_uncertainty': 4,
    'AOT_Pure_uncertainty':   5,
    'AE_Merged':              6,
    'AE_Pure':                7,
    'QA_flag_Merged':         8,
    'QA_flag_Pure':           9,
    'AOT_L2_Mean':           10,
    'AOT_L2_SDV':            11,
    'AOT_L2_Num':            12,
    'AE_L2_Mean':            13,
    'AE_L2_SDV':             14,
    'AE_L2_Num':             15,
}

# Values treated as missing
FILL_VALUES = {-9999.0, -999.0, 9999.0}

# 5×5 pixel layout: (row_offset, col_offset, column_suffix)
#   m2 = minus 2, m1 = minus 1, _0 = center, p1 = plus 1, p2 = plus 2
#   Row = north–south (m2 = 2 pixels north), Col = west–east (m2 = 2 pixels west)
#
#   m2m2  m2m1  m2_0  m2p1  m2p2     ← north outer row
#   m1m2  m1m1  m1_0  m1p1  m1p2     ← north inner row
#   _0m2  _0m1  _0_0  _0p1  _0p2     ← center row (_0_0 = station pixel)
#   p1m2  p1m1  p1_0  p1p1  p1p2     ← south inner row
#   p2m2  p2m1  p2_0  p2p1  p2p2     ← south outer row

GRID_OFFSETS = [
    (-2, -2, "m2m2"), (-2, -1, "m2m1"), (-2,  0, "m2_0"), (-2,  1, "m2p1"), (-2,  2, "m2p2"),
    (-1, -2, "m1m2"), (-1, -1, "m1m1"), (-1,  0, "m1_0"), (-1,  1, "m1p1"), (-1,  2, "m1p2"),
    ( 0, -2, "_0m2"), ( 0, -1, "_0m1"), ( 0,  0, "_0_0"), ( 0,  1, "_0p1"), ( 0,  2, "_0p2"),
    ( 1, -2, "p1m2"), ( 1, -1, "p1m1"), ( 1,  0, "p1_0"), ( 1,  1, "p1p1"), ( 1,  2, "p1p2"),
    ( 2, -2, "p2m2"), ( 2, -1, "p2m1"), ( 2,  0, "p2_0"), ( 2,  1, "p2p1"), ( 2,  2, "p2p2"),
]

# Inner 3×3 pixel offsets (subset of above, for inner vs outer stats)
INNER_LABELS = {"m1m1", "m1_0", "m1p1", "_0m1", "_0_0", "_0p1", "p1m1", "p1_0", "p1p1"}

_log_tag = "extract_l2_v3"


# ══════════════════════════════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════════════════════════════

def wlog(msg: str) -> None:
    """Append a timestamped line to the daily log."""
    os.makedirs(LOG_DIR, exist_ok=True)
    now = datetime.now()
    path = os.path.join(LOG_DIR, now.strftime("%Y-%m-%d") + ".log")
    try:
        with open(path, "a") as f:
            f.write(f"[{now:%Y-%m-%d %H:%M:%S}] [{_log_tag}] {msg}\n")
    except Exception:
        pass


def parse_timestamp_utc(filename: str) -> str:
    """Extract raw UTC timestamp YYYYMMDD_HHMM from L2 filename.

    Example: aod_vietnam_NC_H08_20240115_0230_L2ARP031_FLDK.02.tif
             parts[0] [1]  [2]  [3]  [4]      [5]     [6]     [7]
    """
    parts = filename.split("_")
    return parts[4] + "_" + parts[5]

def parse_timestamp_utc_l3(filename: str) -> str:
    """Extract raw UTC timestamp YYYYMMDD_HHMM from L3 filename.

    Example: aod_vietnam_H08_20240115_0000_1HARP031_FLDK.02.tif
             parts[0] [1]  [2]  [3]      [4]     [5]     [6]
    """
    parts = filename.split("_")
    return parts[3] + "_" + parts[4]


def utc_to_gmt7(raw_ts: str) -> str:
    """Convert 'YYYYMMDD_HHMM' (UTC) → 'YYYY-MM-DD HH:MM:SS' (GMT+7 Vietnam)."""
    dt = pd.to_datetime(raw_ts, format='%Y%m%d_%H%M') + pd.Timedelta(hours=7)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def load_stations(csv_path: str) -> pd.DataFrame:
    """Load station CSV. Handles both naming conventions."""
    df = pd.read_csv(csv_path)
    renames = {}
    if 'station_name' in df.columns and 'stationName' not in df.columns:
        renames['station_name'] = 'stationName'
    if 'station_id' in df.columns and 'stationId' not in df.columns:
        renames['station_id'] = 'stationId'
    if renames:
        df = df.rename(columns=renames)
    df['stationId'] = df['stationId'].astype(str)
    return df


def get_existing_timestamps(csv_path: str) -> set:
    """Load set of timestamps already written to a station CSV."""
    if not os.path.isfile(csv_path):
        return set()
    try:
        return set(pd.read_csv(csv_path, usecols=['timestamp'])['timestamp'].astype(str))
    except Exception:
        return set()


def append_rows(csv_path: str, rows: list[dict]) -> None:
    """Append dicts to CSV. Creates file + header if new."""
    if not rows:
        return
    df = pd.DataFrame(rows)
    exists = os.path.isfile(csv_path)
    if exists:
        old_cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
        new_cols = [c for c in df.columns if c not in old_cols]
        df = df.reindex(columns=old_cols + new_cols)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, mode='a', header=not exists, index=False)


# ══════════════════════════════════════════════════════════════════════
#  Pixel extraction
# ══════════════════════════════════════════════════════════════════════

def extract_5x5(band_data: np.ndarray, row_idx: int, col_idx: int,
                prefix: str) -> dict:
    """
    Extract 5×5 raw pixel grid centered on (row_idx, col_idx).

    At Himawari ~5 km/pixel, this covers 25×25 km around the station.

    Output columns (using prefix='AOT' as example):
      25 raw pixels:
        AOT_m2m2 .. AOT_m2p2   (north outer row)
        AOT_m1m2 .. AOT_m1p2   (north inner row)
        AOT__0m2 .. AOT__0p2   (center row, _0_0 = station pixel)
        AOT_p1m2 .. AOT_p1p2   (south inner row)
        AOT_p2m2 .. AOT_p2p2   (south outer row)

      Summary stats:
        AOT_valid_count         total non-NaN pixels (0–25)
        AOT_mean                mean of all valid pixels
        AOT_std                 spatial std of all valid pixels
        AOT_center              alias for _0_0

      Inner 3×3 vs outer ring:
        AOT_inner_count         valid pixels in inner 3×3 (0–9)
        AOT_inner_mean          mean of inner 3×3 valid pixels
        AOT_outer_count         valid pixels in outer ring (0–16)
        AOT_outer_mean          mean of outer ring valid pixels

    Downstream gradient features you can compute from these 25 values:
        grad_NS    = (south_rows_mean - north_rows_mean)
        grad_EW    = (east_cols_mean  - west_cols_mean)
        local_vs_regional = inner_mean - outer_mean  (hotspot detection)
        upwind_aod = select pixels using wind direction
    """
    h, w = band_data.shape
    result = {}
    all_valid = []
    inner_valid = []
    outer_valid = []

    for dr, dc, label in GRID_OFFSETS:
        r, c = row_idx + dr, col_idx + dc
        if 0 <= r < h and 0 <= c < w:
            val = float(band_data[r, c])
            if np.isnan(val) or val in FILL_VALUES:
                val = np.nan
            else:
                all_valid.append(val)
                if label in INNER_LABELS:
                    inner_valid.append(val)
                else:
                    outer_valid.append(val)
        else:
            val = np.nan
        result[f"{prefix}_{label}"] = val

    # Overall stats (all 25 pixels)
    n_all = len(all_valid)
    result[f"{prefix}_valid_count"] = n_all
    result[f"{prefix}_mean"]   = float(np.mean(all_valid)) if n_all > 0 else np.nan
    result[f"{prefix}_std"]    = float(np.std(all_valid))  if n_all > 1 else np.nan
    result[f"{prefix}_center"] = result[f"{prefix}__0_0"]

    # Inner 3×3 stats
    n_inner = len(inner_valid)
    result[f"{prefix}_inner_count"] = n_inner
    result[f"{prefix}_inner_mean"]  = float(np.mean(inner_valid)) if n_inner > 0 else np.nan

    # Outer ring stats (the 16 pixels NOT in the inner 3×3)
    n_outer = len(outer_valid)
    result[f"{prefix}_outer_count"] = n_outer
    result[f"{prefix}_outer_mean"]  = float(np.mean(outer_valid)) if n_outer > 0 else np.nan

    return result


def extract_exact(band_data: np.ndarray, row_idx: int, col_idx: int,
                  name: str) -> dict:
    """Extract single pixel value. Returns {name: float_or_NaN}."""
    h, w = band_data.shape
    if 0 <= row_idx < h and 0 <= col_idx < w:
        val = float(band_data[row_idx, col_idx])
        if np.isnan(val) or val in FILL_VALUES:
            val = np.nan
    else:
        val = np.nan
    return {name: val}


# ══════════════════════════════════════════════════════════════════════
#  Main extraction loop
# ══════════════════════════════════════════════════════════════════════

def run(
    years: list[int],
    aod_dir: str,
    output_dir: str,
    level: str,
    grid_bands: dict,
    exact_bands: dict,
    glob_template: str,
) -> dict:
    """Iterate GeoTIFF files for the given level, extract AOD 5×5 + metadata for all stations."""
    stations_df = load_stations(STATION_CSV)
    os.makedirs(output_dir, exist_ok=True)

    stations = []
    for _, row in stations_df.iterrows():
        name = str(row['stationName'])
        stations.append({
            'id':   str(row['stationId']),
            'name': name,
            'lon':  row['longitude'],
            'lat':  row['latitude'],
            'csv':  os.path.join(output_dir, f"{name}.csv"),
        })

    ts_cache = {s['csv']: get_existing_timestamps(s['csv']) for s in stations}
    print(f"[{level}] {len(stations)} stations loaded, timestamp caches ready.")

    all_indices = sorted(set(list(grid_bands.values()) + list(exact_bands.values())))
    ts_parser = parse_timestamp_utc if level == "L2" else parse_timestamp_utc_l3

    stats = {'total': 0, 'skipped': 0, 'processed': 0, 'error': 0, 'rows': 0}

    for year in years:
        pattern = os.path.join(aod_dir, glob_template.format(year=year))
        files = sorted(glob.glob(pattern))
        n_files = len(files)
        print(f"[{level}] {year}: {n_files} files")
        wlog(f"YEAR={year}  files={n_files}")
        stats['total'] += n_files

        for fpath in tqdm(files, desc=f"{level}-{year}", unit="f"):
            try:
                fname = os.path.basename(fpath)
                timestamp = utc_to_gmt7(ts_parser(fname))

                if all(timestamp in ts_cache[s['csv']] for s in stations):
                    stats['skipped'] += 1
                    continue

                with rasterio.open(fpath) as src:
                    bands = {}
                    for idx in all_indices:
                        try:
                            bands[idx] = src.read(idx).astype(np.float32)
                        except Exception:
                            pass

                    if not bands:
                        stats['skipped'] += 1
                        continue

                    buffered = {}

                    for s in stations:
                        if timestamp in ts_cache[s['csv']]:
                            continue

                        try:
                            ri, ci = src.index(s['lon'], s['lat'])
                        except Exception:
                            continue

                        row = {'timestamp': timestamp, 'stationId': s['id']}

                        for bname, bidx in grid_bands.items():
                            if bidx in bands:
                                row.update(extract_5x5(bands[bidx], ri, ci, bname))

                        for bname, bidx in exact_bands.items():
                            if bidx in bands:
                                row.update(extract_exact(bands[bidx], ri, ci, bname))

                        if not any(row.get(f"{bn}_valid_count", 0) > 0 for bn in grid_bands):
                            continue

                        buffered.setdefault(s['csv'], []).append(row)

                    for csv_path, rows in buffered.items():
                        append_rows(csv_path, rows)
                        for r in rows:
                            ts_cache[csv_path].add(r['timestamp'])
                        stats['rows'] += len(rows)

                stats['processed'] += 1

            except Exception as e:
                stats['error'] += 1
                tqdm.write(f"[!] {os.path.basename(fpath)}: {e}")
                wlog(f"ERROR  {os.path.basename(fpath)}  {e}")

        wlog(f"YEAR={year} DONE  proc={stats['processed']}  rows={stats['rows']}")
        print(f"  {year} done. Running total: {stats}")

    wlog(f"FINAL  total={stats['total']}  proc={stats['processed']}  "
         f"skip={stats['skipped']}  err={stats['error']}  rows={stats['rows']}")
    print(f"\n{'='*60}")
    print(f"DONE: {stats}")
    print(f"{'='*60}")
    return stats


# ══════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════

def main():
    global STATION_CSV

    parser = argparse.ArgumentParser(
        description="Extract Himawari L2/L3 AOD (5×5 grid) for all stations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract_stations_aod_spatial.py --level L2 --years 2024
  python extract_stations_aod_spatial.py --level L2 --years 2022 2023 2024 2025 2026
  python extract_stations_aod_spatial.py --level L3 --years 2024
  python extract_stations_aod_spatial.py --level L3 --years 2022 2023 2024 2025 2026

L2 output columns per station:
  timestamp, stationId
  AOT 5×5 (AOT_m2m2..AOT_p2p2, valid_count, mean, std, center, inner/outer stats)
  Exact pixel: Uncertainty, AE, QA_flag, SSA, RF

L3 output columns per station:
  timestamp, stationId
  AOT_Merged 5×5, AOT_Pure 5×5 (same stats as L2 AOT)
  Exact pixel: AOT_Merged_uncertainty, AOT_Pure_uncertainty,
               AE_Merged, AE_Pure, QA_flag_Merged, QA_flag_Pure,
               AOT_L2_Mean, AOT_L2_SDV, AOT_L2_Num,
               AE_L2_Mean, AE_L2_SDV, AE_L2_Num

Incremental: re-running skips already-processed timestamps.
        """,
    )
    parser.add_argument("--level", required=True, choices=["L2", "L3"],
                        help="Data level: L2 (10-min) or L3 (hourly composite)")
    parser.add_argument("--years", required=True, nargs="+", type=int)
    parser.add_argument("--station-csv", default=None,
                        help=f"Station CSV (default: {STATION_CSV})")
    parser.add_argument("--output-dir", default=None,
                        help="Output directory (overrides level default)")
    parser.add_argument("--aod-dir", default=None,
                        help="AOD input directory (overrides level default)")
    args = parser.parse_args()

    if args.station_csv:
        STATION_CSV = args.station_csv

    level = args.level
    if level == "L2":
        default_aod = L2_AOD_DIR
        default_out = L2_OUTPUT_DIR
        grid_bands  = L2_GRID_BANDS
        exact_bands = L2_EXACT_BANDS
        glob_tmpl   = L2_GLOB_TEMPLATE
        cadence     = "10-min"
    else:
        default_aod = L3_AOD_DIR
        default_out = L3_OUTPUT_DIR
        grid_bands  = L3_GRID_BANDS
        exact_bands = L3_EXACT_BANDS
        glob_tmpl   = L3_GLOB_TEMPLATE
        cadence     = "hourly"

    aod  = args.aod_dir    or default_aod
    out  = args.output_dir or default_out
    years = sorted(args.years)

    print("=" * 60)
    print(f"  Himawari {level} | 5×5 AOT (~25km) | {len(years)} year(s) | {cadence}")
    print(f"  Stations:  {STATION_CSV}")
    print(f"  Input:     {aod}")
    print(f"  Output:    {out}")
    print("=" * 60)

    run(
        years=years,
        aod_dir=aod,
        output_dir=out,
        level=level,
        grid_bands=grid_bands,
        exact_bands=exact_bands,
        glob_template=glob_tmpl,
    )


if __name__ == "__main__":
    # python extract_stations_aod_spatial.py --level L2 --years 2022 2023 2024 2025 2026
    # python extract_stations_aod_spatial.py --level L3 --years 2022 2023 2024 2025 2026
    main()