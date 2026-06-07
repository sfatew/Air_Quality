"""
Extract Himawari L2/L3 AOD for all target stations, with QA-based spatial filtering.

Key change vs v3:
- QA_flag is now extracted as a 5×5 grid (25 raw values + pass_count + center).
- AOT pixels are QUALITY-MASKED before stats:
    Pixel kept only if its QA_flag satisfies (JAXA strict screening):
      bit 0  (data_avail)         = 0
      bit 2  (cloud)              = 0
      bit 3  (retrieval status)   = 0
      bit 4-5 (AOT confidence)    = 00  (very good)
      bit 8  (additional cloud)   = 0
      bit 10 (Solz/Satz > 70°)    = 0
      bit 11 (surf refl conf)     = 0
      bit 12 (snow/ice)           = 0   (L3-only flag; 0 on L2 since TBD)
      bit 13 (turbid water)       = 0   (L3-only flag; 0 on L2 since TBD)
  AOT pixels failing QA OR missing → set to NaN in raw 25 cols AND excluded
  from mean / std / valid_count / inner / outer aggregates.

- L3 uses the same logic on both pairs:
    (AOT_Merged ↔ QA_flag_Merged)
    (AOT_Pure   ↔ QA_flag_Pure)

- All timestamps converted UTC → GMT+7.
- Incremental: re-running skips already-processed timestamps.

Usage:
    python extract_stations_aod_qa.py --level L2 --years 2024
    python extract_stations_aod_qa.py --level L2 --years 2022 2023 2024 2025 2026
    python extract_stations_aod_qa.py --level L3 --years 2022 2023 2024 2025 2026
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
#  Configuration
# ══════════════════════════════════════════════════════════════════════

STATION_CSV   = "/home/work1/projects/Air_Quality/Masterdata/AERONET_sites.csv"
STATION_FILTER = ["Bac_Lieu", "NGHIA_DO"]
LOG_DIR       = "/home/slow_data/Air_Quality/Himawari/logs"

# ── L2 ────────────────────────────────────────────────────────────────
L2_AOD_DIR    = "/home/slow_data/Air_Quality/Himawari/L2_AOD"
L2_OUTPUT_DIR = "/home/slow_data/Air_Quality/Himawari/sites_aod_v4/L2"
L2_GLOB_TEMPLATE = r'{year}??/??/??/aod_vietnam_NC_H??_*_L2ARP031_FLDK.*.tif'

# (aot_name, aot_band_idx, qa_name, qa_band_idx) — AOT filtered by paired QA
L2_QA_PAIRS = [
    ('AOT', 1, 'QA_flag', 4),
]
L2_EXACT_BANDS = {'Uncertainty': 2, 'AE': 3, 'SSA': 5, 'RF': 6}

# ── L3 ────────────────────────────────────────────────────────────────
L3_AOD_DIR    = "/home/slow_data/Air_Quality/Himawari/L3_AOD"
L3_OUTPUT_DIR = "/home/slow_data/Air_Quality/Himawari/sites_aod_v4/L3"
L3_GLOB_TEMPLATE = r'{year}??/*/aod_vietnam_H??_*_1HARP031_FLDK.*.tif'

L3_QA_PAIRS = [
    ('AOT_Merged', 2, 'QA_flag_Merged', 8),
    ('AOT_Pure',   3, 'QA_flag_Pure',   9),
]
L3_EXACT_BANDS = {
    'AOT_Merged_uncertainty':  4,
    'AOT_Pure_uncertainty':    5,
    'AE_Merged':               6,
    'AE_Pure':                 7,
    'AOT_L2_Mean':            10,
    'AOT_L2_SDV':             11,
    'AOT_L2_Num':             12,
    'AE_L2_Mean':             13,
    'AE_L2_SDV':              14,
    'AE_L2_Num':              15,
}

FILL_VALUES = {-9999.0, -999.0, 9999.0}
AOT_MIN_VALID = 0.001 
AOT_MAX_VALID = 5.0  

# 5×5 layout: (row_offset, col_offset, column_suffix)
GRID_OFFSETS = [
    (-2, -2, "m2m2"), (-2, -1, "m2m1"), (-2,  0, "m2_0"), (-2,  1, "m2p1"), (-2,  2, "m2p2"),
    (-1, -2, "m1m2"), (-1, -1, "m1m1"), (-1,  0, "m1_0"), (-1,  1, "m1p1"), (-1,  2, "m1p2"),
    ( 0, -2, "_0m2"), ( 0, -1, "_0m1"), ( 0,  0, "_0_0"), ( 0,  1, "_0p1"), ( 0,  2, "_0p2"),
    ( 1, -2, "p1m2"), ( 1, -1, "p1m1"), ( 1,  0, "p1_0"), ( 1,  1, "p1p1"), ( 1,  2, "p1p2"),
    ( 2, -2, "p2m2"), ( 2, -1, "p2m1"), ( 2,  0, "p2_0"), ( 2,  1, "p2p1"), ( 2,  2, "p2p2"),
]

INNER_LABELS = {"m1m1", "m1_0", "m1p1", "_0m1", "_0_0", "_0p1", "p1m1", "p1_0", "p1p1"}

_log_tag = "extract_aod_qa_v4"


# ══════════════════════════════════════════════════════════════════════
#  QA decoding
# ══════════════════════════════════════════════════════════════════════

def qa_passes_array(qa: np.ndarray) -> np.ndarray:
    """
    Vectorized strict JAXA quality check (returns 2D bool array).

    Pixel passes iff ALL of:
      bit 0  data_avail     = 0
      bit 2  cloud          = 0
      bit 3  retrieval ok   = 0
      bit 4-5 AOT conf      = 00 (very good)
      bit 8  add_cloud      = 0
      bit 10 high_zenith    = 0
      bit 11 surf_refl bad  = 0
      bit 12 snow_ice       = 0  (L3 only; 0 on L2 by design)
      bit 13 turbid_water   = 0  (L3 only; 0 on L2 by design)
    """
    qa_i = qa.astype(np.int32)
    return (
        (((qa_i >> 0) & 1) == 0)
        & (((qa_i >> 2) & 1) == 0)
        & (((qa_i >> 3) & 1) == 0)
        & (((qa_i >> 4) & 0b11) == 0)
        & (((qa_i >> 8) & 1) == 0)
        & (((qa_i >> 10) & 1) == 0)
        & (((qa_i >> 11) & 1) == 0)
        & (((qa_i >> 12) & 1) == 0)
        & (((qa_i >> 13) & 1) == 0)
    )


# ══════════════════════════════════════════════════════════════════════
#  Helpers (unchanged from v3)
# ══════════════════════════════════════════════════════════════════════

def wlog(msg: str) -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    now = datetime.now()
    path = os.path.join(LOG_DIR, now.strftime("%Y-%m-%d") + ".log")
    try:
        with open(path, "a") as f:
            f.write(f"[{now:%Y-%m-%d %H:%M:%S}] [{_log_tag}] {msg}\n")
    except Exception:
        pass


def parse_timestamp_utc(filename: str) -> str:
    parts = filename.split("_")
    return parts[4] + "_" + parts[5]


def parse_timestamp_utc_l3(filename: str) -> str:
    parts = filename.split("_")
    return parts[3] + "_" + parts[4]


def utc_to_gmt7(raw_ts: str) -> str:
    dt = pd.to_datetime(raw_ts, format='%Y%m%d_%H%M') + pd.Timedelta(hours=7)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def load_stations(csv_path: str) -> pd.DataFrame:
    """Load station CSV. Handles both naming conventions, applies STATION_FILTER."""
    df = pd.read_csv(csv_path)
    renames = {}
    if 'station_name' in df.columns and 'stationName' not in df.columns:
        renames['station_name'] = 'stationName'
    if 'station_id' in df.columns and 'stationId' not in df.columns:
        renames['station_id'] = 'stationId'
    if renames:
        df = df.rename(columns=renames)
    df['stationId'] = df['stationId'].astype(str)

    if STATION_FILTER:
        kw = [k.lower() for k in STATION_FILTER]
        names_lc = df['stationName'].astype(str).str.lower()
        mask = names_lc.apply(lambda n: any(k in n for k in kw))
        df = df[mask].reset_index(drop=True)
        print(f"STATION_FILTER active: {STATION_FILTER} → {len(df)} stations kept")

    return df


def get_existing_timestamps(csv_path: str) -> set:
    if not os.path.isfile(csv_path):
        return set()
    try:
        return set(pd.read_csv(csv_path, usecols=['timestamp'])['timestamp'].astype(str))
    except Exception:
        return set()


def append_rows(csv_path: str, rows: list) -> None:
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
                prefix: str, quality_mask: np.ndarray = None) -> dict:
    """
    Extract 5×5 grid centered on (row_idx, col_idx).

    If quality_mask (2D bool) provided, any pixel with mask=False is set to NaN
    in the raw 25 columns AND excluded from mean / std / valid_count aggregates.

    Output columns (prefix='AOT' example):
      Raw 25: AOT_m2m2 .. AOT_p2p2  (NaN if QA fail or out-of-bounds or fill)
      Overall:  AOT_valid_count, AOT_mean, AOT_std, AOT_center
      Inner 3×3: AOT_inner_count, AOT_inner_mean
      Outer ring: AOT_outer_count, AOT_outer_mean
    """
    h, w = band_data.shape
    result = {}
    all_valid, inner_valid, outer_valid = [], [], []

    for dr, dc, label in GRID_OFFSETS:
        r, c = row_idx + dr, col_idx + dc
        val = np.nan
        if 0 <= r < h and 0 <= c < w:
            raw = float(band_data[r, c])
            if not (np.isnan(raw) or raw in FILL_VALUES):
                if AOT_MIN_VALID <= raw <= AOT_MAX_VALID:
                    if quality_mask is None or bool(quality_mask[r, c]):
                        val = raw

        result[f"{prefix}_{label}"] = val
        if not np.isnan(val):
            all_valid.append(val)
            if label in INNER_LABELS:
                inner_valid.append(val)
            else:
                outer_valid.append(val)

    n_all = len(all_valid)
    result[f"{prefix}_valid_count"] = n_all
    result[f"{prefix}_mean"]   = float(np.mean(all_valid)) if n_all > 0 else np.nan
    result[f"{prefix}_std"]    = float(np.std(all_valid))  if n_all > 1 else np.nan
    result[f"{prefix}_center"] = result[f"{prefix}__0_0"]

    n_inner = len(inner_valid)
    result[f"{prefix}_inner_count"] = n_inner
    result[f"{prefix}_inner_mean"]  = float(np.mean(inner_valid)) if n_inner > 0 else np.nan

    n_outer = len(outer_valid)
    result[f"{prefix}_outer_count"] = n_outer
    result[f"{prefix}_outer_mean"]  = float(np.mean(outer_valid)) if n_outer > 0 else np.nan

    return result


def extract_5x5_qa(qa_data: np.ndarray, pass_mask_2d: np.ndarray,
                   row_idx: int, col_idx: int, prefix: str) -> dict:
    """
    Extract 5×5 raw QA_flag values + pass-count statistic.

    Output columns (prefix='QA_flag' example):
      Raw 25: QA_flag_m2m2 .. QA_flag_p2p2  (integer bitmask, NaN if out-of-bounds)
      QA_flag_pass_count   number of pixels passing strict QA (0–25)
      QA_flag_center       alias for _0_0
    """
    h, w = qa_data.shape
    result = {}
    n_pass = 0

    for dr, dc, label in GRID_OFFSETS:
        r, c = row_idx + dr, col_idx + dc
        if 0 <= r < h and 0 <= c < w:
            result[f"{prefix}_{label}"] = int(qa_data[r, c])
            if pass_mask_2d[r, c]:
                n_pass += 1
        else:
            result[f"{prefix}_{label}"] = np.nan

    result[f"{prefix}_pass_count"] = n_pass
    result[f"{prefix}_center"]     = result[f"{prefix}__0_0"]
    return result


def extract_exact(band_data: np.ndarray, row_idx: int, col_idx: int,
                  name: str) -> dict:
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
    years: list,
    aod_dir: str,
    output_dir: str,
    level: str,
    qa_pairs: list,
    exact_bands: dict,
    glob_template: str,
) -> dict:
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
    print(f"[{level}] {len(stations)} stations loaded.")

    # Indices needed: every AOT band, every QA band, every exact band
    all_indices = sorted(set(
        [p[1] for p in qa_pairs] +
        [p[3] for p in qa_pairs] +
        list(exact_bands.values())
    ))
    ts_parser = parse_timestamp_utc if level == "L2" else parse_timestamp_utc_l3
    aot_names = [p[0] for p in qa_pairs]

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

                    # Pre-compute quality mask per QA band (shared across stations)
                    qa_masks = {}
                    for _, _, qa_name, qa_idx in qa_pairs:
                        if qa_idx in bands:
                            qa_masks[qa_idx] = qa_passes_array(bands[qa_idx])

                    buffered = {}
                    for s in stations:
                        if timestamp in ts_cache[s['csv']]:
                            continue
                        try:
                            ri, ci = src.index(s['lon'], s['lat'])
                        except Exception:
                            continue

                        row = {'timestamp': timestamp, 'stationId': s['id']}

                        # For each (AOT, QA) pair: masked AOT 5×5 + raw QA 5×5
                        for aot_name, aot_idx, qa_name, qa_idx in qa_pairs:
                            mask = qa_masks.get(qa_idx)
                            if aot_idx in bands:
                                row.update(extract_5x5(
                                    bands[aot_idx], ri, ci, aot_name,
                                    quality_mask=mask,
                                ))
                            if qa_idx in bands:
                                row.update(extract_5x5_qa(
                                    bands[qa_idx], mask, ri, ci, qa_name,
                                ))

                        # Other bands → exact pixel only
                        for bname, bidx in exact_bands.items():
                            if bidx in bands:
                                row.update(extract_exact(bands[bidx], ri, ci, bname))

                        # Skip row if no AOT band has any quality-passing pixel
                        if not any(row.get(f"{n}_valid_count", 0) > 0 for n in aot_names):
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
    print(f"\n{'='*60}\nDONE: {stats}\n{'='*60}")
    return stats


# ══════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════

def main():
    global STATION_CSV

    parser = argparse.ArgumentParser(
        description="Extract Himawari L2/L3 AOD (5×5 grid, QA-masked) for all stations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--level", required=True, choices=["L2", "L3"])
    parser.add_argument("--years", required=True, nargs="+", type=int)
    parser.add_argument("--station-csv", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--aod-dir", default=None)
    args = parser.parse_args()

    if args.station_csv:
        STATION_CSV = args.station_csv

    level = args.level
    if level == "L2":
        default_aod = L2_AOD_DIR
        default_out = L2_OUTPUT_DIR
        qa_pairs    = L2_QA_PAIRS
        exact_bands = L2_EXACT_BANDS
        glob_tmpl   = L2_GLOB_TEMPLATE
        cadence     = "10-min"
    else:
        default_aod = L3_AOD_DIR
        default_out = L3_OUTPUT_DIR
        qa_pairs    = L3_QA_PAIRS
        exact_bands = L3_EXACT_BANDS
        glob_tmpl   = L3_GLOB_TEMPLATE
        cadence     = "hourly"

    aod  = args.aod_dir    or default_aod
    out  = args.output_dir or default_out
    years = sorted(args.years)

    print("=" * 60)
    print(f"  Himawari {level} | QA-masked 5×5 AOT (~25km) | {len(years)} year(s) | {cadence}")
    print(f"  Stations:  {STATION_CSV}")
    print(f"  Input:     {aod}")
    print(f"  Output:    {out}")
    print("=" * 60)

    run(
        years=years,
        aod_dir=aod,
        output_dir=out,
        level=level,
        qa_pairs=qa_pairs,
        exact_bands=exact_bands,
        glob_template=glob_tmpl,
    )


if __name__ == "__main__":
    main()