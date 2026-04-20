import sys
import os
import re
import glob
import zipfile
import pandas as pd
import numpy as np
from io import BytesIO
from PIL import Image
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STATIONS_FILE = "/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv"
OUTPUT_DIR    = "/home/slow_data/Air_Quality/GIS/station_gis_extracted"

# ---------------------------------------------------------------------------
# Run-type TIF field definitions
#
# Each entry: (tif_suffix, output_column_name, scale_factor, missing_raw_value)
#
# FINAL run  — 9 fields, suffixes follow the "total.accum.tif" / "liquid.rate.tif" pattern
# LATE  run  — 6 fields, suffixes follow the "30min.tif" / "30min.ice.tif" pattern
#              Fields absent in Late (total_rate, liquid_rate, ice_rate) will be stored as NaN.
# ---------------------------------------------------------------------------

FINAL_TIF_FIELDS = [
    ("total.accum.tif",      "total_accum_mm",      10.0,  29999),
    ("total.rate.tif",       "total_rate_mmh",      10.0,  29999),
    ("liquid.accum.tif",     "liquid_accum_mm",     10.0,  29999),
    ("liquid.rate.tif",      "liquid_rate_mmh",     10.0,  29999),
    ("liquidPercent.tif",    "liquid_pct",           1.0,    255),   # 1-byte, 0-100; 255 = missing
    ("ice.accum.tif",        "ice_accum_mm",        10.0,  29999),
    ("ice.rate.tif",         "ice_rate_mmh",        10.0,  29999),
    ("numValidHalfHour.tif", "num_valid_halfhour",   1.0,   None),   # count, no missing sentinel
    ("numPrecipHalfHour.tif","num_precip_halfhour",  1.0,   None),   # count, no missing sentinel
]

# NOTE: "30min.tif" intentionally ends differently from "30min.ice.tif",
#       "30min.liquid.tif", etc., so endswith() matching stays unambiguous.
LATE_TIF_FIELDS = [
    ("30min.tif",                  "total_accum_mm",      10.0,  29999),
    # total_rate_mmh  → not published in Late run → NaN
    ("30min.liquid.tif",           "liquid_accum_mm",     10.0,  29999),
    # liquid_rate_mmh → not published in Late run → NaN
    ("30min.liquidPercent.tif",    "liquid_pct",           1.0,    255),
    ("30min.ice.tif",              "ice_accum_mm",        10.0,  29999),
    # ice_rate_mmh    → not published in Late run → NaN
    ("30min.numValidHalfHour.tif", "num_valid_halfhour",   1.0,   None),
    ("30min.numPrecipHalfHour.tif","num_precip_halfhour",  1.0,   None),
]

# Master list of every possible output column (union of both run types).
# Columns absent for a given run type will contain NaN.
TARGET_COLUMNS = [
    "total_accum_mm",
    "total_rate_mmh",      # Final only
    "liquid_accum_mm",
    "liquid_rate_mmh",     # Final only
    "liquid_pct",
    "ice_accum_mm",
    "ice_rate_mmh",        # Final only
    "num_valid_halfhour",
    "num_precip_halfhour",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize_filename(name):
    """Removes invalid characters to create safe filenames."""
    return str(name).replace(":", "").replace(" ", "_").replace("/", "_").replace('"', '')


def detect_run_type(filename):
    """
    Returns 'Late' if the filename starts with the near-realtime prefix (3B-HHR-L),
    otherwise 'Final'.

    Examples
    --------
    3B-HHR-L.MS.MRG.3IMERG.20251001-S000000-E002959.0000.V07B.30min.zip  → 'Late'
    3B-HHR-GIS.MS.MRG.3IMERG.20250406-S160000-E162959.0960.V07B.zip       → 'Final'
    """
    basename = os.path.basename(filename)
    return "Late" if basename.startswith("3B-HHR-L") else "Final"


def parse_timestamp_from_filename(filename):
    """
    Parse start timestamp from either Final or Late IMERG GIS zip filename.

    Both share the same date/time encoding:
      …3IMERG.<YYYYMMDD>-S<HHMMSS>-…

    Converts from UTC to UTC+7 and returns 'YYYY-MM-DD HH:MM:SS', or None on failure.
    """
    m = re.search(r"(\d{8})-S(\d{6})", filename)
    if not m:
        return None

    dt_utc   = datetime.strptime(f"{m.group(1)}{m.group(2)}", "%Y%m%d%H%M%S")
    dt_local = dt_utc + timedelta(hours=7)
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")


def latlon_to_index(lat, lon):
    """
    Convert lat/lon to IMERG GIS pixel indices (row, col).

    Grid: 1800 rows × 3600 cols, 0.1° resolution, north-up.
    """
    row = int((90.0  - lat) * 10.0 + 1e-8)
    col = int((lon + 180.0) * 10.0 + 1e-8)
    row = max(0, min(row, 1799))
    col = max(0, min(col, 3599))
    return row, col


def read_tif_from_zip(zf, suffix):
    """
    Find and read a .tif by suffix from an open ZipFile into a numpy array.
    Returns None if the file is not found inside the archive.
    """
    for name in zf.namelist():
        if name.endswith(suffix):
            return np.array(Image.open(BytesIO(zf.read(name))))
    return None

# ---------------------------------------------------------------------------
# Core per-file processing (runs in worker processes)
# ---------------------------------------------------------------------------

def process_single_file(zip_path, station_coords):
    """
    Opens one GIS .zip (Final or Late run), reads all available TIF fields,
    then extracts one pixel per station using pre-computed (row, col) indices.

    Returns a dict  { safe_id: record_dict }  or None on failure.
    Each record_dict always contains all TARGET_COLUMNS; fields absent in the
    Late run are stored as NaN.
    """
    try:
        filename  = os.path.basename(zip_path)
        timestamp = parse_timestamp_from_filename(filename)
        if timestamp is None:
            print(f"Could not parse timestamp from: {filename}")
            return None

        run_type  = detect_run_type(filename)
        tif_fields = FINAL_TIF_FIELDS if run_type == "Final" else LATE_TIF_FIELDS

        # Read all available arrays from the zip
        with zipfile.ZipFile(zip_path, "r") as zf:
            arrays = {}
            for suffix, col_name, scale, missing in tif_fields:
                arrays[col_name] = (read_tif_from_zip(zf, suffix), scale, missing)

        # Extract per-station pixel values
        file_results = {}
        for safe_id, r, c in station_coords:
            station_record = {"timestamp": timestamp, "run_type": run_type}

            # Populate every master column; default to NaN
            for col_name in TARGET_COLUMNS:
                if col_name not in arrays:
                    # Field not published by this run type
                    station_record[col_name] = np.nan
                    continue

                arr, scale, missing = arrays[col_name]

                if arr is None:
                    station_record[col_name] = np.nan
                    continue

                if arr.ndim != 2 or r >= arr.shape[0] or c >= arr.shape[1]:
                    station_record[col_name] = np.nan
                    continue

                pixel_val = float(arr[r, c])

                if np.isnan(pixel_val) or (missing is not None and pixel_val == missing):
                    station_record[col_name] = np.nan
                else:
                    station_record[col_name] = round(pixel_val / scale, 3)

            file_results[safe_id] = station_record

        return file_results

    except zipfile.BadZipFile:
        print(f"Bad zip file, skipping: {os.path.basename(zip_path)}")
        return None
    except Exception as e:
        print(f"Error processing {os.path.basename(zip_path)}: {e}")
        return None

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(target_directory):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Warn if stale CSVs exist from a previous run
    existing = glob.glob(os.path.join(OUTPUT_DIR, "*.csv"))
    if existing:
        print(
            f"WARNING: {len(existing)} existing CSV(s) found in {OUTPUT_DIR}.\n"
            "If they were produced before the latlon_to_index row-formula fix,\n"
            "before the UTC+7 conversion, or before Late-run support was added,\n"
            "delete them first so they are fully regenerated:\n"
            f"  rm {OUTPUT_DIR}/*.csv\n"
        )

    stations_df = pd.read_csv(STATIONS_FILE)

    # Resolve ID column
    id_col = 'stationId' if 'stationId' in stations_df.columns else 'ID'
    if id_col not in stations_df.columns:
        raise ValueError(
            f"Could not find a valid ID column. "
            f"Available columns: {stations_df.columns.tolist()}"
        )
    if 'stationName' not in stations_df.columns:
        raise ValueError(
            f"Could not find 'stationName' column. "
            f"Available columns: {stations_df.columns.tolist()}"
        )

    stations_df["safe_id"]   = stations_df[id_col].apply(sanitize_filename)
    stations_df["safe_name"] = stations_df['stationName'].apply(sanitize_filename)
    id_to_name_map           = dict(zip(stations_df["safe_id"], stations_df["safe_name"]))
    unique_stations          = stations_df["safe_id"].unique()

    # Resolve lat/lon columns (case-insensitive)
    lat_col = next(
        (c for c in stations_df.columns if c.strip().lower() in ['latitude', 'lat']), None
    )
    lon_col = next(
        (c for c in stations_df.columns if c.strip().lower() in ['longitude', 'lon', 'long']), None
    )
    if not lat_col or not lon_col:
        raise ValueError(
            f"Could not find valid latitude/longitude columns. "
            f"Available columns: {stations_df.columns.tolist()}"
        )

    # Pre-compute pixel indices once to avoid repeated work in worker processes
    station_coords = []
    for _, row in stations_df.iterrows():
        r, c = latlon_to_index(row[lat_col], row[lon_col])
        station_coords.append((row["safe_id"], r, c))

    search_path = os.path.join(target_directory, "**", "*.zip")
    zip_files   = sorted(glob.glob(search_path, recursive=True))

    if not zip_files:
        print(f"No .zip files found in {target_directory}.")
        return

    # Report breakdown by run type before starting
    final_count = sum(1 for f in zip_files if detect_run_type(f) == "Final")
    late_count  = len(zip_files) - final_count
    print(
        f"Found {len(zip_files)} zip files "
        f"({final_count} Final, {late_count} Late/NRT). "
        "Starting parallel extraction with 8 workers..."
    )

    station_data    = {station: [] for station in unique_stations}
    processed_count = 0

    with ProcessPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(process_single_file, f, station_coords): f
            for f in zip_files
        }

        for future in as_completed(futures):
            result = future.result()
            if result:
                for station_id, record in result.items():
                    if station_id in station_data:
                        station_data[station_id].append(record)

            processed_count += 1
            if processed_count % 500 == 0:
                print(f"Processed {processed_count}/{len(zip_files)} files...")

    print("Extraction complete. Saving individual station CSV files...")

    # Output column order: timestamp, run_type, then all data columns
    cols = ["timestamp", "run_type"] + TARGET_COLUMNS

    for station_id, records in station_data.items():
        if not records:
            continue

        safe_station_name = id_to_name_map.get(station_id, station_id)
        new_df     = pd.DataFrame(records)
        output_csv = os.path.join(OUTPUT_DIR, f"{safe_station_name}.csv")

        # Append to existing CSV if present, then dedup on timestamp
        if os.path.exists(output_csv):
            df_old   = pd.read_csv(output_csv)
            df_final = pd.concat([df_old, new_df], ignore_index=True)
            # When the same timestamp appears as both Late and Final,
            # keep the Final record (higher quality) by sorting run_type
            # so "Final" sorts before "Late", then dedup keeps first.
            df_final["_sort_run"] = df_final["run_type"].map({"Final": 0, "Late": 1}).fillna(1)
            df_final = (
                df_final
                .sort_values(["timestamp", "_sort_run"])
                .drop_duplicates(subset=["timestamp"], keep="first")
                .drop(columns=["_sort_run"])
            )
        else:
            df_final = new_df

        # Ensure all master columns exist (handles old CSVs missing run_type)
        for col in cols:
            if col not in df_final.columns:
                df_final[col] = np.nan

        df_final = df_final[cols]
        df_final = df_final.sort_values("timestamp").reset_index(drop=True)

        df_final.to_csv(output_csv, index=False)
        print(
            f"Saved {len(df_final)} rows for ID {station_id} "
            f"-> {os.path.basename(output_csv)}"
        )


if __name__ == "__main__":
    target_dir = '/home/slow_data/Air_Quality/GIS'
    main(target_dir)
