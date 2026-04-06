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

STATIONS_FILE = "/home/slow_data/Air_Quality/Envisoft_station_metadata.csv"
OUTPUT_DIR    = "/home/slow_data/Air_Quality/GIS/station_gis_extracted"

# All 9 fields from each ZIP:
# (tif_suffix, output_column_name, scale_factor, missing_raw_value)
TIF_FIELDS = [
    ("total.accum.tif",         "total_accum_mm",      10.0,  29999),
    ("total.rate.tif",          "total_rate_mmh",      10.0,  29999),
    ("liquid.accum.tif",        "liquid_accum_mm",     10.0,  29999),
    ("liquid.rate.tif",         "liquid_rate_mmh",     10.0,  29999),
    ("liquidPercent.tif",       "liquid_pct",           1.0,    255),  # 1-byte, 0-100; 255 = missing  (absent in some months → NaN)
    ("ice.accum.tif",           "ice_accum_mm",        10.0,  29999),
    ("ice.rate.tif",            "ice_rate_mmh",        10.0,  29999),
    ("numValidHalfHour.tif",    "num_valid_halfhour",   1.0,   None),  # count, no missing sentinel
    ("numPrecipHalfHour.tif",   "num_precip_halfhour",  1.0,   None),  # count, no missing sentinel
]

TARGET_COLUMNS = [col for _, col, _, _ in TIF_FIELDS]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize_filename(name):
    """Removes invalid characters to create safe filenames."""
    return str(name).replace(":", "").replace(" ", "_").replace("/", "_").replace('"', '')


def parse_timestamp_from_filename(filename):
    """
    Parse start timestamp from Final IMERG GIS zip filename.
    Example: 3B-HHR-GIS.MS.MRG.3IMERG.20250406-S160000-E162959.0960.V07B.zip
    Converts from UTC to UTC+7.
    Returns 'YYYY-MM-DD HH:MM:SS' string or None on failure.
    """
    m = re.search(r"(\d{8})-S(\d{6})", filename)
    if not m:
        return None
    
    date_str, time_str = m.group(1), m.group(2)
    
    # Parse as UTC datetime
    dt_utc = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
    
    # Convert to UTC+7
    dt_local = dt_utc + timedelta(hours=7)
    
    # Return formatted string
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")


def latlon_to_index(lat, lon):
    """
    Convert lat/lon to IMERG GIS pixel indices (row, col).

    Grid: 1800 rows × 3600 cols, 0.1° resolution, **north-up**.

    """
    row = int((90.0 - lat) * 10.0 + 1e-8)
    col = int((lon + 180.0) * 10.0 + 1e-8) 
    
    # Clamp to valid bounds
    row = max(0, min(row, 1799))
    col = max(0, min(col, 3599))

    return row, col


def read_tif_from_zip(zf, suffix):
    """
    Find and read a .tif by suffix from an open ZipFile into a numpy array.
    Returns None if the file is not found.
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
    Opens one GIS .zip, reads all 9 TIF fields into numpy arrays,
    then extracts one pixel per station using precomputed row/col indices.
    Returns a dict keyed by station safe_id.
    """
    try:
        filename  = os.path.basename(zip_path)
        timestamp = parse_timestamp_from_filename(filename)
        if timestamp is None:
            print(f"Could not parse timestamp from: {filename}")
            return None

        # Read all arrays from zip at once
        with zipfile.ZipFile(zip_path, "r") as zf:
            arrays = {}
            for suffix, col_name, scale, missing in TIF_FIELDS:
                arrays[col_name] = (read_tif_from_zip(zf, suffix), scale, missing)

        # Extract per-station values using pre-computed indices for maximum efficiency
        file_results = {}
        for safe_id, r, c in station_coords:
            station_record = {"timestamp": timestamp}

            for col_name, (arr, scale, missing) in arrays.items():
                if arr is None:
                    station_record[col_name] = np.nan
                    continue

                if arr.ndim != 2 or r >= arr.shape[0] or c >= arr.shape[1]:
                    station_record[col_name] = np.nan
                    continue

                # Cast to float to handle potential native np.nan safely without throwing ValueError
                pixel_val = float(arr[r, c])

                # Check for standard missing, or naturally occurring NaN
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

    # Warn if stale CSVs exist from a previous (potentially wrong) run
    existing = glob.glob(os.path.join(OUTPUT_DIR, "*.csv"))
    if existing:
        print(
            f"WARNING: {len(existing)} existing CSV(s) found in {OUTPUT_DIR}.\n"
            "If they were produced before the latlon_to_index row-formula fix,\n"
            "or before the UTC+7 conversion, delete them first so they are fully regenerated:\n"
            f"  rm {OUTPUT_DIR}/*.csv\n"
        )

    stations_df = pd.read_csv(STATIONS_FILE)
    
    # Safely handle column name variant: 'stationId' vs 'ID'
    id_col = 'stationId' if 'stationId' in stations_df.columns else 'ID'
    if id_col not in stations_df.columns:
        raise ValueError(f"Could not find a valid ID column. Available columns: {stations_df.columns.tolist()}")
        
    stations_df["safe_id"] = stations_df[id_col].apply(sanitize_filename)
    unique_stations = stations_df["safe_id"].unique()

    # Safely handle latitude and longitude column variants (case-insensitive & whitespace-stripped)
    lat_col = next((c for c in stations_df.columns if c.strip().lower() in ['latitude', 'lat']), None)
    lon_col = next((c for c in stations_df.columns if c.strip().lower() in ['longitude', 'lon', 'long']), None)
    
    if not lat_col or not lon_col:
        raise ValueError(f"Could not find valid latitude/longitude columns. Available columns: {stations_df.columns.tolist()}")

    # Pre-compute target arrays/indices once (drastically saves time & serialization overhead)
    station_coords = []
    for _, row in stations_df.iterrows():
        r, c = latlon_to_index(row[lat_col], row[lon_col])
        station_coords.append((row["safe_id"], r, c))

    search_path = os.path.join(target_directory, "**", "*.zip")
    zip_files   = sorted(glob.glob(search_path, recursive=True))

    if not zip_files:
        print(f"No .zip files found in {target_directory}.")
        return

    print(f"Found {len(zip_files)} zip files. Starting parallel extraction with 8 workers...")

    station_data    = {station: [] for station in unique_stations}
    processed_count = 0

    with ProcessPoolExecutor(max_workers=8) as executor:
        # Pass the lightweight precomputed tuple array 'station_coords' instead of the DataFrame
        futures = {executor.submit(process_single_file, f, station_coords): f for f in zip_files}

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

    cols = ["timestamp"] + TARGET_COLUMNS

    for station_id, records in station_data.items():
        if not records:
            continue

        new_df     = pd.DataFrame(records)
        output_csv = os.path.join(OUTPUT_DIR, f"{station_id}.csv")

        # Append to existing CSV if present, then dedup
        if os.path.exists(output_csv):
            df_old   = pd.read_csv(output_csv)
            df_final = pd.concat([df_old, new_df], ignore_index=True)
            df_final = df_final.drop_duplicates(subset=["timestamp"])
        else:
            df_final = new_df

        df_final = df_final[cols]
        df_final = df_final.sort_values("timestamp").reset_index(drop=True)
        
        df_final.to_csv(output_csv, index=False)
        print(f"Saved {len(df_final)} rows for {station_id} -> {os.path.basename(output_csv)}")


if __name__ == "__main__":
    target_dir = '/home/slow_data/Air_Quality/GIS'
    main(target_dir)