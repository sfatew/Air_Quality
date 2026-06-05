"""
Extract MCD19A2 MODIS AOD values for target stations.

For each HDF file, pixel values are sampled at each station's exact 1 km pixel.
If that pixel is NaN (cloud, retrieval failure, etc.) the search expands
outward ring-by-ring (Chebyshev) up to `MAX_FILL_RADIUS` km, averaging all
valid pixels found at the nearest non-empty ring.

Output: one CSV per station under OUTPUT_DIR, appended incrementally.

Usage:
    python extract_station_mcd.py
    python extract_station_mcd.py --hdf-dir /custom/hdf/path --output-dir /custom/output
"""

import os
import re
import glob
import argparse
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyproj
import rasterio
import rioxarray
from rasterio.transform import rowcol
from tqdm import tqdm

# ── Paths ────────────────────────────────────────────────────────────────────
STATION_CSV   = "/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv"
HDF_DIR       = "/home/slow_data/Air_Quality/MODIS_MCD19A2/raw"
OUTPUT_DIR    = "/home/slow_data/Air_Quality/MODIS_MCD19A2/outputs/stations_aod_v3"
ERROR_LOG     = "/home/slow_data/Air_Quality/MODIS_MCD19A2/outputs/extract_errors.log"

# ── Extraction settings ───────────────────────────────────────────────────────
MAX_FILL_RADIUS = 5          # Chebyshev search radius in pixels / km
TZ_OFFSET_HOURS = 7          # UTC+7 (Vietnam)

TARGET_SDS = [
    'Optical_Depth_047',
    'Optical_Depth_055',
    'AOD_Uncertainty',
    'Column_WV',
    'AngstromExp_470-780',
    'AOD_QA',
    'FineModeFraction',
    'Injection_Height',
    'cosSZA',
    'cosVZA',
    'RelAZ',
    'Scattering_Angle',
    'Glint_Angle',
]

MODIS_CRS = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs"


# ── Coordinate helpers ────────────────────────────────────────────────────────
def build_station_df(csv_path: str) -> pd.DataFrame:
    """Load station CSV and add MODIS sinusoidal x/y columns."""
    df = pd.read_csv(csv_path)
    df['stationId'] = df['stationId'].astype(str)

    transformer = pyproj.Transformer.from_crs("EPSG:4326", MODIS_CRS, always_xy=True)
    df['modis_x'], df['modis_y'] = transformer.transform(
        df['longitude'].values,
        df['latitude'].values,
    )
    return df


# ── Orbit-time parsing ────────────────────────────────────────────────────────
def parse_modis_orbit_times(orbit_time_str: str, tz_offset_hours: int = TZ_OFFSET_HOURS) -> list:
    """
    Parse the Orbit_time_stamp HDF attribute into a list of local datetimes.

    Orbit_time_stamp format: YYYYDDDHHMMX … (space-separated tokens)
      YYYY = year, DDD = day-of-year, HH = UTC hour, MM = UTC minute,
      X    = satellite designator (T/A, ignored here)
    """
    if isinstance(orbit_time_str, bytes):
        orbit_time_str = orbit_time_str.decode('utf-8')

    local_datetimes = []
    for token in str(orbit_time_str).strip().split():
        if len(token) >= 11:
            year   = int(token[0:4])
            doy    = int(token[4:7])
            hour   = int(token[7:9])
            minute = int(token[9:11])
            dt_utc = datetime(year, 1, 1) + timedelta(days=doy - 1, hours=hour, minutes=minute)
            local_datetimes.append(dt_utc + timedelta(hours=tz_offset_hours))
    return local_datetimes


# SDS that live in the 5 km EOS grid (all others are 1 km)
_GRID5KM_SDS = {'cosSZA', 'cosVZA', 'RelAZ', 'Scattering_Angle', 'Glint_Angle'}


# ── SDS loading ───────────────────────────────────────────────────────────────
def load_sds_with_metadata(hdf_path: str, sds_name: str):
    """
    Load a Science Dataset (SDS) via rioxarray and apply scale factor / fill-value masking.
    Returns (data_array, metadata_dict).
    """
    grid = 'grid5km' if sds_name in _GRID5KM_SDS else 'grid1km'
    path = f'HDF4_EOS:EOS_GRID:"{hdf_path}":{grid}:{sds_name}'
    da = rioxarray.open_rasterio(path, masked=False, lock=False)
    attrs = dict(da.attrs)
    data = da.values.astype(float)  # (bands, H, W)

    # Squeeze single-orbit/2-D SDS to (H, W)
    if data.ndim == 3 and data.shape[0] == 1:
        data = data.squeeze(axis=0)

    scale_factor = float(attrs.get('scale_factor', 1.0))
    add_offset   = float(attrs.get('add_offset',   0.0))
    fill_value   = attrs.get('_FillValue')
    valid_range  = attrs.get('valid_range')

    if fill_value is not None:
        data = np.where(data == float(fill_value), np.nan, data)

    if valid_range is not None:
        try:
            lo, hi = float(valid_range[0]), float(valid_range[1])
            data = np.where((data < lo) | (data > hi), np.nan, data)
        except (TypeError, IndexError, ValueError):
            pass

    data = data * scale_factor + add_offset
    return data, attrs


# ── Spatial fill logic ────────────────────────────────────────────────────────
def get_nearest_valid(data_array, orbit_idx: int, row: int, col: int, max_radius: int = MAX_FILL_RADIUS):
    """
    Return the nearest valid (non-NaN) pixel value using Chebyshev distance rings.

    Priority:
      1. Exact pixel (ring 0) – returned immediately if not NaN.
      2. Rings 1..max_radius: collect ALL valid pixels at the nearest
         non-empty ring and return their mean.

    Returns:
        (value, ring)  – ring is 0 for exact pixel, k for fill ring, None if no
                         valid pixel found within max_radius.
    """
    h = data_array.shape[-2]
    w = data_array.shape[-1]

    def _get(r, c):
        return data_array[orbit_idx, r, c] if data_array.ndim == 3 else data_array[r, c]

    val = _get(row, col)
    if not np.isnan(val):
        return val, 0

    for ring in range(1, max_radius + 1):
        valid_vals = []
        # Top / bottom rows of Chebyshev square
        for dc in range(-ring, ring + 1):
            for dr in (-ring, ring):
                nr, nc = row + dr, col + dc
                if 0 <= nr < h and 0 <= nc < w:
                    v = _get(nr, nc)
                    if not np.isnan(v):
                        valid_vals.append(v)
        # Left / right columns (corners already covered)
        for dr in range(-ring + 1, ring):
            for dc in (-ring, ring):
                nr, nc = row + dr, col + dc
                if 0 <= nr < h and 0 <= nc < w:
                    v = _get(nr, nc)
                    if not np.isnan(v):
                        valid_vals.append(v)
        if valid_vals:
            return np.mean(valid_vals), ring

    return np.nan, None


# ── Per-file extraction ───────────────────────────────────────────────────────
def extract_modis_for_stations(
    hdf_path: str,
    df_stations: pd.DataFrame,
    target_sds: list = None,
    max_fill_radius: int = MAX_FILL_RADIUS,
) -> pd.DataFrame:
    """
    Extract MODIS AOD values for each station from a single MCD19A2 HDF file.

    Rows are appended only when at least one primary AOD band
    (Optical_Depth_047 or Optical_Depth_055) is non-NaN after the spatial fill.

    Returns a DataFrame with columns: ID, timestamp, Timestamp_MODIS_str, <SDS…>,
    and optional <SDS>_fill_ring columns when a fill was applied.
    """
    if target_sds is None:
        target_sds = TARGET_SDS

    # Read orbit timestamps via rasterio (global HDF4 file attributes).
    # Suppress NotGeoreferencedWarning — the raw HDF4 container has no geotransform by design.
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', message='.*no geotransform.*')
        with rasterio.open(hdf_path) as src:
            orbit_time_str = src.tags().get('Orbit_time_stamp', '')
    local_times = parse_modis_orbit_times(orbit_time_str)

    if not local_times:
        return pd.DataFrame()

    # Load all requested SDS cubes
    data_cubes = {}
    for sds_name in target_sds:
        try:
            data_array, _ = load_sds_with_metadata(hdf_path, sds_name)
            data_cubes[sds_name] = data_array
        except Exception as exc:
            tqdm.write(f"  [!] Could not load {sds_name}: {exc}")

    if not data_cubes:
        return pd.DataFrame()

    # Use rasterio only for pixel-coordinate mapping
    first_sds = next(iter(data_cubes))
    sample_path = f'HDF4_EOS:EOS_GRID:"{hdf_path}":grid1km:{first_sds}'
    records = []

    with rasterio.open(sample_path) as src:
        all_rows, all_cols = rowcol(
            src.transform,
            df_stations['modis_x'].values,
            df_stations['modis_y'].values,
        )
        all_rows = [int(r) for r in all_rows]
        all_cols = [int(c) for c in all_cols]

        for orbit_idx, layer_time in enumerate(local_times):
            for i, station in enumerate(df_stations.itertuples()):
                try:
                    row, col = all_rows[i], all_cols[i]
                except IndexError:
                    continue

                if not (0 <= row < src.height and 0 <= col < src.width):
                    continue

                record = {
                    'stationId': str(station.stationId),
                    'stationName': station.stationName,
                    'timestamp': layer_time,
                    'Timestamp_MODIS_str': layer_time.strftime('%Y%m%d_%H%M'),
                }
                is_valid_row = False

                for sds_name, data_array in data_cubes.items():
                    arr_h = data_array.shape[-2]
                    arr_w = data_array.shape[-1]

                    # Scale pixel coords if SDS resolution differs from 1 km
                    t_row = int(row * (arr_h / src.height))
                    t_col = int(col * (arr_w  / src.width))

                    val, fill_ring = get_nearest_valid(
                        data_array, orbit_idx, t_row, t_col, max_radius=max_fill_radius
                    )
                    record[sds_name] = val
                    record[f'{sds_name}_fill_ring'] = fill_ring if (fill_ring is not None and fill_ring > 0) else np.nan

                    if sds_name in ('Optical_Depth_047', 'Optical_Depth_055') and not np.isnan(val):
                        is_valid_row = True

                if is_valid_row:
                    records.append(record)

    return pd.DataFrame(records)


# ── Incremental timestamp cache ───────────────────────────────────────────────
def get_existing_timestamps(csv_path: str) -> set:
    if not os.path.isfile(csv_path):
        return set()
    try:
        df = pd.read_csv(csv_path, usecols=['Timestamp_MODIS_str'])
        return set(df['Timestamp_MODIS_str'].astype(str).tolist())
    except Exception:
        return set()


# ── Main extraction loop ──────────────────────────────────────────────────────
def run_extraction(hdf_dir: str, output_dir: str, error_log: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    df_stations = build_station_df(STATION_CSV)
    print(f"Loaded {len(df_stations)} stations from {STATION_CSV}")

    # Build per-station output paths (keyed by stationId, filename uses stationName)
    station_csv_paths = {
        str(row.stationId): os.path.join(output_dir, f"{row.stationName}.csv")
        for row in df_stations.itertuples()
    }

    # Pre-load existing timestamps for incremental processing
    ts_cache: dict[str, set] = {
        sid: get_existing_timestamps(path)
        for sid, path in station_csv_paths.items()
    }
    print("Timestamp cache loaded.")

    # Discover all HDF files
    hdf_files = sorted(glob.glob(os.path.join(hdf_dir, '**', 'MCD19A2*.hdf'), recursive=True))
    print(f"Found {len(hdf_files)} HDF files to process.\n")

    stats = {'total': len(hdf_files), 'skipped': 0, 'processed': 0, 'error': 0}

    for hdf_file in tqdm(hdf_files, unit='file', desc='MCD19A2'):
        try:
            df_file = extract_modis_for_stations(hdf_file, df_stations)

            if df_file.empty:
                stats['skipped'] += 1
                continue

            # Append new records per station
            for station_id, group in df_file.groupby('stationId'):
                sid = str(station_id)
                csv_path = station_csv_paths.get(sid)
                if csv_path is None:
                    continue

                # Filter out already-saved timestamps
                new_rows = group[
                    ~group['Timestamp_MODIS_str'].isin(ts_cache[sid])
                ]
                if new_rows.empty:
                    continue

                file_exists = os.path.isfile(csv_path)
                if file_exists:
                    existing_cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
                    new_rows = new_rows.reindex(columns=existing_cols)
                new_rows.to_csv(csv_path, mode='a', header=not file_exists, index=False)

                # Update cache
                ts_cache[sid].update(new_rows['Timestamp_MODIS_str'].astype(str).tolist())

            stats['processed'] += 1

        except Exception as exc:
            stats['error'] += 1
            tqdm.write(f"[!] Error processing {os.path.basename(hdf_file)}: {exc}")
            os.makedirs(os.path.dirname(error_log), exist_ok=True)
            with open(error_log, 'a') as f:
                f.write(f"{hdf_file}\t{exc}\n")

    print("\nDone.")
    print(f"  total={stats['total']}  processed={stats['processed']}  "
          f"skipped={stats['skipped']}  error={stats['error']}")


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract MCD19A2 MODIS AOD values for target stations."
    )
    parser.add_argument(
        '--hdf-dir', default=HDF_DIR,
        help=f"Root directory containing MCD19A2 HDF files (default: {HDF_DIR})",
    )
    parser.add_argument(
        '--output-dir', default=OUTPUT_DIR,
        help=f"Directory for per-station output CSVs (default: {OUTPUT_DIR})",
    )
    parser.add_argument(
        '--error-log', default=ERROR_LOG,
        help=f"Path to error log file (default: {ERROR_LOG})",
    )
    return parser.parse_args()


if __name__ == '__main__':
    # python "extract_station_mcd.py"
    # python "extract_station_mcd.py" --hdf-dir /custom/hdf --output-dir /custom/out
    args = parse_args()
    run_extraction(
        hdf_dir=args.hdf_dir,
        output_dir=args.output_dir,
        error_log=args.error_log,
    )
