"""
Extract MCD19A2 MODIS AOD values for target sites — nearest-pixel only.

Spatial strategy (matches VIIRS ``extract_station_l2_nearest``):
  * The exact 1 km pixel that contains each station's coordinates is sampled.
  * The geodesic distance from the station to that pixel's centre (``dist_km``)
    is computed via the Haversine formula and stored in the output.
  * If ``dist_km > MAX_DIST_KM`` the observation is discarded, mirroring the
    hard distance cut-off used in the VIIRS nearest-pixel extractor.
  * If the exact pixel is NaN (cloud, retrieval failure, etc.) the orbit/station
    pair is dropped — no Chebyshev ring fill or spatial averaging is applied.

Output: one CSV per station under OUTPUT_DIR, appended incrementally.
Output columns include ``pixel_lat``, ``pixel_lon``, ``dist_km`` so that
downstream consumers can apply their own distance threshold.

Usage:
    python extract_station_mcd.py
    python extract_station_mcd.py --hdf-dir /custom/hdf/path --output-dir /custom/output
"""

import os
import glob
import argparse
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyproj
import rasterio
import rioxarray
from rasterio.transform import rowcol, xy as rasterio_xy
from tqdm import tqdm

# ── Paths ────────────────────────────────────────────────────────────────────
STATION_CSV = "/home/work1/projects/Air_Quality/Masterdata/AERONET_sites.csv"
HDF_DIR     = "/home/slow_data/Air_Quality/MODIS_MCD19A2"
OUTPUT_DIR  = "/home/slow_data/Air_Quality/MODIS_MCD19A2/sites_aod_v4"
ERROR_LOG   = "/home/slow_data/Air_Quality/MODIS_MCD19A2/extract_errors_nearest.log"

# ── Extraction settings ───────────────────────────────────────────────────────
MAX_DIST_KM     = 1.0   # max station → pixel-centre distance (km); 1 km matches
                        # the MODIS 1 km pixel size.  Raise to ~3.5 km to accept
                        # the coarser 5 km SDS pixel centres as well.
TZ_OFFSET_HOURS = 7     # UTC+7 (Vietnam)

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

# Module-level CRS transformers — created once, reused for every file
_WGS84_TO_SINU = pyproj.Transformer.from_crs("EPSG:4326", MODIS_CRS, always_xy=True)
_SINU_TO_WGS84 = pyproj.Transformer.from_crs(MODIS_CRS, "EPSG:4326", always_xy=True)

# SDS that live in the 5 km EOS grid (all others are 1 km)
_GRID5KM_SDS = {'cosSZA', 'cosVZA', 'RelAZ', 'Scattering_Angle', 'Glint_Angle'}


# ── Coordinate / distance helpers ─────────────────────────────────────────────
def build_station_df(csv_path: str) -> pd.DataFrame:
    """Load station CSV and add MODIS sinusoidal x/y columns."""
    df = pd.read_csv(csv_path)
    df['stationId'] = df['stationId'].astype(str)
    df['modis_x'], df['modis_y'] = _WGS84_TO_SINU.transform(
        df['longitude'].values,
        df['latitude'].values,
    )
    return df


def pixel_center_wgs84(transform, row: int, col: int) -> tuple[float, float]:
    """Return (lat, lon) WGS-84 of the centre of the pixel at (row, col)."""
    sinu_x, sinu_y = rasterio_xy(transform, row, col, offset='center')
    lon, lat = _SINU_TO_WGS84.transform(sinu_x, sinu_y)
    return float(lat), float(lon)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km (Haversine, WGS-84 mean Earth radius)."""
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (np.sin(dlat / 2) ** 2
         + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2) ** 2)
    return float(R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))


# ── Orbit-time parsing ────────────────────────────────────────────────────────
def parse_modis_orbit_times(
    orbit_time_str: str,
    tz_offset_hours: int = TZ_OFFSET_HOURS,
) -> list[datetime]:
    """
    Parse the Orbit_time_stamp HDF attribute into a list of local datetimes.

    Orbit_time_stamp format (space-separated tokens): YYYYDDDHHMMX
      YYYY = year, DDD = day-of-year, HH = UTC hour, MM = UTC minute,
      X    = satellite designator (T/A — ignored).
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


# ── SDS loading ───────────────────────────────────────────────────────────────
def load_sds_with_metadata(hdf_path: str, sds_name: str):
    """
    Load a Science Dataset (SDS) via rioxarray and apply scale / fill masking.
    Returns (data_array, attrs_dict).

    Shape after return:
      * (n_orbits, H, W)  when the file contains multiple orbits for this SDS.
      * (H, W)            when there is exactly one orbit (squeezed).
    """
    grid = 'grid5km' if sds_name in _GRID5KM_SDS else 'grid1km'
    path = f'HDF4_EOS:EOS_GRID:"{hdf_path}":{grid}:{sds_name}'
    da = rioxarray.open_rasterio(path, masked=False, lock=False)
    attrs = dict(da.attrs)
    data = da.values.astype(float)  # (bands, H, W)

    # Squeeze single-orbit SDS to (H, W)
    if data.ndim == 3 and data.shape[0] == 1:
        data = data.squeeze(axis=0)

    fill_value  = attrs.get('_FillValue')
    valid_range = attrs.get('valid_range')
    scale       = float(attrs.get('scale_factor', 1.0))
    offset      = float(attrs.get('add_offset',   0.0))

    if fill_value is not None:
        data = np.where(data == float(fill_value), np.nan, data)
    if valid_range is not None:
        try:
            lo, hi = float(valid_range[0]), float(valid_range[1])
            data = np.where((data < lo) | (data > hi), np.nan, data)
        except (TypeError, IndexError, ValueError):
            pass

    data = data * scale + offset
    return data, attrs


# ── Per-file extraction ───────────────────────────────────────────────────────
def extract_modis_for_sites(
    hdf_path: str,
    df_sites: pd.DataFrame,
    target_sds: list | None = None,
    max_dist_km: float = MAX_DIST_KM,
) -> pd.DataFrame:
    """
    Extract MODIS AOD at each station from a single MCD19A2 HDF file.

    Nearest-pixel only — no Chebyshev ring fill:
      1. Map each station to the 1 km pixel that contains its coordinates.
      2. Compute the Haversine distance from the station to that pixel's centre.
      3. Skip the station if ``dist_km > max_dist_km``.
      4. Sample the exact pixel across all requested SDS bands.
      5. Keep the orbit/station pair only when at least one primary AOD band
         (Optical_Depth_047 or Optical_Depth_055) is non-NaN.

    Output columns:
        stationId, stationName, timestamp, Timestamp_MODIS_str,
        pixel_lat, pixel_lon, dist_km, <all SDS bands>
    """
    if target_sds is None:
        target_sds = TARGET_SDS

    # Read orbit timestamps from global HDF4 file attributes
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', message='.*no geotransform.*')
        with rasterio.open(hdf_path) as src:
            orbit_time_str = src.tags().get('Orbit_time_stamp', '')
    local_times = parse_modis_orbit_times(orbit_time_str)

    if not local_times:
        return pd.DataFrame()

    # Load all requested SDS cubes
    data_cubes: dict[str, np.ndarray] = {}
    for sds_name in target_sds:
        try:
            data_array, _ = load_sds_with_metadata(hdf_path, sds_name)
            data_cubes[sds_name] = data_array
        except Exception as exc:
            tqdm.write(f"  [!] Could not load {sds_name}: {exc}")

    if not data_cubes:
        return pd.DataFrame()

    # Pixel-coordinate mapping uses the first 1 km SDS as the reference grid
    first_1km_sds = next((s for s in data_cubes if s not in _GRID5KM_SDS), None)
    if first_1km_sds is None:
        return pd.DataFrame()
    sample_path = f'HDF4_EOS:EOS_GRID:"{hdf_path}":grid1km:{first_1km_sds}'

    records = []

    with rasterio.open(sample_path) as src:
        all_rows, all_cols = rowcol(
            src.transform,
            df_sites['modis_x'].values,
            df_sites['modis_y'].values,
        )
        all_rows = [int(r) for r in all_rows]
        all_cols = [int(c) for c in all_cols]

        # Pre-extract as plain Python floats so the type-checker is satisfied
        # (itertuples() returns pandas Scalar which may include complex)
        station_lats: list[float] = df_sites['latitude'].astype(float).tolist()
        station_lons: list[float] = df_sites['longitude'].astype(float).tolist()

        # ── Pre-compute pixel centres & distances (constant across orbits) ──
        # station_pixel_info[i] = (row, col, pixel_lat, pixel_lon, dist_km)
        #                       or None when out-of-tile or beyond max_dist_km
        station_pixel_info: list[tuple | None] = []
        for i, station in enumerate(df_sites.itertuples()):
            row, col = all_rows[i], all_cols[i]

            # Skip sites outside this HDF tile
            if not (0 <= row < src.height and 0 <= col < src.width):
                station_pixel_info.append(None)
                continue

            plat, plon = pixel_center_wgs84(src.transform, row, col)
            dist = haversine_km(station_lats[i], station_lons[i], plat, plon)

            # Hard distance cut-off — mirrors VIIRS `if nearest_dist > threshold: continue`
            if dist > max_dist_km:
                station_pixel_info.append(None)
            else:
                station_pixel_info.append((row, col, plat, plon, dist))

        # ── Per-orbit, per-station extraction ────────────────────────────────
        for orbit_idx, layer_time in enumerate(local_times):
            for i, station in enumerate(df_sites.itertuples()):
                pinfo = station_pixel_info[i]
                if pinfo is None:
                    continue

                row, col, plat, plon, dist = pinfo

                record: dict = {
                    'stationId':           str(station.stationId),
                    'stationName':         station.stationName,
                    'timestamp':           layer_time,
                    'Timestamp_MODIS_str': layer_time.strftime('%Y%m%d_%H%M'),
                    'pixel_lat':           plat,
                    'pixel_lon':           plon,
                    'dist_km':             dist,
                }
                is_valid_row = False

                for sds_name, data_array in data_cubes.items():
                    arr_h = data_array.shape[-2]
                    arr_w = data_array.shape[-1]

                    # Scale row/col when SDS resolution differs from the 1 km reference
                    t_row = int(row * (arr_h / src.height))
                    t_col = int(col * (arr_w  / src.width))

                    # ── Exact pixel only — no ring fill ──────────────────────
                    if data_array.ndim == 3:
                        val = (float(data_array[orbit_idx, t_row, t_col])
                               if orbit_idx < data_array.shape[0] else np.nan)
                    else:
                        val = float(data_array[t_row, t_col])

                    record[sds_name] = val  # np.nan propagates cleanly

                    if sds_name in ('Optical_Depth_047', 'Optical_Depth_055') \
                            and not np.isnan(val):
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

    df_sites = build_station_df(STATION_CSV)
    print(f"Loaded {len(df_sites)} sites from {STATION_CSV}")

    # Build per-station output paths (keyed by stationId, filename uses stationName)
    station_csv_paths = {
        str(row.stationId): os.path.join(output_dir, f"{row.stationName}.csv")
        for row in df_sites.itertuples()
    }

    # Pre-load existing timestamps for incremental processing
    ts_cache: dict[str, set] = {
        sid: get_existing_timestamps(path)
        for sid, path in station_csv_paths.items()
    }
    print("Timestamp cache loaded.")

    hdf_files = sorted(
        glob.glob(os.path.join(hdf_dir, '**', 'MCD19A2*.hdf'), recursive=True)
    )
    print(f"Found {len(hdf_files)} HDF files to process.\n")

    stats = {'total': len(hdf_files), 'skipped': 0, 'processed': 0, 'error': 0}

    for hdf_file in tqdm(hdf_files, unit='file', desc='MCD19A2'):
        try:
            df_file = extract_modis_for_sites(hdf_file, df_sites)

            if df_file.empty:
                stats['skipped'] += 1
                continue

            for station_id, group in df_file.groupby('stationId'):
                sid = str(station_id)
                csv_path = station_csv_paths.get(sid)
                if csv_path is None:
                    continue

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

                ts_cache[sid].update(
                    new_rows['Timestamp_MODIS_str'].astype(str).tolist()
                )

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
        description=(
            "Extract MCD19A2 MODIS AOD values for target sites "
            "(nearest-pixel only, no spatial fill)."
        )
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
    parser.add_argument(
        '--max-dist-km', type=float, default=MAX_DIST_KM,
        help=(
            f"Maximum station-to-pixel-centre distance in km (default: {MAX_DIST_KM}). "
            "Observations beyond this threshold are discarded."
        ),
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    run_extraction(
        hdf_dir=args.hdf_dir,
        output_dir=args.output_dir,
        error_log=args.error_log,
    )
