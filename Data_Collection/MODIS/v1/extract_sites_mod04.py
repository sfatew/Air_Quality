"""
Extract MODIS Deep Blue + Dark Target AOD values (MOD04_L2 / MYD04_L2) for
target sites — nearest-pixel strategy.

Spatial strategy (mirrors MCD19A2 extractor in extract_sites_mcd.py):
  * Load the per-granule Latitude / Longitude arrays.
  * Build a 3-D unit-sphere KD-tree for fast nearest-pixel lookup.
  * For each station find the single nearest granule pixel.
  * Compute the Haversine distance station → pixel centre; discard if
    > MAX_DIST_KM (default 5 km ≈ half the nominal 10 km footprint).
  * If all primary AOD bands at the nearest pixel are NaN, drop the record.

Output: one CSV per station under OUTPUT_DIR, appended incrementally so
        interrupted runs can be safely restarted.

Output columns:
    stationId, stationName, satellite, timestamp, Timestamp_str,
    pixel_lat, pixel_lon, dist_km, <all requested SDS fields>

Dependencies (beyond the standard scientific stack):
    pyhdf   — HDF4 reader  (pip install pyhdf)
    scipy   — KD-tree      (pip install scipy)

Usage
-----
    # Process both Terra (MOD04_L2) and Aqua (MYD04_L2) with defaults:
    python extract_sites_mod04.py

    # Process only Terra, custom paths:
    python extract_sites_mod04.py --products MOD04 \\
        --hdf-dir /data/MOD04_L2 --output-dir /out/sites_aod

    # Process only Aqua, raise distance threshold to 7.5 km:
    python extract_sites_mod04.py --products MYD04 --max-dist-km 7.5
"""

import os
import re
import glob
import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pyhdf.SD import SD, SDC
from scipy.spatial import cKDTree
from tqdm import tqdm

# ── Default paths ─────────────────────────────────────────────────────────────
STATION_CSV = "/home/work1/projects/Air_Quality/Masterdata/AERONET_sites.csv"

PRODUCT_CFG: dict[str, dict] = {
    'MOD04': {
        'hdf_dir':    '/home/slow_data/Air_Quality/MODIS_MOD04_L2',
        'output_dir': '/home/slow_data/Air_Quality/MODIS_MOD04_L2/sites_aod',
        'error_log':  '/home/slow_data/Air_Quality/MODIS_MOD04_L2/extract_errors.log',
        'satellite':  'Terra',
    },
    'MYD04': {
        'hdf_dir':    '/home/slow_data/Air_Quality/MODIS_MYD04_L2',
        'output_dir': '/home/slow_data/Air_Quality/MODIS_MYD04_L2/sites_aod',
        'error_log':  '/home/slow_data/Air_Quality/MODIS_MYD04_L2/extract_errors.log',
        'satellite':  'Aqua',
    },
}

# ── Extraction settings ───────────────────────────────────────────────────────
MAX_DIST_KM     = 5.0   # ≈ half the nominal 10 km pixel footprint
TZ_OFFSET_HOURS = 7     # UTC+7 (Vietnam)

# Geolocation SDS names (present in every MOD04_L2 / MYD04_L2 file)
LAT_SDS = 'Latitude'
LON_SDS = 'Longitude'

TARGET_SDS = [
    # ── Deep Blue — land aerosol retrievals ──────────────────────────────
    'Deep_Blue_Aerosol_Optical_Depth_550nm_Land',
    'Deep_Blue_Aerosol_Optical_Depth_550nm_Land_Best_Estimate',
    'Deep_Blue_Aerosol_Optical_Depth_550nm_Land_QA_Flag',
    'Deep_Blue_Aerosol_Optical_Depth_550nm_Land_STD',
    'Deep_Blue_Angstrom_Exponent_Land',
    'Deep_Blue_Single_Scattering_Albedo_Land',
    # ── Dark Target — combined land + ocean ──────────────────────────────
    'Optical_Depth_Land_And_Ocean',
    'Image_Optical_Depth_Land_And_Ocean',
    'Optical_Depth_Ratio_Small_Land',
    'Optical_Depth_Ratio_Small_Land_And_Ocean',
    # ── Viewing geometry ─────────────────────────────────────────────────
    'Solar_Zenith',
    'Sensor_Zenith',
    'Solar_Azimuth',
    'Sensor_Azimuth',
    'Scattering_Angle',
    'Glint_Angle',
]

# At least one of these must be non-NaN to keep a record
PRIMARY_AOD_SDS = frozenset({
    'Deep_Blue_Aerosol_Optical_Depth_550nm_Land',
    'Optical_Depth_Land_And_Ocean',
})

# Matches both MOD04_L2 and MYD04_L2 filenames:
# e.g. MOD04_L2.A2020015.0335.061.2020016154201.hdf
_FNAME_RE = re.compile(r'\.A(\d{7})\.(\d{4})\.')


# ── Station loading ───────────────────────────────────────────────────────────
def build_station_df(csv_path: str) -> pd.DataFrame:
    """Load station CSV; ensure stationId is a string column."""
    df = pd.read_csv(csv_path)
    df['stationId'] = df['stationId'].astype(str)
    return df


# ── Filename / time parsing ───────────────────────────────────────────────────
def parse_granule_datetime(
    hdf_path: str,
    tz_offset_hours: int = TZ_OFFSET_HOURS,
) -> datetime | None:
    """
    Parse acquisition datetime from a MOD04_L2 / MYD04_L2 filename.

    Filename pattern:  {PRODUCT}.A{YYYYDDD}.{HHMM}.{VER}.{PRODTIME}.hdf
    Example:           MOD04_L2.A2020015.0335.061.2020016154201.hdf
    """
    m = _FNAME_RE.search(os.path.basename(hdf_path))
    if m is None:
        return None
    year_doy, hhmm = m.group(1), m.group(2)
    dt_utc = (
        datetime(int(year_doy[:4]), 1, 1)
        + timedelta(
            days    = int(year_doy[4:]) - 1,
            hours   = int(hhmm[:2]),
            minutes = int(hhmm[2:]),
        )
    )
    return dt_utc + timedelta(hours=tz_offset_hours)


# ── HDF4 I/O ──────────────────────────────────────────────────────────────────
@contextmanager
def open_hdf4(hdf_path: str):
    """Context manager for an HDF4 SD file — guarantees closure on exit."""
    sd = SD(hdf_path, SDC.READ)
    try:
        yield sd
    finally:
        sd.end()


def read_sds(sd: SD, sds_name: str) -> tuple[np.ndarray | None, dict]:
    """
    Read one SDS from an already-open HDF4 SD object.

    Applies _FillValue masking, valid_range clipping (raw units), and
    scale_factor / add_offset conversion.

    Returns ``(physical_array, attrs)`` or ``(None, {})`` when the SDS is
    absent from the file.
    """
    if sds_name not in sd.datasets():
        return None, {}

    sds_obj = sd.select(sds_name)
    try:
        raw   = sds_obj[:].astype(float)
        attrs = sds_obj.attributes()
    finally:
        sds_obj.endaccess()

    fill        = attrs.get('_FillValue')
    valid_range = attrs.get('valid_range')
    scale       = float(attrs.get('scale_factor', 1.0))
    offset      = float(attrs.get('add_offset',   0.0))

    data = raw.copy()
    if fill is not None:
        data[data == float(fill)] = np.nan
    if valid_range is not None:
        try:
            lo, hi = float(valid_range[0]), float(valid_range[1])
            data[(data < lo) | (data > hi)] = np.nan
        except (TypeError, IndexError, ValueError):
            pass

    data = data * scale + offset
    return data, attrs


# ── Spatial helpers ───────────────────────────────────────────────────────────
def _latlon_to_xyz(lat_deg: np.ndarray, lon_deg: np.ndarray) -> np.ndarray:
    """Convert lat/lon arrays (degrees) to 3-D unit-sphere Cartesian coords."""
    lat_r = np.radians(lat_deg)
    lon_r = np.radians(lon_deg)
    return np.stack(
        [
            np.cos(lat_r) * np.cos(lon_r),
            np.cos(lat_r) * np.sin(lon_r),
            np.sin(lat_r),
        ],
        axis=-1,
    )


def build_kdtree(lat_2d: np.ndarray, lon_2d: np.ndarray) -> cKDTree:
    """
    Build a KD-tree on 3-D unit-sphere coordinates derived from the granule's
    Latitude / Longitude grids.

    NaN pixels are placed at the origin (0, 0, 0) so they are never the
    nearest point for any valid station query.
    """
    flat_lat = lat_2d.ravel()
    flat_lon = lon_2d.ravel()
    valid    = np.isfinite(flat_lat) & np.isfinite(flat_lon)

    xyz = np.zeros((flat_lat.size, 3), dtype=np.float64)
    if valid.any():
        xyz[valid] = _latlon_to_xyz(flat_lat[valid], flat_lon[valid])
    return cKDTree(xyz)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km (Haversine formula, mean Earth radius)."""
    R = 6371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2))
        * np.sin(dlon / 2) ** 2
    )
    return float(R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))


# ── Per-granule extraction ────────────────────────────────────────────────────
def extract_mod04_for_sites(
    hdf_path: str,
    df_sites: pd.DataFrame,
    satellite: str,
    target_sds: list[str] | None = None,
    max_dist_km: float = MAX_DIST_KM,
) -> pd.DataFrame:
    """
    Extract Deep Blue / Dark Target AOD at each station from one L2 granule.

    Algorithm
    ---------
    1.  Parse acquisition datetime from the filename.
    2.  Load Latitude / Longitude arrays; build a 3-D KD-tree.
    3.  Quick-reject the granule when no station falls inside the bounding box
        (lat/lon extent of valid pixels ± max_dist_km buffer).
    4.  For each candidate station, query the KD-tree for the nearest pixel.
    5.  Compute the Haversine distance; discard if > max_dist_km.
    6.  Load all requested SDS; sample the nearest pixel for each station.
    7.  Retain records where at least one primary AOD band is non-NaN.
    """
    if target_sds is None:
        target_sds = TARGET_SDS

    # 1. Granule datetime from filename
    dt_local = parse_granule_datetime(hdf_path)
    if dt_local is None:
        return pd.DataFrame()

    with open_hdf4(hdf_path) as sd:

        # 2. Geolocation
        lat_arr, _ = read_sds(sd, LAT_SDS)
        lon_arr, _ = read_sds(sd, LON_SDS)
        if lat_arr is None or lon_arr is None:
            return pd.DataFrame()

        valid_geo = np.isfinite(lat_arr) & np.isfinite(lon_arr)
        if not valid_geo.any():
            return pd.DataFrame()

        # 3. Bounding-box pre-filter (1° ≈ 111 km conservative buffer)
        buf     = max_dist_km / 111.0
        lat_min = float(lat_arr[valid_geo].min()) - buf
        lat_max = float(lat_arr[valid_geo].max()) + buf
        lon_min = float(lon_arr[valid_geo].min()) - buf
        lon_max = float(lon_arr[valid_geo].max()) + buf

        in_bbox = (
            (df_sites['latitude']  >= lat_min) &
            (df_sites['latitude']  <= lat_max) &
            (df_sites['longitude'] >= lon_min) &
            (df_sites['longitude'] <= lon_max)
        )
        df_near = df_sites[in_bbox].reset_index(drop=True)
        if df_near.empty:
            return pd.DataFrame()

        # 4. KD-tree nearest-pixel look-up
        tree        = build_kdtree(lat_arr, lon_arr)
        grid_shape  = lat_arr.shape

        # station_pinfo[i] = (row, col, pixel_lat, pixel_lon, dist_km) | None
        station_pinfo: list[tuple | None] = []
        for station in df_near.itertuples():
            s_lat = float(station.latitude)
            s_lon = float(station.longitude)
            q_xyz = _latlon_to_xyz(np.array([s_lat]), np.array([s_lon]))[0]
            _, flat_idx = tree.query(q_xyz)

            prow, pcol = np.unravel_index(int(flat_idx), grid_shape)
            prow, pcol = int(prow), int(pcol)
            plat = float(lat_arr[prow, pcol])
            plon = float(lon_arr[prow, pcol])

            # 5. Distance threshold
            if not (np.isfinite(plat) and np.isfinite(plon)):
                station_pinfo.append(None)
                continue
            dist = haversine_km(s_lat, s_lon, plat, plon)
            if dist > max_dist_km:
                station_pinfo.append(None)
            else:
                station_pinfo.append((prow, pcol, plat, plon, dist))

        if all(p is None for p in station_pinfo):
            return pd.DataFrame()

        # 6. Load SDS cubes (skip absent fields gracefully)
        data_cubes: dict[str, np.ndarray] = {}
        for sds_name in target_sds:
            arr, _ = read_sds(sd, sds_name)
            if arr is not None:
                data_cubes[sds_name] = arr

    # 7. Build output records
    records: list[dict] = []
    for i, station in enumerate(df_near.itertuples()):
        pinfo = station_pinfo[i]
        if pinfo is None:
            continue

        prow, pcol, plat, plon, dist = pinfo
        record: dict = {
            'stationId':     str(station.stationId),
            'stationName':   station.stationName,
            'satellite':     satellite,
            'timestamp':     dt_local,
            'Timestamp_str': dt_local.strftime('%Y%m%d_%H%M'),
            'pixel_lat':     plat,
            'pixel_lon':     plon,
            'dist_km':       dist,
        }

        has_primary_aod = False
        for sds_name, arr in data_cubes.items():
            if arr.ndim == 2:
                val = float(arr[prow, pcol])
            elif arr.ndim == 3:
                # Multi-band SDS (e.g. Corrected_Optical_Depth_Land has
                # bands 0/1/2 = 470/550/660 nm).  Default: take band 0
                # and store the full vector under indexed column names.
                # Here we simplify to band 0 — adjust if you need all bands.
                val = float(arr[0, prow, pcol])
            else:
                val = np.nan
            record[sds_name] = val

            if sds_name in PRIMARY_AOD_SDS and not np.isnan(val):
                has_primary_aod = True

        if has_primary_aod:
            records.append(record)

    return pd.DataFrame(records)


# ── Incremental timestamp cache ───────────────────────────────────────────────
def get_existing_timestamps(csv_path: str) -> set[str]:
    """Return the set of Timestamp_str values already written to a station CSV."""
    if not os.path.isfile(csv_path):
        return set()
    try:
        df = pd.read_csv(csv_path, usecols=['Timestamp_str'])
        return set(df['Timestamp_str'].astype(str).tolist())
    except Exception:
        return set()


# ── Main extraction loop ──────────────────────────────────────────────────────
def run_extraction(
    product: str,
    hdf_dir: str,
    output_dir: str,
    error_log: str,
    satellite: str,
    max_dist_km: float = MAX_DIST_KM,
) -> None:
    """Run the full extraction loop for one product (MOD04 or MYD04)."""
    os.makedirs(output_dir, exist_ok=True)

    df_sites = build_station_df(STATION_CSV)
    print(f"[{product}/{satellite}] {len(df_sites)} sites from {STATION_CSV}")

    # Per-station output paths keyed by stationId
    station_csv_paths: dict[str, str] = {
        str(row.stationId): os.path.join(output_dir, f"{row.stationName}.csv")
        for row in df_sites.itertuples()
    }

    # Pre-load existing timestamps for incremental (restartable) processing
    ts_cache: dict[str, set[str]] = {
        sid: get_existing_timestamps(path)
        for sid, path in station_csv_paths.items()
    }
    print(f"[{product}/{satellite}] Timestamp cache loaded.")

    hdf_files = sorted(
        glob.glob(os.path.join(hdf_dir, '**', '*.hdf'), recursive=True)
    )
    print(f"[{product}/{satellite}] Found {len(hdf_files)} HDF files.\n")

    stats = {
        'total':     len(hdf_files),
        'processed': 0,
        'no_data':   0,
        'error':     0,
    }

    for hdf_file in tqdm(hdf_files, unit='file', desc=f'{product}'):
        try:
            df_file = extract_mod04_for_sites(
                hdf_file,
                df_sites,
                satellite,
                max_dist_km=max_dist_km,
            )

            if df_file.empty:
                stats['no_data'] += 1
                continue

            for station_id, group in df_file.groupby('stationId'):
                sid      = str(station_id)
                csv_path = station_csv_paths.get(sid)
                if csv_path is None:
                    continue

                new_rows = group[
                    ~group['Timestamp_str'].isin(ts_cache[sid])
                ]
                if new_rows.empty:
                    continue

                file_exists = os.path.isfile(csv_path)
                if file_exists:
                    # Align columns to the existing file's schema
                    existing_cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
                    new_rows = new_rows.reindex(columns=existing_cols)
                new_rows.to_csv(
                    csv_path, mode='a', header=not file_exists, index=False,
                )
                ts_cache[sid].update(
                    new_rows['Timestamp_str'].astype(str).tolist()
                )

            stats['processed'] += 1

        except Exception as exc:
            stats['error'] += 1
            tqdm.write(f"[!] {os.path.basename(hdf_file)}: {exc}")
            os.makedirs(os.path.dirname(error_log), exist_ok=True)
            with open(error_log, 'a') as f:
                f.write(f"{hdf_file}\t{exc}\n")

    print(f"\n[{product}/{satellite}] Done.")
    print(
        f"  total={stats['total']}  processed={stats['processed']}  "
        f"no_data={stats['no_data']}  error={stats['error']}"
    )


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract MODIS Deep Blue + Dark Target AOD (MOD04_L2 / MYD04_L2) "
            "for target sites (nearest-pixel, no spatial fill)."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--products',
        nargs='+',
        choices=['MOD04', 'MYD04'],
        default=['MOD04', 'MYD04'],
        metavar='PRODUCT',
        help="Products to process: MOD04 (Terra), MYD04 (Aqua), or both.",
    )
    # Path overrides — only meaningful when a single product is requested
    parser.add_argument(
        '--hdf-dir',
        default=None,
        help=(
            "Root directory containing HDF files.  "
            "Overrides the product default; only applied when a single "
            "--products value is given."
        ),
    )
    parser.add_argument(
        '--output-dir',
        default=None,
        help="Directory for per-station output CSVs (overrides product default).",
    )
    parser.add_argument(
        '--error-log',
        default=None,
        help="Path to the error log file (overrides product default).",
    )
    parser.add_argument(
        '--max-dist-km',
        type=float,
        default=MAX_DIST_KM,
        help=(
            f"Maximum station-to-pixel-centre distance in km (default: {MAX_DIST_KM}). "
            "Observations beyond this threshold are discarded."
        ),
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    single = len(args.products) == 1   # path overrides apply only for single-product runs
    for product in args.products:
        cfg = PRODUCT_CFG[product]
        run_extraction(
            product     = product,
            hdf_dir     = (args.hdf_dir    if single and args.hdf_dir    else cfg['hdf_dir']),
            output_dir  = (args.output_dir if single and args.output_dir else cfg['output_dir']),
            error_log   = (args.error_log  if single and args.error_log  else cfg['error_log']),
            satellite   = cfg['satellite'],
            max_dist_km = args.max_dist_km,
        )
