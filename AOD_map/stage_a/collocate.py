"""Extract satellite AOD at AERONET station coordinates and build matched pairs.

Spatial strategy (Ichoku et al. 2002; Levy et al. 2010):
    Primary   — exact pixel / grid cell containing the station coordinates.
    Fallback 1 — 3×3 native-cell neighbourhood mean of valid pixels.
    Fallback 2 — 5×5 neighbourhood (flagged; used only for low-N strata).
    Rejection  — matchup dropped if within-box AOD std > COLLOCATE_BOX_STD_MAX.

Temporal strategy per sensor:
    Himawari L2 : each AERONET observation; all 10-min files within ±LEO_WINDOW_MIN.
    Himawari L3 : daily mean AERONET AOD vs. all L3 hourly composites for the day.
    VIIRS       : each AERONET observation; granules within ±LEO_WINDOW_MIN.
    MODIS MAIAC : each AERONET observation; per-orbit UTC from HDF Orbit_time_stamp
                  attribute; only orbits within ±MODIS_WINDOW_MIN are used.

Output:
    One CSV per (sensor, site) at COLLOCATE_DIR / {sensor}_{site}.csv
    Columns: sensor, site, region, season, month, lat, lon, date,
             timestamp_aeronet, timestamp_satellite,
             satellite_aod, aeronet_aod, dist_km, vza, sza,
             spatial_flag, box_std
"""

from __future__ import annotations
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional
import glob

import numpy as np
import pandas as pd
import rasterio

try:
    from tqdm import tqdm as _tqdm_cls
    _HAS_TQDM = True
except ImportError:
    _tqdm_cls = None  # type: ignore[assignment]
    _HAS_TQDM = False


def _make_bar(iterable, **kwargs):
    if _HAS_TQDM and _tqdm_cls is not None:
        return _tqdm_cls(iterable, **kwargs)
    return None

from config import (
    AERONET_SITES, AERONET_DIR, DRY_MONTHS,
    HIMAWARI_L2_DIR, HIMAWARI_L3_DIR,
    COLLOCATE_DIR, EARTH_RADIUS_KM,
    LATS, LONS, NLAT, NLON, GRID_RES, LAT_MAX, LON_MIN,
    LEO_WINDOW_MIN, MODIS_WINDOW_MIN,
    TZ_OFFSET_HOURS,
    COLLOCATE_SPATIAL_FLAG_EXACT, COLLOCATE_SPATIAL_FLAG_3X3, COLLOCATE_SPATIAL_FLAG_5X5,
    COLLOCATE_BOX_STD_MAX,
    VIIRS_EXACT_MAX_KM, VIIRS_3X3_MAX_KM, VIIRS_5X5_MAX_KM,
    MODIS_EXACT_MAX_KM, MODIS_3X3_MAX_KM, MODIS_5X5_MAX_KM,
)
from aeronet   import load_aeronet, window_mean
from himawari  import (
    _l2_files_in_window, _l3_files_in_window,
    _parse_l2_utc, _parse_l3_utc, _read_band,
)
from viirs     import _viirs_files_in_window, _read_viirs_file, _parse_viirs_utc
from modis     import _modis_files_for_date, _read_modis_tile, _read_modis_orbit_times

# AERONET timestamps are stored as UTC+7 (Vietnam local time) by crawl.py.
# All satellite file names and orbit-time stamps use UTC.
# _TZ converts between the two: subtract to get UTC, add to get UTC+7.
_TZ = timedelta(hours=TZ_OFFSET_HOURS)


# ── Haversine distance ────────────────────────────────────────────────────────

def _haversine_km(lat1: float, lon1: float,
                  lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    R = EARTH_RADIUS_KM
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1))*np.cos(np.radians(lat2))*np.sin(dlon/2)**2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


# ── Grid-index helper for Himawari ───────────────────────────────────────────

def _station_pixel(station_lat: float, station_lon: float
                   ) -> Optional[tuple[int, int]]:
    """Return (row, col) of the 0.05° grid cell containing the station."""
    row = int((LAT_MAX - station_lat) / GRID_RES)
    col = int((station_lon - LON_MIN) / GRID_RES)
    if 0 <= row < NLAT and 0 <= col < NLON:
        return row, col
    return None


# ── Spatial-sampling helpers ─────────────────────────────────────────────────

_GRID_TIERS = (
    (0, COLLOCATE_SPATIAL_FLAG_EXACT),
    (1, COLLOCATE_SPATIAL_FLAG_3X3),
    (2, COLLOCATE_SPATIAL_FLAG_5X5),
)


def _sample_grid_aod(
    aod_2d: np.ndarray,
    row: int,
    col: int,
    nrows: int,
    ncols: int,
    std_max: float = COLLOCATE_BOX_STD_MAX,
) -> 'tuple[float, float, int, int] | None':
    """Sample a 2-D AOD grid at (row, col) with tiered neighbourhood fallback.

    Tries exact cell → 3×3 → 5×5.  Returns ``(mean, std, n_valid, spatial_flag)``
    for the first tier that has at least one valid (finite, non-negative) value
    and whose within-box std does not exceed *std_max*.  Returns ``None`` if the
    scene is heterogeneous at the first populated tier, or if no data exist.

    Used for Himawari (0.05° grid) and MODIS MAIAC when a 2-D array is
    available (e.g. after loading an orbit layer).
    """
    for hw, flag in _GRID_TIERS:
        r0 = max(0, row - hw)
        r1 = min(nrows, row + hw + 1)
        c0 = max(0, col - hw)
        c1 = min(ncols, col + hw + 1)
        patch = aod_2d[r0:r1, c0:c1].ravel()
        vals  = patch[np.isfinite(patch) & (patch >= 0)]
        if len(vals) == 0:
            continue
        std_v = float(np.std(vals, ddof=0))
        if std_v > std_max:
            return None   # scene too heterogeneous at this scale
        return float(np.mean(vals)), std_v, int(len(vals)), flag
    return None


def _sample_swath_aod(
    swath_lat: np.ndarray,
    swath_lon: np.ndarray,
    swath_aod: np.ndarray,
    station_lat: float,
    station_lon: float,
    exact_km: float,
    r3x3_km: float,
    r5x5_km: float,
    std_max: float = COLLOCATE_BOX_STD_MAX,
) -> 'tuple[float, float, float, int, int] | None':
    """Sample ungridded swath pixels near a station with tiered neighbourhood fallback.

    Tries exact pixel → 3×3-equivalent radius → 5×5-equivalent radius.
    Returns ``(mean_aod, std_aod, mean_dist_km, n_valid, spatial_flag)`` for the
    first tier that has data and passes the heterogeneity filter, or ``None``.

    Used for VIIRS (raw swath) and MODIS when flat pixel arrays are used.
    """
    dist = _haversine_km(station_lat, station_lon, swath_lat, swath_lon)
    valid_aod = np.isfinite(swath_aod) & (swath_aod >= 0)

    for max_km, flag in (
        (exact_km, COLLOCATE_SPATIAL_FLAG_EXACT),
        (r3x3_km,  COLLOCATE_SPATIAL_FLAG_3X3),
        (r5x5_km,  COLLOCATE_SPATIAL_FLAG_5X5),
    ):
        mask = valid_aod & (dist <= max_km)
        vals = swath_aod[mask]
        if len(vals) == 0:
            continue
        std_v = float(np.std(vals, ddof=0))
        if std_v > std_max:
            return None
        return (
            float(np.mean(vals)),
            std_v,
            float(np.mean(dist[mask])),
            int(len(vals)),
            flag,
        )
    return None


# ── Per-sensor collocators ────────────────────────────────────────────────────

def _collocate_himawari_l2(
    aeronet_df: pd.DataFrame,
    site: str,
    station_lat: float,
    station_lon: float,
    window_min: int = LEO_WINDOW_MIN,
) -> list[dict]:
    """Match Himawari L2 10-min files to individual AERONET observations.

    Spatial strategy: exact grid cell → 3×3 → 5×5 neighbourhood (cell indices).
    For each AERONET time, all L2 files within ±window_min are collected; the
    neighbourhood mean is computed per file and then averaged across files.
    Records where the within-box std exceeds COLLOCATE_BOX_STD_MAX are dropped.
    """
    records = []

    for _, aer_row in aeronet_df.iterrows():
        aer_dt  = aer_row['datetime']          # UTC+7 (Vietnam local time)
        aer_aod = float(aer_row['aod_550'])

        files = _l2_files_in_window(aer_dt - _TZ, window_min)  # file names are UTC
        if not files:
            continue

        per_file_means: list[float] = []
        best_flag = COLLOCATE_SPATIAL_FLAG_5X5  # track the smallest neighbourhood used

        for fpath in files:
            try:
                with rasterio.open(str(fpath)) as src:
                    # Locate station in TIF coordinates via the file's own georeference
                    tif_row, tif_col = rasterio.transform.rowcol(
                        src.transform, station_lon, station_lat
                    )
                    if not (0 <= tif_row < src.height and 0 <= tif_col < src.width):
                        continue
                    aot_band = _read_band(src, 1)
                    H, W = src.height, src.width
            except Exception:
                continue

            result = _sample_grid_aod(aot_band, tif_row, tif_col, H, W)
            if result is None:
                continue
            mean_v, _std, _n, flag = result
            per_file_means.append(mean_v)
            if flag < best_flag:
                best_flag = flag

        if not per_file_means:
            continue

        records.append({
            'sensor':              'himawari_l2',
            'site':                site,
            'region':              AERONET_SITES[site]['region'],
            'season':              'dry' if aer_dt.month in DRY_MONTHS else 'wet',
            'month':               aer_dt.month,
            'lat':                 station_lat,
            'lon':                 station_lon,
            'date':                aer_dt.date().isoformat(),
            'timestamp_aeronet':   aer_dt.isoformat(),
            'timestamp_satellite': aer_dt.isoformat(),
            'satellite_aod':       float(np.mean(per_file_means)),
            'aeronet_aod':         aer_aod,
            'dist_km':             0.0,
            'vza':                 np.nan,
            'sza':                 np.nan,
            'spatial_flag':        best_flag,
            'box_std':             float(np.std(per_file_means, ddof=0)),
        })
    return records


def _collocate_himawari_l3(
    aeronet_df: pd.DataFrame,
    site: str,
    station_lat: float,
    station_lon: float,
) -> list[dict]:
    """Match Himawari L3 hourly composites to AERONET daily means.

    L3 is a daily composite product; temporal matching is whole-day.
    Spatial strategy: exact grid cell → 3×3 → 5×5 neighbourhood (cell indices).
    """
    records = []

    aeronet_daily = (aeronet_df.groupby(aeronet_df['datetime'].dt.date)
                                ['aod_550'].mean().reset_index())
    aeronet_daily.columns = ['date', 'aod_550']

    for date_obj in sorted(set(aeronet_df['datetime'].dt.date)):
        aer_aod_row = aeronet_daily[aeronet_daily['date'] == date_obj]
        if aer_aod_row.empty:
            continue
        aer_aod = float(aer_aod_row['aod_550'].values[0])

        # 05:00 UTC = 12:00 UTC+7 (Vietnam noon); keeps the same calendar date for
        # directory lookup while centering the ±12 h window on the UTC+7 day.
        day_dt = datetime(date_obj.year, date_obj.month, date_obj.day, 5, 0)
        files  = _l3_files_in_window(day_dt, window_min=720)  # whole-day window

        per_file_means: list[float] = []
        best_flag = COLLOCATE_SPATIAL_FLAG_5X5

        for fpath in files:
            try:
                with rasterio.open(str(fpath)) as src:
                    tif_row, tif_col = rasterio.transform.rowcol(
                        src.transform, station_lon, station_lat
                    )
                    if not (0 <= tif_row < src.height and 0 <= tif_col < src.width):
                        continue
                    band2 = _read_band(src, 2)   # AOT_Merged
                    H, W  = src.height, src.width
            except Exception:
                continue

            result = _sample_grid_aod(band2, tif_row, tif_col, H, W)
            if result is None:
                continue
            mean_v, _std, _n, flag = result
            per_file_means.append(mean_v)
            if flag < best_flag:
                best_flag = flag

        if not per_file_means:
            continue

        dt_repr = datetime(date_obj.year, date_obj.month, date_obj.day)
        records.append({
            'sensor':              'himawari_l3',
            'site':                site,
            'region':              AERONET_SITES[site]['region'],
            'season':              'dry' if date_obj.month in DRY_MONTHS else 'wet',
            'month':               date_obj.month,
            'lat':                 station_lat,
            'lon':                 station_lon,
            'date':                date_obj.isoformat(),
            'timestamp_aeronet':   dt_repr.isoformat(),
            'timestamp_satellite': dt_repr.isoformat(),
            'satellite_aod':       float(np.mean(per_file_means)),
            'aeronet_aod':         aer_aod,
            'dist_km':             0.0,
            'vza':                 np.nan,
            'sza':                 np.nan,
            'spatial_flag':        best_flag,
            'box_std':             float(np.std(per_file_means, ddof=0)),
        })
    return records


def _collocate_viirs(
    aeronet_df: pd.DataFrame,
    sensor: str,
    site: str,
    station_lat: float,
    station_lon: float,
    window_min: int = LEO_WINDOW_MIN,
) -> list[dict]:
    """Match VIIRS granules to individual AERONET observations.

    Spatial strategy: exact pixel → 3×3 → 5×5 neighbourhood, defined by
    distance thresholds based on the ~6 km aggregated VIIRS L2 pixel size
    (VIIRS_EXACT_MAX_KM / VIIRS_3X3_MAX_KM / VIIRS_5X5_MAX_KM).  All pixels
    from all granules within ±window_min are pooled per AERONET observation
    before neighbourhood sampling.
    """
    records = []

    for _, aer_row in aeronet_df.iterrows():
        aer_dt  = aer_row['datetime']          # UTC+7
        aer_aod = float(aer_row['aod_550'])

        files = _viirs_files_in_window(sensor, aer_dt - _TZ, window_min)  # file names are UTC
        if not files:
            continue

        # Pool all valid pixels from all granules in the time window
        all_lat, all_lon, all_aod, all_vza, all_sza = [], [], [], [], []
        sat_ts = ''
        for fpath in files:
            px = _read_viirs_file(fpath)
            if px is None:
                continue
            all_lat.append(px['lat'])
            all_lon.append(px['lon'])
            all_aod.append(px['aod'])
            all_vza.append(px['vza'])
            all_sza.append(px['sza'])
            if not sat_ts:
                fdt = _parse_viirs_utc(fpath.name) if hasattr(fpath, 'name') else None
                if fdt:
                    sat_ts = (fdt + _TZ).isoformat()  # store as UTC+7

        if not all_lat:
            continue

        lat_arr = np.concatenate(all_lat)
        lon_arr = np.concatenate(all_lon)
        aod_arr = np.concatenate(all_aod)
        vza_arr = np.concatenate(all_vza)
        sza_arr = np.concatenate(all_sza)

        result = _sample_swath_aod(
            lat_arr, lon_arr, aod_arr,
            station_lat, station_lon,
            VIIRS_EXACT_MAX_KM, VIIRS_3X3_MAX_KM, VIIRS_5X5_MAX_KM,
        )
        if result is None:
            continue
        mean_aod, box_std, mean_dist, _n, flag = result

        # Mean VZA/SZA of contributing pixels
        dist_arr = _haversine_km(station_lat, station_lon, lat_arr, lon_arr)
        max_km = (VIIRS_EXACT_MAX_KM, VIIRS_3X3_MAX_KM, VIIRS_5X5_MAX_KM)[flag]
        hood   = dist_arr <= max_km
        mean_vza = float(np.nanmean(vza_arr[hood])) if np.any(hood) else np.nan
        mean_sza = float(np.nanmean(sza_arr[hood])) if np.any(hood) else np.nan

        records.append({
            'sensor':              sensor,
            'site':                site,
            'region':              AERONET_SITES[site]['region'],
            'season':              'dry' if aer_dt.month in DRY_MONTHS else 'wet',
            'month':               aer_dt.month,
            'lat':                 station_lat,
            'lon':                 station_lon,
            'date':                aer_dt.date().isoformat(),
            'timestamp_aeronet':   aer_dt.isoformat(),
            'timestamp_satellite': sat_ts,
            'satellite_aod':       mean_aod,
            'aeronet_aod':         aer_aod,
            'dist_km':             mean_dist,
            'vza':                 mean_vza,
            'sza':                 mean_sza,
            'spatial_flag':        flag,
            'box_std':             box_std,
        })
    return records


def _collocate_modis(
    aeronet_df: pd.DataFrame,
    site: str,
    station_lat: float,
    station_lon: float,
) -> list[dict]:
    """Match MODIS MAIAC orbits to individual AERONET observations.

    Temporal strategy: for each AERONET observation, find MCD19A2 orbit layers
    whose UTC overpass time (read from the HDF ``Orbit_time_stamp`` attribute)
    falls within ±MODIS_WINDOW_MIN.  If orbit times cannot be parsed, all orbits
    from the daily file are used as a fallback.

    Spatial strategy: exact MAIAC pixel → 3×3 → 5×5 neighbourhood, defined by
    distance thresholds matched to the 1 km MAIAC native grid
    (MODIS_EXACT_MAX_KM / MODIS_3X3_MAX_KM / MODIS_5X5_MAX_KM).
    """
    records = []

    for _, aer_row in aeronet_df.iterrows():
        aer_dt  = aer_row['datetime']          # UTC+7
        aer_aod = float(aer_row['aod_550'])
        aer_dt_utc = aer_dt - _TZ             # UTC — used for all satellite lookups

        files = _modis_files_for_date(aer_dt_utc)  # directory structure uses UTC
        if not files:
            continue

        # Collect pixels from all tiles, filtering to matching orbits
        all_lat, all_lon, all_aod, all_vza, all_sza = [], [], [], [], []
        sat_ts = ''

        for fpath in files:
            orbit_times = _read_modis_orbit_times(str(fpath))  # returns UTC datetimes

            if orbit_times:
                matching = [
                    i for i, t in enumerate(orbit_times)
                    if abs((t - aer_dt_utc).total_seconds()) <= MODIS_WINDOW_MIN * 60
                ]
                if not matching:
                    continue
                if not sat_ts and matching:
                    sat_ts = (orbit_times[matching[0]] + _TZ).isoformat()  # store as UTC+7
                orb_filter = matching
            else:
                # Orbit times unavailable — use all orbits for this day
                orb_filter = None
                if not sat_ts:
                    sat_ts = aer_dt.date().isoformat()  # UTC+7 date

            px = _read_modis_tile(str(fpath), orbit_indices=orb_filter)
            if px is None:
                continue
            all_lat.append(px['lat'])
            all_lon.append(px['lon'])
            all_aod.append(px['aod'])
            all_vza.append(px['vza'])
            all_sza.append(px['sza'])

        if not all_lat:
            continue

        lat_arr = np.concatenate(all_lat)
        lon_arr = np.concatenate(all_lon)
        aod_arr = np.concatenate(all_aod)
        vza_arr = np.concatenate(all_vza)
        sza_arr = np.concatenate(all_sza)

        result = _sample_swath_aod(
            lat_arr, lon_arr, aod_arr,
            station_lat, station_lon,
            MODIS_EXACT_MAX_KM, MODIS_3X3_MAX_KM, MODIS_5X5_MAX_KM,
        )
        if result is None:
            continue
        mean_aod, box_std, mean_dist, _n, flag = result

        dist_arr = _haversine_km(station_lat, station_lon, lat_arr, lon_arr)
        max_km   = (MODIS_EXACT_MAX_KM, MODIS_3X3_MAX_KM, MODIS_5X5_MAX_KM)[flag]
        hood     = dist_arr <= max_km
        mean_vza = float(np.nanmean(vza_arr[hood])) if np.any(hood) else np.nan
        mean_sza = float(np.nanmean(sza_arr[hood])) if np.any(hood) else np.nan

        records.append({
            'sensor':              'modis_maiac',
            'site':                site,
            'region':              AERONET_SITES[site]['region'],
            'season':              'dry' if aer_dt.month in DRY_MONTHS else 'wet',
            'month':               aer_dt.month,
            'lat':                 station_lat,
            'lon':                 station_lon,
            'date':                aer_dt.date().isoformat(),
            'timestamp_aeronet':   aer_dt.isoformat(),
            'timestamp_satellite': sat_ts,
            'satellite_aod':       mean_aod,
            'aeronet_aod':         aer_aod,
            'dist_km':             mean_dist,
            'vza':                 mean_vza,
            'sza':                 mean_sza,
            'spatial_flag':        flag,
            'box_std':             box_std,
        })
    return records


# ── Main colocation entry point ───────────────────────────────────────────────

def collocate_site(
    site: str,
    start_date: date,
    end_date:   date,
    sensors: tuple[str, ...] = (
        'himawari_l2', 'himawari_l3',
        'viirs_snpp', 'viirs_noaa20',
        'modis_maiac',
    ),
    output_dir: Path | str = COLLOCATE_DIR,
) -> dict[str, pd.DataFrame]:
    """Run colocation for one AERONET site over a date range.

    Returns a dict mapping sensor → DataFrame of matched pairs.
    Also writes/appends per-sensor CSVs to output_dir.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    meta = AERONET_SITES[site]
    station_lat = meta['lat']
    station_lon = meta['lon']

    # Load AERONET data for the requested period
    aer_all = load_aeronet(site)
    aer_all = aer_all[
        (aer_all['datetime'].dt.date >= start_date)
        & (aer_all['datetime'].dt.date <= end_date)
    ].copy()

    if aer_all.empty:
        print(f'  [{site}] No AERONET data in range.')
        return {}

    results: dict[str, pd.DataFrame] = {}

    sensor_bar = _make_bar(list(sensors), desc=f'  {site} sensors', unit='sensor',
                           ncols=72, leave=False)
    for sensor in (sensor_bar if sensor_bar is not None else sensors):
        records: list[dict] = []

        if sensor == 'himawari_l2':
            records = _collocate_himawari_l2(aer_all, site, station_lat, station_lon)
        elif sensor == 'himawari_l3':
            records = _collocate_himawari_l3(aer_all, site, station_lat, station_lon)
        elif sensor in ('viirs_snpp', 'viirs_noaa20'):
            records = _collocate_viirs(aer_all, sensor, site, station_lat, station_lon)
        elif sensor == 'modis_maiac':
            records = _collocate_modis(aer_all, site, station_lat, station_lon)

        if not records:
            continue

        df = pd.DataFrame(records)
        csv_path = output_dir / f'{sensor}_{site}.csv'

        if csv_path.exists():
            existing = pd.read_csv(csv_path)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(
                subset=['timestamp_aeronet', 'timestamp_satellite'],
                keep='last',
            )

        df.to_csv(csv_path, index=False)
        results[sensor] = df
        print(f'  [{site}] {sensor}: {len(records)} new pairs  (total {len(df)})')

    return results
