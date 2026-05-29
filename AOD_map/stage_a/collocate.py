"""Extract satellite AOD at AERONET station coordinates and build matched pairs.

For each AERONET observation, find the satellite pixel that contains the
station's coordinates (nearest-pixel, no spatial fallback) and record the
pair (satellite_aod, aeronet_aod) together with metadata needed for
bias-correction stratum assignment (sensor, region, season, month, lat, lon).

Collocation strategy per sensor:
    Himawari L2 : already on 0.05° grid → read pixel at station lat/lon.
                  Match: all 10-min files within ±30 min of AERONET observation.
    Himawari L3 : same grid → read pixel.
                  Match: L3 hourly composite whose timestamp is within ±30 min.
    VIIRS       : L2 swath → find pixel with minimum distance to station (no ring).
                  Match: granules within ±30 min of AERONET observation.
    MODIS MAIAC : 1 km L2 → find pixel with minimum distance to station.
                  Match: any orbit in the MCD19A2 file for that date (daily file).

Output:
    One CSV per (sensor, site) at COLLOCATE_DIR / {sensor}_{site}.csv
    Columns: sensor, site, region, season, month, lat, lon, date,
             timestamp_aeronet, timestamp_satellite,
             satellite_aod, aeronet_aod, dist_km, vza, sza
"""

from __future__ import annotations
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional
import glob

import numpy as np
import pandas as pd
import rasterio

from config import (
    AERONET_SITES, AERONET_DIR, DRY_MONTHS,
    HIMAWARI_L2_DIR, HIMAWARI_L3_DIR,
    COLLOCATE_DIR, EARTH_RADIUS_KM,
    LATS, LONS, NLAT, NLON, GRID_RES, LAT_MAX, LON_MIN,
    LEO_WINDOW_MIN,
)
from aeronet   import load_aeronet, window_mean
from himawari  import (
    _l2_files_in_window, _l3_files_in_window,
    _parse_l2_utc, _parse_l3_utc, _read_band,
)
from viirs     import _viirs_files_in_window, _read_viirs_file
from modis     import _modis_files_for_date, _read_modis_tile


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


# ── Per-sensor collocators ────────────────────────────────────────────────────

def _collocate_himawari_l2(
    aeronet_df: pd.DataFrame,
    site: str,
    station_lat: float,
    station_lon: float,
    window_min: int = LEO_WINDOW_MIN,
) -> list[dict]:
    """Match Himawari L2 10-min files to AERONET observations.

    For each AERONET time, gather all L2 files within ±window_min, read Band 1
    (AOT) at the station's grid cell, and average them.  A record is only kept
    when the pixel is non-NaN (i.e. a valid retrieval exists at that cell).
    """
    records = []
    rc = _station_pixel(station_lat, station_lon)
    if rc is None:
        return records
    row, col = rc

    # Group AERONET observations by slot to avoid redundant file reads
    for _, aer_row in aeronet_df.iterrows():
        aer_dt  = aer_row['datetime']
        aer_aod = float(aer_row['aod_550'])

        files = _l2_files_in_window(aer_dt, window_min)
        if not files:
            continue

        sat_vals = []
        for fpath in files:
            try:
                with rasterio.open(str(fpath)) as src:
                    aot_band = _read_band(src, 1)
                    val = float(aot_band[row, col])
                    if np.isfinite(val) and val >= 0:
                        sat_vals.append(val)
            except Exception:
                continue

        if not sat_vals:
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
            'timestamp_satellite': aer_dt.isoformat(),  # approximate (averaged window)
            'satellite_aod':       float(np.mean(sat_vals)),
            'aeronet_aod':         aer_aod,
            'dist_km':             0.0,   # station is inside the pixel
            'vza':                 np.nan,
            'sza':                 np.nan,
        })
    return records


def _collocate_himawari_l3(
    aeronet_df: pd.DataFrame,
    site: str,
    station_lat: float,
    station_lon: float,
    window_min: int = LEO_WINDOW_MIN,
) -> list[dict]:
    """Match Himawari L3 hourly composites to AERONET daily means."""
    records = []
    rc = _station_pixel(station_lat, station_lon)
    if rc is None:
        return records
    row, col = rc

    # L3 is an hourly composite; match by daily mean for the plan's validation
    aeronet_daily = (aeronet_df.groupby(aeronet_df['datetime'].dt.date)
                                ['aod_550'].mean().reset_index())
    aeronet_daily.columns = ['date', 'aod_550']

    for date_obj in sorted(set(aeronet_df['datetime'].dt.date)):
        aer_aod_row = aeronet_daily[aeronet_daily['date'] == date_obj]
        if aer_aod_row.empty:
            continue
        aer_aod = float(aer_aod_row['aod_550'].values[0])

        # Gather all L3 files for that day
        day_dt = datetime(date_obj.year, date_obj.month, date_obj.day, 6, 0)
        files = _l3_files_in_window(day_dt, window_min=720)  # whole day window

        sat_vals = []
        for fpath in files:
            try:
                with rasterio.open(str(fpath)) as src:
                    band2 = _read_band(src, 2)   # AOT_Merged
                    val = float(band2[row, col])
                    if np.isfinite(val) and val >= 0:
                        sat_vals.append(val)
            except Exception:
                continue

        if not sat_vals:
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
            'satellite_aod':       float(np.mean(sat_vals)),
            'aeronet_aod':         aer_aod,
            'dist_km':             0.0,
            'vza':                 np.nan,
            'sza':                 np.nan,
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
    """Match VIIRS granules to AERONET observations.

    Nearest-pixel strategy: find the pixel in the granule with smallest
    haversine distance to the station.  No ring search — if the nearest pixel
    is invalid, the observation is discarded.
    """
    records = []
    from viirs import _SENSOR_DIRS, _viirs_files_for_date

    for _, aer_row in aeronet_df.iterrows():
        aer_dt  = aer_row['datetime']
        aer_aod = float(aer_row['aod_550'])

        files = _viirs_files_in_window(sensor, aer_dt, window_min)
        for fpath in files:
            px = _read_viirs_file(fpath)
            if px is None:
                continue

            dist = _haversine_km(station_lat, station_lon, px['lat'], px['lon'])
            idx  = int(np.argmin(dist))
            nearest_dist = float(dist[idx])

            # Accept only if nearest pixel is within ~25 km (≈ 4 grid cells)
            if nearest_dist > 25.0:
                continue

            sat_aod = float(px['aod'][idx])
            if not np.isfinite(sat_aod) or sat_aod < 0:
                continue

            fdt = _parse_viirs_utc(fpath.name) if hasattr(fpath, 'name') else None

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
                'timestamp_satellite': fdt.isoformat() if fdt else '',
                'satellite_aod':       sat_aod,
                'aeronet_aod':         aer_aod,
                'dist_km':             nearest_dist,
                'vza':                 float(px['vza'][idx]),
                'sza':                 float(px['sza'][idx]),
            })
    return records


def _collocate_modis(
    aeronet_df: pd.DataFrame,
    site: str,
    station_lat: float,
    station_lon: float,
) -> list[dict]:
    """Match MODIS MAIAC to AERONET daily means (one value per orbit per day)."""
    records = []

    # Aggregate AERONET to daily mean for MODIS match (daily overpass)
    aeronet_daily = (aeronet_df.groupby(aeronet_df['datetime'].dt.date)
                                ['aod_550'].mean().reset_index())
    aeronet_daily.columns = ['date', 'aod_550']

    for date_obj in sorted(set(aeronet_df['datetime'].dt.date)):
        aer_row = aeronet_daily[aeronet_daily['date'] == date_obj]
        if aer_row.empty:
            continue
        aer_aod = float(aer_row['aod_550'].values[0])

        dt = datetime(date_obj.year, date_obj.month, date_obj.day)
        files = _modis_files_for_date(dt)
        if not files:
            continue

        sat_vals = []
        for fpath in files:
            px = _read_modis_tile(str(fpath))
            if px is None:
                continue
            dist = _haversine_km(station_lat, station_lon, px['lat'], px['lon'])
            idx  = int(np.argmin(dist))
            if dist[idx] > 1.5:   # 1 km pixel, accept ≤ 1.5 km
                continue
            val = float(px['aod'][idx])
            if np.isfinite(val) and val >= 0:
                sat_vals.append(val)

        if not sat_vals:
            continue

        records.append({
            'sensor':              'modis_maiac',
            'site':                site,
            'region':              AERONET_SITES[site]['region'],
            'season':              'dry' if date_obj.month in DRY_MONTHS else 'wet',
            'month':               date_obj.month,
            'lat':                 station_lat,
            'lon':                 station_lon,
            'date':                date_obj.isoformat(),
            'timestamp_aeronet':   dt.isoformat(),
            'timestamp_satellite': dt.isoformat(),
            'satellite_aod':       float(np.mean(sat_vals)),
            'aeronet_aod':         aer_aod,
            'dist_km':             float(np.min([
                _haversine_km(station_lat, station_lon,
                              _read_modis_tile(str(f))['lat'],
                              _read_modis_tile(str(f))['lon']).min()
                for f in files
                if _read_modis_tile(str(f)) is not None
            ], default=np.nan)),
            'vza':                 np.nan,
            'sza':                 np.nan,
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

    for sensor in sensors:
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
