"""Extract satellite AOD time series at AERONET station coordinates.

For each sensor and station, scans all satellite files over a date range and
extracts the AOD at the station location using the tiered spatial strategy
(exact pixel / 3×3 / 5×5 neighbourhood, with within-box std rejection).
The result is a per-(sensor, site) raw CSV that is independent of AERONET and
can be matched against any AERONET period without re-reading satellite files.

Output CSV schema (one row per satellite snapshot):
    timestamp_sat  — UTC ISO-8601 string; empty for MODIS orbit-time fallback
    sat_aod        — neighbourhood-mean AOD at the station
    box_std        — within-neighbourhood AOD std (scene heterogeneity)
    spatial_flag   — 0 = exact pixel/cell, 1 = 3×3, 2 = 5×5
    dist_km        — mean pixel-to-station distance (0.0 for gridded sensors)
    vza            — mean viewing zenith angle (NaN for Himawari)
    sza            — mean solar zenith angle (NaN for Himawari)
    is_fallback    — True only when MODIS orbit timestamps are unavailable
                     (whole-day orbit used; match step treats as full-day)
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
    AERONET_SITES,
    HIMAWARI_L2_DIR, HIMAWARI_L3_DIR,
    EXTRACT_DIR,
    EARTH_RADIUS_KM,
    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
    COLLOCATE_SPATIAL_FLAG_EXACT, COLLOCATE_SPATIAL_FLAG_3X3, COLLOCATE_SPATIAL_FLAG_5X5,
    BOX_STD_ABS_FLOOR, BOX_STD_SLOPE,
    VIIRS_EXACT_MAX_KM, VIIRS_3X3_MAX_KM, VIIRS_5X5_MAX_KM,
    VIIRS_OVERPASS_GAP_MIN,
)
from himawari import _parse_l2_utc, _parse_l3_utc, _read_band, _wavelength_correct
from viirs    import _viirs_files_for_date, _parse_viirs_utc, _read_viirs_file
from modis    import _modis_files_for_date, _tile_station_rowcol, _read_modis_tile_aod2d


# ── Haversine distance ────────────────────────────────────────────────────────

def _haversine_km(lat1: float, lon1: float,
                  lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    R = EARTH_RADIUS_KM
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (np.sin(dlat / 2) ** 2
         + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2) ** 2)
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


# ── Spatial-sampling helpers ──────────────────────────────────────────────────

def _box_std_max(mean_aod: float, sensor: str) -> float:
    return max(BOX_STD_ABS_FLOOR, BOX_STD_SLOPE.get(sensor, 0.15) * mean_aod)


_GRID_TIERS = (
    (0, COLLOCATE_SPATIAL_FLAG_EXACT),
    (1, COLLOCATE_SPATIAL_FLAG_3X3),
    (2, COLLOCATE_SPATIAL_FLAG_5X5),
)


def _sample_grid_aod(
    aod_2d: np.ndarray,
    row: int, col: int,
    nrows: int, ncols: int,
    sensor: str,
) -> 'tuple[float, float, int, int] | None':
    """Sample a 2-D AOD grid at (row, col) with tiered neighbourhood fallback.

    Returns (mean, std, n_valid, spatial_flag) for the first tier with data
    that passes the within-box heterogeneity threshold, or None.
    """
    for hw, flag in _GRID_TIERS:
        r0 = max(0, row - hw);  r1 = min(nrows, row + hw + 1)
        c0 = max(0, col - hw);  c1 = min(ncols, col + hw + 1)
        vals = aod_2d[r0:r1, c0:c1].ravel()
        vals = vals[np.isfinite(vals) & (vals >= 0) & (vals <= 5.0)]
        if len(vals) == 0:
            continue
        mean_v = float(np.mean(vals))
        std_v  = float(np.std(vals, ddof=0))
        if std_v > _box_std_max(mean_v, sensor):
            return None
        return mean_v, std_v, int(len(vals)), flag
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
    sensor: str,
    *,
    dist: 'np.ndarray | None' = None,
) -> 'tuple[float, float, float, int, int] | None':
    """Sample ungridded swath pixels near a station with tiered neighbourhood fallback.

    Returns (mean_aod, std_aod, mean_dist_km, n_valid, spatial_flag) or None.
    """
    if dist is None:
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
        mean_v = float(np.mean(vals))
        std_v  = float(np.std(vals, ddof=0))
        if std_v > _box_std_max(mean_v, sensor):
            return None
        return mean_v, std_v, float(np.mean(dist[mask])), int(len(vals)), flag
    return None


# ── Date iteration ────────────────────────────────────────────────────────────

def _date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


# ── Per-sensor extractors ─────────────────────────────────────────────────────

def _extract_himawari_l2(
    station_lat: float,
    station_lon: float,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """One row per 10-min L2 TIF file that has a valid retrieval at the station."""
    records = []
    for d in _date_range(start_date, end_date):
        pattern = str(
            HIMAWARI_L2_DIR / d.strftime('%Y%m') / d.strftime('%d') / '*' /
            f'aod_vietnam_NC_H0?_{d.strftime("%Y%m%d")}_????_L2ARP031_FLDK.*.tif'
        )
        for fpath in sorted(Path(p) for p in glob.glob(pattern)):
            ts = _parse_l2_utc(fpath.name)
            if ts is None:
                continue
            try:
                with rasterio.open(str(fpath)) as src:
                    tif_row, tif_col = rasterio.transform.rowcol(
                        src.transform, station_lon, station_lat
                    )
                    if not (0 <= tif_row < src.height and 0 <= tif_col < src.width):
                        continue
                    aot = _read_band(src, 1)   # 500 nm
                    ae  = _read_band(src, 3)   # Ångström exponent
                    H, W = src.height, src.width
            except Exception:
                continue

            # 500 → 550 nm Ångström interpolation; drops AE-missing pixels
            aot = _wavelength_correct(aot, ae)

            result = _sample_grid_aod(aot, tif_row, tif_col, H, W, sensor='himawari_l2')
            if result is None:
                continue
            mean_v, std_v, _n, flag = result
            records.append({
                'timestamp_sat': ts.isoformat(),
                'sat_aod':       mean_v,
                'box_std':       std_v,
                'spatial_flag':  flag,
                'dist_km':       0.0,
                'vza':           np.nan,
                'sza':           np.nan,
                'is_fallback':   False,
            })
    return records


def _extract_himawari_l3(
    station_lat: float,
    station_lon: float,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """One row per hourly L3 composite TIF that has a valid retrieval at the station."""
    records = []
    for d in _date_range(start_date, end_date):
        pattern = str(
            HIMAWARI_L3_DIR / d.strftime('%Y%m') / d.strftime('%d') /
            f'aod_vietnam_H0?_{d.strftime("%Y%m%d")}_????_1HARP031_FLDK.*.tif'
        )
        for fpath in sorted(Path(p) for p in glob.glob(pattern)):
            ts = _parse_l3_utc(fpath.name)
            if ts is None:
                continue
            try:
                with rasterio.open(str(fpath)) as src:
                    tif_row, tif_col = rasterio.transform.rowcol(
                        src.transform, station_lon, station_lat
                    )
                    if not (0 <= tif_row < src.height and 0 <= tif_col < src.width):
                        continue
                    band2   = _read_band(src, 2)   # AOT_Merged   (500 nm)
                    band10  = _read_band(src, 10)  # AOT_L2_Mean  (500 nm fallback)
                    band6   = _read_band(src, 6)   # AE_Merged
                    band13  = _read_band(src, 13)  # AE_L2_Mean   (AE fallback)
                    aot     = np.where(~np.isnan(band2), band2, band10)
                    ae      = np.where(~np.isnan(band6), band6, band13)
                    H, W    = src.height, src.width
            except Exception:
                continue

            # 500 → 550 nm Ångström interpolation; drops AE-missing pixels
            aot = _wavelength_correct(aot, ae)

            result = _sample_grid_aod(aot, tif_row, tif_col, H, W, sensor='himawari_l3')
            if result is None:
                continue
            mean_v, std_v, _n, flag = result
            records.append({
                'timestamp_sat': ts.isoformat(),
                'sat_aod':       mean_v,
                'box_std':       std_v,
                'spatial_flag':  flag,
                'dist_km':       0.0,
                'vza':           np.nan,
                'sza':           np.nan,
                'is_fallback':   False,
            })
    return records


def _group_viirs_overpasses(
    files: list[Path],
) -> list[tuple[datetime, list[Path]]]:
    """Group consecutive VIIRS granules into per-overpass file lists.

    Files whose UTC timestamps are within VIIRS_OVERPASS_GAP_MIN of each other
    are considered the same overpass and pooled for spatial extraction.
    Returns a list of (overpass_utc, files) sorted by overpass_utc.
    """
    timed = [(f, _parse_viirs_utc(f.name)) for f in files]
    timed = sorted(((f, t) for f, t in timed if t is not None), key=lambda x: x[1])
    if not timed:
        return []

    groups: list[tuple[datetime, list[Path]]] = []
    cur_ts, cur_files = timed[0][1], [timed[0][0]]
    for f, t in timed[1:]:
        if (t - cur_ts).total_seconds() / 60 <= VIIRS_OVERPASS_GAP_MIN:
            cur_files.append(f)
        else:
            groups.append((cur_ts, cur_files))
            cur_ts, cur_files = t, [f]
    groups.append((cur_ts, cur_files))
    return groups


def _extract_viirs(
    sensor: str,
    station_lat: float,
    station_lon: float,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """One row per VIIRS overpass (grouped consecutive granules) at the station.

    Granules within VIIRS_OVERPASS_GAP_MIN of each other are pooled into one
    pixel set before spatial extraction — preserving the same pooling behaviour
    as iterating over all files within a ±window of a given timestamp.
    """
    records = []
    for d in _date_range(start_date, end_date):
        dt = datetime(d.year, d.month, d.day)
        files = _viirs_files_for_date(sensor, dt)
        # Include adjacent-day files to handle midnight-boundary overpasses
        if d > start_date:
            prev_dt = dt - timedelta(days=1)
            for f in _viirs_files_for_date(sensor, prev_dt):
                t = _parse_viirs_utc(f.name)
                if t and t >= dt - timedelta(minutes=VIIRS_OVERPASS_GAP_MIN):
                    files.append(f)
        if not files:
            continue

        for overpass_utc, group_files in _group_viirs_overpasses(files):
            # Only yield overpasses whose timestamp belongs to this date
            # (avoids duplicate rows at the day boundary)
            if overpass_utc.date() != d:
                continue

            all_lat, all_lon, all_aod, all_vza, all_sza = [], [], [], [], []
            for fpath in group_files:
                px = _read_viirs_file(fpath)
                if px is None:
                    continue
                # Pre-clip to Vietnam domain before haversine to avoid
                # computing distances for the entire global swath (~2M pixels).
                bbox_mask = (
                    (px['lat'] >= LAT_MIN) & (px['lat'] <= LAT_MAX)
                    & (px['lon'] >= LON_MIN) & (px['lon'] <= LON_MAX)
                )
                if not np.any(bbox_mask):
                    continue
                px = {k: v[bbox_mask] for k, v in px.items()}
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

            dist_arr = _haversine_km(station_lat, station_lon, lat_arr, lon_arr)
            result = _sample_swath_aod(
                lat_arr, lon_arr, aod_arr,
                station_lat, station_lon,
                VIIRS_EXACT_MAX_KM, VIIRS_3X3_MAX_KM, VIIRS_5X5_MAX_KM,
                sensor=sensor,
                dist=dist_arr,
            )
            if result is None:
                continue
            mean_aod, box_std, mean_dist, _n, flag = result

            hood = dist_arr <= (VIIRS_EXACT_MAX_KM, VIIRS_3X3_MAX_KM, VIIRS_5X5_MAX_KM)[flag]
            mean_vza = float(np.nanmean(vza_arr[hood])) if np.any(hood) else np.nan
            mean_sza = float(np.nanmean(sza_arr[hood])) if np.any(hood) else np.nan

            records.append({
                'timestamp_sat': overpass_utc.isoformat(),
                'sat_aod':       mean_aod,
                'box_std':       box_std,
                'spatial_flag':  flag,
                'dist_km':       mean_dist,
                'vza':           mean_vza,
                'sza':           mean_sza,
                'is_fallback':   False,
            })
    return records


def _extract_modis(
    station_lat: float,
    station_lon: float,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """One row per MODIS orbit at the station using grid-based neighbourhood sampling.

    MAIAC is a 1 km sinusoidal grid — the station's row/col is looked up via
    the tile's rasterio transform and _sample_grid_aod is applied on the 2D
    QA-filtered AOD array, exactly like Himawari.  When the station is near a
    tile boundary the neighbourhood is clipped to the tile edge (acceptable:
    Vietnam stations are never closer than 5 pixels to a tile boundary).

    When orbit timestamps are unavailable, all orbits for the day are pooled
    into a single fallback row (is_fallback=True); the match step uses
    date-based rather than time-window matching for those rows.

    dist_km is 0.0 for all rows (station is inside its grid cell by definition).
    """
    records = []
    for d in _date_range(start_date, end_date):
        dt = datetime(d.year, d.month, d.day)
        files = _modis_files_for_date(dt)
        if not files:
            continue

        # orbit_key → {aod_2d, row, col, H, W, vza, sza, ts, fallback}
        orbit_pools: dict[str, dict] = {}

        for fpath in files:
            rc = _tile_station_rowcol(str(fpath), station_lat, station_lon)
            if rc is None:
                continue  # station not in this tile
            row, col, H, W = rc

            tile_data = _read_modis_tile_aod2d(str(fpath))
            if tile_data is None:
                continue
            orbit_times, aod_2d_list, angle_list = tile_data

            for orb_idx, (ts, aod_2d) in enumerate(zip(orbit_times, aod_2d_list)):
                is_fb = (ts is None)
                key   = ts.isoformat() if ts else f'fallback_{d.isoformat()}'
                vza_2d, sza_2d = angle_list[orb_idx]
                vza_val = float(vza_2d[row, col]) if np.isfinite(vza_2d[row, col]) else np.nan
                sza_val = float(sza_2d[row, col]) if np.isfinite(sza_2d[row, col]) else np.nan

                if key not in orbit_pools:
                    orbit_pools[key] = {
                        'aod_2d':  aod_2d.copy(),
                        'row': row, 'col': col, 'H': H, 'W': W,
                        'vza': vza_val, 'sza': sza_val,
                        'ts': ts, 'fallback': is_fb,
                    }
                else:
                    # Merge overlapping tiles: first non-NaN value wins per pixel
                    existing = orbit_pools[key]['aod_2d']
                    orbit_pools[key]['aod_2d'] = np.where(np.isnan(existing), aod_2d, existing)
                    if np.isnan(orbit_pools[key]['vza']):
                        orbit_pools[key]['vza'] = vza_val
                    if np.isnan(orbit_pools[key]['sza']):
                        orbit_pools[key]['sza'] = sza_val

        for pool in orbit_pools.values():
            result = _sample_grid_aod(
                pool['aod_2d'], pool['row'], pool['col'],
                pool['H'], pool['W'], sensor='modis_maiac',
            )
            if result is None:
                continue
            mean_aod, box_std, _n, flag = result
            ts_str = pool['ts'].isoformat() if pool['ts'] else ''

            records.append({
                'timestamp_sat': ts_str,
                'utc_date':      d.isoformat() if pool['fallback'] else '',
                'sat_aod':       mean_aod,
                'box_std':       box_std,
                'spatial_flag':  flag,
                'dist_km':       0.0,
                'vza':           pool['vza'],
                'sza':           pool['sza'],
                'is_fallback':   pool['fallback'],
            })
    return records


# ── Main extraction entry point ───────────────────────────────────────────────

def extract_site(
    site: str,
    start_date: date,
    end_date: date,
    sensors: tuple[str, ...] = (
        'himawari_l2', 'himawari_l3',
        'viirs_snpp', 'viirs_noaa20',
        'modis_maiac',
    ),
    output_dir: Path | str = EXTRACT_DIR,
) -> dict[str, pd.DataFrame]:
    """Extract satellite AOD time series at one AERONET station over a date range.

    Reads satellite files, extracts AOD at the station coordinates, and writes
    one raw CSV per sensor to output_dir/{sensor}_{site}_raw.csv.

    New rows are appended and de-duplicated on timestamp_sat so the function
    is safe to re-run for overlapping date ranges.

    Returns a dict mapping sensor → DataFrame of all rows (old + new).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    meta        = AERONET_SITES[site]
    station_lat = meta['lat']
    station_lon = meta['lon']

    results: dict[str, pd.DataFrame] = {}

    sensor_bar = _make_bar(list(sensors), desc=f'  {site} extract',
                           unit='sensor', ncols=72, leave=False)
    for sensor in (sensor_bar if sensor_bar is not None else sensors):
        records: list[dict] = []

        if sensor == 'himawari_l2':
            records = _extract_himawari_l2(station_lat, station_lon, start_date, end_date)
        elif sensor == 'himawari_l3':
            records = _extract_himawari_l3(station_lat, station_lon, start_date, end_date)
        elif sensor in ('viirs_snpp', 'viirs_noaa20'):
            records = _extract_viirs(sensor, station_lat, station_lon, start_date, end_date)
        elif sensor == 'modis_maiac':
            records = _extract_modis(station_lat, station_lon, start_date, end_date)

        if not records:
            continue

        df = pd.DataFrame(records)
        csv_path = output_dir / f'{sensor}_{site}_raw.csv'

        if csv_path.exists():
            existing = pd.read_csv(csv_path)
            df = pd.concat([existing, df], ignore_index=True)
            non_fallback = df[~df['is_fallback'].fillna(False)]
            fallback     = df[df['is_fallback'].fillna(False)]
            non_fallback = non_fallback.drop_duplicates(subset=['timestamp_sat'], keep='last')
            if not fallback.empty and 'utc_date' in fallback.columns:
                fallback = fallback.drop_duplicates(subset=['utc_date'], keep='last')
            df = pd.concat([non_fallback, fallback], ignore_index=True)

        df.to_csv(csv_path, index=False)
        results[sensor] = df
        print(f'  [{site}] {sensor}: {len(records)} snapshots  (total {len(df)})')

    return results
