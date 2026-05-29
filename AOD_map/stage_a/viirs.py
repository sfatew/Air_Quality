"""Read VIIRS SNPP / NOAA-20 Deep Blue L2 NetCDF and apply Step A1 QA filters.

Deep Blue L2 variables used:
    Aerosol_Optical_Thickness_550_Land_Ocean_Best_Estimate  – primary AOD (550 nm)
    Aerosol_Optical_Thickness_QA_Flag_Land                  – 0=none, 1=poor, 2=moderate, 3=good
    Aerosol_Optical_Thickness_QA_Flag_Ocean                 – same scale
    Viewing_Zenith_Angle
    Solar_Zenith_Angle
    Latitude / Longitude

File naming:
    AERDB_L2_VIIRS_{SNPP|NOAA20}.A{YYYY}{DOY}.{HHMM}.{ver}.{proc}.nc
"""

from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import glob
from typing import Optional

import numpy as np
import netCDF4 as nc

from config import (
    VIIRS_SNPP_DIR, VIIRS_N20_DIR,
    VIIRS_QA_MIN_LAND, VIIRS_QA_MIN_OCEAN,
    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
    LEO_WINDOW_MIN,
)

# Sensor key → data directory mapping
_SENSOR_DIRS = {
    'viirs_snpp':   VIIRS_SNPP_DIR,
    'viirs_noaa20': VIIRS_N20_DIR,
}

# Fill value used in all VIIRS Deep Blue variables
_FILL = -999.0


def _doy_from_datetime(dt: datetime) -> str:
    return f'{dt.timetuple().tm_yday:03d}'


def _parse_viirs_utc(fname: str) -> Optional[datetime]:
    """Parse UTC overpass time from VIIRS filename.

    Example: AERDB_L2_VIIRS_NOAA20.A2022239.0636.002.2023080174927.nc
    Fields:  .A{YYYY}{DOY}.{HHMM}.
    """
    parts = fname.split('.')
    if len(parts) < 3:
        return None
    try:
        date_part = parts[1]          # A2022239
        time_part = parts[2]          # 0636
        year = int(date_part[1:5])
        doy  = int(date_part[5:8])
        hour = int(time_part[:2])
        minute = int(time_part[2:4])
        return (datetime(year, 1, 1)
                + timedelta(days=doy - 1, hours=hour, minutes=minute))
    except (ValueError, IndexError):
        return None


def _viirs_files_for_date(sensor: str, date: datetime) -> list[Path]:
    """Return all VIIRS files for the given date (day-of-year directory)."""
    base = _SENSOR_DIRS[sensor]
    year = date.strftime('%Y')
    doy  = _doy_from_datetime(date)
    pattern = str(base / year / doy / f'AERDB_L2_VIIRS_*.A{year}{doy}.*.nc')
    return sorted(Path(p) for p in glob.glob(pattern))


def _viirs_files_in_window(
    sensor: str,
    slot_utc: datetime,
    window_min: int = LEO_WINDOW_MIN,
) -> list[Path]:
    """Return VIIRS files whose overpass time falls within ±window_min of slot_utc."""
    candidates = _viirs_files_for_date(sensor, slot_utc)
    # Also check adjacent day if slot is near midnight
    if slot_utc.hour == 0:
        candidates += _viirs_files_for_date(sensor, slot_utc - timedelta(days=1))
    result = []
    for p in candidates:
        t = _parse_viirs_utc(p.name)
        if t and abs((t - slot_utc).total_seconds()) <= window_min * 60:
            result.append(p)
    return result


def _read_viirs_file(fpath: Path) -> Optional[dict[str, np.ndarray]]:
    """Read one VIIRS Deep Blue L2 file, returning ungridded pixel arrays.

    Returns a dict with flat 1-D arrays (valid pixels only) or None on error:
        lat, lon       – pixel centre coordinates (degrees)
        aod            – AOD at 550 nm
        vza, sza       – angles (degrees)
        qa             – combined QA flag (max of land and ocean flag)
    """
    try:
        with nc.Dataset(fpath) as ds:
            aod_raw = ds.variables['Aerosol_Optical_Thickness_550_Land_Ocean_Best_Estimate'][:]
            qa_land  = ds.variables['Aerosol_Optical_Thickness_QA_Flag_Land'][:]
            qa_ocean = ds.variables['Aerosol_Optical_Thickness_QA_Flag_Ocean'][:]
            vza      = ds.variables['Viewing_Zenith_Angle'][:]
            sza      = ds.variables['Solar_Zenith_Angle'][:]
            lat      = ds.variables['Latitude'][:]
            lon      = ds.variables['Longitude'][:]
    except Exception:
        return None

    # Convert masked arrays to plain numpy (fill → NaN / 0)
    def _unpack(arr, fill=_FILL):
        if hasattr(arr, 'filled'):
            arr = arr.filled(fill)
        return np.array(arr, dtype=np.float32)

    aod = _unpack(aod_raw)
    lat = _unpack(lat)
    lon = _unpack(lon)
    vza = _unpack(vza)
    sza = _unpack(sza)
    qa_land  = np.array(qa_land,  dtype=np.int8)
    qa_ocean = np.array(qa_ocean, dtype=np.int8)

    # Replace fill with 0 for QA arrays (fill_value=0 per file attributes means "no retrieval")
    qa_land[qa_land < 0]  = 0
    qa_ocean[qa_ocean < 0] = 0

    # Combined QA: take the higher of land or ocean flag
    qa_combined = np.maximum(qa_land, qa_ocean)

    # Step A1 QA filter
    valid = (
        (aod != _FILL)
        & ~np.isnan(aod)
        & (aod >= 0.0)
        & (aod <= 5.0)
        & (
            (qa_land  >= VIIRS_QA_MIN_LAND)
            | (qa_ocean >= VIIRS_QA_MIN_OCEAN)
        )
        & (~np.isnan(lat)) & (~np.isnan(lon))
        & (lat >= -90) & (lat <= 90)
        & (lon >= -180) & (lon <= 180)
    )

    if not np.any(valid):
        return None

    return {
        'lat': lat[valid].ravel(),
        'lon': lon[valid].ravel(),
        'aod': aod[valid].ravel(),
        'vza': vza[valid].ravel(),
        'sza': sza[valid].ravel(),
        'qa':  qa_combined[valid].ravel().astype(np.int8),
    }


def read_viirs_slot(
    sensor: str,
    slot_utc: datetime,
    window_min: int = LEO_WINDOW_MIN,
    domain_clip: bool = True,
) -> Optional[dict[str, np.ndarray]]:
    """Return QA-filtered VIIRS pixel arrays for all granules within the time slot.

    Parameters
    ----------
    sensor      : 'viirs_snpp' or 'viirs_noaa20'
    slot_utc    : centre of the 30-min slot (UTC datetime)
    window_min  : half-window in minutes for granule selection
    domain_clip : if True, restrict to the Vietnam study domain

    Returns a dict with flat 1-D arrays (all valid pixels from all matching files):
        lat, lon, aod, vza, sza, qa
    Returns None if no valid data found.
    """
    if sensor not in _SENSOR_DIRS:
        raise ValueError(f'Unknown sensor key "{sensor}". '
                         f'Use one of: {list(_SENSOR_DIRS)}')

    files = _viirs_files_in_window(sensor, slot_utc, window_min)
    if not files:
        return None

    parts: list[dict[str, np.ndarray]] = []
    for fpath in files:
        px = _read_viirs_file(fpath)
        if px is None:
            continue

        if domain_clip:
            mask = (
                (px['lat'] >= LAT_MIN) & (px['lat'] <= LAT_MAX)
                & (px['lon'] >= LON_MIN) & (px['lon'] <= LON_MAX)
            )
            if not np.any(mask):
                continue
            px = {k: v[mask] for k, v in px.items()}

        parts.append(px)

    if not parts:
        return None

    combined: dict[str, np.ndarray] = {}
    for key in parts[0]:
        combined[key] = np.concatenate([p[key] for p in parts])
    return combined


def read_viirs_date(sensor: str, date: datetime) -> Optional[dict[str, np.ndarray]]:
    """Return all QA-filtered VIIRS pixels for an entire calendar day.

    Equivalent to calling read_viirs_slot with a large window (24 h),
    but more efficient: scans only the day-of-year directory.
    """
    files = _viirs_files_for_date(sensor, date)
    if not files:
        return None

    parts = []
    for fpath in files:
        px = _read_viirs_file(fpath)
        if px is None:
            continue
        # Clip to domain
        mask = (
            (px['lat'] >= LAT_MIN) & (px['lat'] <= LAT_MAX)
            & (px['lon'] >= LON_MIN) & (px['lon'] <= LON_MAX)
        )
        if np.any(mask):
            parts.append({k: v[mask] for k, v in px.items()})

    if not parts:
        return None

    combined: dict[str, np.ndarray] = {}
    for key in parts[0]:
        combined[key] = np.concatenate([p[key] for p in parts])
    return combined
