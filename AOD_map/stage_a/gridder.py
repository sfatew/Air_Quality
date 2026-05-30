"""Step A2: box-average ungridded L2 pixel data onto the 0.05° target grid.

For each 30-min slot the gridder:
  1. Receives flat (lat, lon, values…) pixel arrays from viirs.py or modis.py.
  2. Maps each pixel to its 0.05° grid cell by coordinate binning (no nearest-
     neighbour fill, no ring search — centre-of-pixel must fall inside the cell).
  3. Computes per-cell mean, std, count, and mean VZA/SZA.

Himawari L2/L3 are already on the 0.05° grid (returned by himawari.py), so
they bypass the gridder entirely.

For VIIRS, the VIIRS Deep Blue 6 km pixels can span multiple 0.05° cells. A
single pass over the pixel array and numpy histogram2d are used for efficiency
(O(N) with no Python loops over grid cells).
"""

from __future__ import annotations
from typing import Optional

import numpy as np

try:
    import cupy as cp
    _xp = cp
except ImportError:
    cp = None
    _xp = np

from config import (
    LATS, LONS, NLAT, NLON, GRID_RES,
    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
)

# Bin edges for latitude (north → south) and longitude (west → east)
# histogram2d uses [min, max) bins, so we define edges from extremes
_LAT_EDGES = np.linspace(LAT_MAX, LAT_MIN, NLAT + 1)   # descending (north first)
_LON_EDGES = np.linspace(LON_MIN, LON_MAX, NLON + 1)    # ascending


def _lat_to_row(lat: np.ndarray) -> np.ndarray:
    """Map latitude → 0-based row index (0 = northernmost row)."""
    return np.floor((LAT_MAX - lat) / GRID_RES).astype(np.int32)


def _lon_to_col(lon: np.ndarray) -> np.ndarray:
    """Map longitude → 0-based column index (0 = westernmost column)."""
    return np.floor((lon - LON_MIN) / GRID_RES).astype(np.int32)


def bin_to_grid(
    lat: np.ndarray,
    lon: np.ndarray,
    aod: np.ndarray,
    vza: Optional[np.ndarray] = None,
    sza: Optional[np.ndarray] = None,
    ae:  Optional[np.ndarray] = None,
) -> dict[str, np.ndarray]:
    """Box-average pixel data onto the 0.05° grid.

    Parameters
    ----------
    lat, lon : 1-D arrays of pixel centre coordinates (degrees).
    aod      : 1-D array of AOD values at 550 nm.
    vza, sza : optional 1-D arrays of viewing / solar zenith angles (degrees).
    ae       : optional 1-D Ångström exponent array.

    Returns a dict with 2-D (NLAT × NLON) float32 arrays:
        aod_mean   – cell mean AOD (NaN where n_pixels == 0)
        aod_std    – within-cell std dev (NaN where n_pixels < 2)
        n_pixels   – number of valid pixels contributing to each cell (int16)
        vza_mean   – mean VZA (NaN where n_pixels == 0, or if vza not provided)
        sza_mean   – mean SZA (similarly)
        ae_mean    – mean Ångström exponent (similarly)
    """
    shape = (NLAT, NLON)

    # In-domain pixel mask
    in_dom = (
        (lat >= LAT_MIN) & (lat < LAT_MAX)
        & (lon >= LON_MIN) & (lon < LON_MAX)
        & np.isfinite(aod) & (aod >= 0) & (aod <= 5.0)
    )
    lat  = lat[in_dom]
    lon  = lon[in_dom]
    aod  = aod[in_dom]
    vza  = vza[in_dom]  if vza is not None else None
    sza  = sza[in_dom]  if sza is not None else None
    ae   = ae[in_dom]   if ae  is not None else None

    # Pixel → grid cell indices
    rows = _lat_to_row(lat)
    cols = _lon_to_col(lon)

    # Clamp to valid range (should be in-domain already, but guard against FP edge)
    valid = (rows >= 0) & (rows < NLAT) & (cols >= 0) & (cols < NLON)
    rows = rows[valid];  cols = cols[valid];  aod = aod[valid]
    if vza is not None: vza = vza[valid]
    if sza is not None: sza = sza[valid]
    if ae  is not None: ae  = ae[valid]

    # GPU-accelerated accumulation via linearised bincount (replaces np.add.at)
    xp      = _xp
    rows_d  = xp.asarray(rows)
    cols_d  = xp.asarray(cols)
    aod_d   = xp.asarray(aod.astype(np.float64))
    linear  = (rows_d * NLON + cols_d).astype(np.int64)
    minlen  = NLAT * NLON

    n_pix_d   = xp.bincount(linear, minlength=minlen).reshape(shape)
    aod_sum_d = xp.bincount(linear, weights=aod_d,      minlength=minlen).reshape(shape)
    aod_sq_d  = xp.bincount(linear, weights=aod_d ** 2, minlength=minlen).reshape(shape)
    vza_sum_d = (xp.bincount(linear, weights=xp.asarray(vza.astype(np.float64)), minlength=minlen).reshape(shape)
                 if vza is not None else None)
    sza_sum_d = (xp.bincount(linear, weights=xp.asarray(sza.astype(np.float64)), minlength=minlen).reshape(shape)
                 if sza is not None else None)
    ae_sum_d  = (xp.bincount(linear, weights=xp.asarray(ae.astype(np.float64)),  minlength=minlen).reshape(shape)
                 if ae  is not None else None)

    has_data = n_pix_d > 0
    n_d = xp.where(has_data, n_pix_d.astype(np.float64), np.nan)

    aod_mean = np.asarray(xp.where(has_data, aod_sum_d / n_d, np.nan).astype(np.float32))

    variance = xp.where(has_data, aod_sq_d / n_d - (aod_sum_d / n_d) ** 2, np.nan)
    variance = xp.clip(variance, 0, None)
    aod_std  = np.asarray(xp.where(n_pix_d >= 2, xp.sqrt(variance), np.nan).astype(np.float32))

    def _mean_optional(arr_sum_d, flag):
        if arr_sum_d is None:
            return np.full(shape, np.nan, dtype=np.float32)
        return np.asarray(xp.where(flag, arr_sum_d / n_d, np.nan).astype(np.float32))

    return {
        'aod_mean': aod_mean,
        'aod_std':  aod_std,
        'n_pixels': np.asarray(n_pix_d.astype(np.int16)),
        'vza_mean': _mean_optional(vza_sum_d, has_data),
        'sza_mean': _mean_optional(sza_sum_d, has_data),
        'ae_mean':  _mean_optional(ae_sum_d,  has_data),
    }


def grid_from_himawari(himawari_result: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """Repackage himawari.read_himawari_slot output into gridder-compatible format.

    Himawari is already on the 0.05° grid, so no binning is needed.
    This just renames keys to match the unified schema used by fusion.py.
    """
    r = himawari_result
    return {
        'aod_mean': r.get('aot',         np.full((NLAT, NLON), np.nan, dtype=np.float32)),
        'aod_std':  np.full((NLAT, NLON), np.nan, dtype=np.float32),  # single TIF → no std
        'n_pixels': r.get('n_obs',       np.zeros((NLAT, NLON), dtype=np.int16)),
        'vza_mean': r.get('vza',         np.full((NLAT, NLON), np.nan, dtype=np.float32)),
        'sza_mean': r.get('sza',         np.full((NLAT, NLON), np.nan, dtype=np.float32)),
        'ae_mean':  r.get('ae',          np.full((NLAT, NLON), np.nan, dtype=np.float32)),
        'vza_flag': r.get('vza_flag',    np.zeros((NLAT, NLON), dtype=np.int8)),
    }


def daily_max_valid(daily_grids: list[dict[str, np.ndarray]]) -> dict[str, np.ndarray]:
    """Aggregate a list of 30-min grid dicts into a daily summary.

    Returns:
        aod_daily_mean   – mean over all valid slots
        aod_daily_std    – std over all valid slots
        n_valid_slots    – number of slots with valid data per cell
        aod_daily_max    – maximum AOD across slots
        hour_of_max      – slot index (0–47) at which daily max was observed
    """
    shape = (NLAT, NLON)
    n     = len(daily_grids)

    xp    = _xp
    stack = xp.full((n, NLAT, NLON), np.nan, dtype=np.float32)
    for i, g in enumerate(daily_grids):
        aod = g.get('aod_mean')
        if aod is not None:
            stack[i] = xp.asarray(aod)

    n_valid_d   = xp.sum(~xp.isnan(stack), axis=0).astype(np.int16)
    aod_mean_d  = xp.nanmean(stack, axis=0).astype(np.float32)
    aod_std_d   = xp.nanstd(stack,  axis=0).astype(np.float32)
    aod_max_d   = xp.nanmax(stack,  axis=0).astype(np.float32)
    hour_d      = xp.argmax(
        xp.where(xp.isnan(stack), -np.inf, stack), axis=0
    ).astype(np.int8)
    hour_d[n_valid_d == 0] = -1

    return {
        'aod_daily_mean': np.asarray(aod_mean_d),
        'aod_daily_std':  np.asarray(aod_std_d),
        'n_valid_slots':  np.asarray(n_valid_d),
        'aod_daily_max':  np.asarray(aod_max_d),
        'hour_of_max':    np.asarray(hour_d),
    }
