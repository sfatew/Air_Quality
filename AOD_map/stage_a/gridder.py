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

    n_pixels = np.zeros(shape, dtype=np.int16)
    aod_sum  = np.zeros(shape, dtype=np.float64)
    aod_sq   = np.zeros(shape, dtype=np.float64)
    vza_sum  = np.zeros(shape, dtype=np.float64) if vza is not None else None
    sza_sum  = np.zeros(shape, dtype=np.float64) if sza is not None else None
    ae_sum   = np.zeros(shape, dtype=np.float64) if ae  is not None else None

    # Accumulate using numpy add.at (handles repeated indices correctly)
    np.add.at(n_pixels, (rows, cols), 1)
    np.add.at(aod_sum,  (rows, cols), aod)
    np.add.at(aod_sq,   (rows, cols), aod ** 2)
    if vza is not None:
        np.add.at(vza_sum, (rows, cols), vza)
    if sza is not None:
        np.add.at(sza_sum, (rows, cols), sza)
    if ae is not None:
        np.add.at(ae_sum,  (rows, cols), ae)

    has_data = n_pixels > 0
    n = n_pixels.astype(np.float64)
    n[~has_data] = np.nan

    aod_mean = np.where(has_data, aod_sum / n, np.nan).astype(np.float32)

    # Population std dev: E[X²] - (E[X])²
    variance = np.where(has_data, aod_sq / n - (aod_sum / n) ** 2, np.nan)
    variance = np.clip(variance, 0, None)  # numerical safety
    aod_std  = np.where(n_pixels >= 2, np.sqrt(variance), np.nan).astype(np.float32)

    def _mean_optional(arr_sum, flag):
        if arr_sum is None:
            return np.full(shape, np.nan, dtype=np.float32)
        return np.where(flag, arr_sum / n, np.nan).astype(np.float32)

    return {
        'aod_mean': aod_mean,
        'aod_std':  aod_std,
        'n_pixels': n_pixels,
        'vza_mean': _mean_optional(vza_sum, has_data),
        'sza_mean': _mean_optional(sza_sum, has_data),
        'ae_mean':  _mean_optional(ae_sum,  has_data),
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

    stack = np.full((n, NLAT, NLON), np.nan, dtype=np.float32)
    for i, g in enumerate(daily_grids):
        aod = g.get('aod_mean')
        if aod is not None:
            stack[i] = aod

    n_valid    = np.sum(~np.isnan(stack), axis=0).astype(np.int16)
    aod_mean   = np.nanmean(stack, axis=0).astype(np.float32)
    aod_std    = np.nanstd(stack,  axis=0).astype(np.float32)
    aod_max    = np.nanmax(stack,  axis=0).astype(np.float32)
    # slot index of max (first occurrence in ties)
    hour_of_max = np.argmax(
        np.where(np.isnan(stack), -np.inf, stack), axis=0
    ).astype(np.int8)
    hour_of_max[n_valid == 0] = -1

    return {
        'aod_daily_mean':  aod_mean,
        'aod_daily_std':   aod_std,
        'n_valid_slots':   n_valid,
        'aod_daily_max':   aod_max,
        'hour_of_max':     hour_of_max,
    }
