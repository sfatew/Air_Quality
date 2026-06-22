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

# Floor on the divisor when computing the coefficient of variation, so cells
# with vanishing means don't blow cv up to infinity (clean-air cells with
# small absolute spread should report low cv, not large).
CV_MEAN_FLOOR = 0.02


def _lat_to_row(lat: np.ndarray) -> np.ndarray:
    """Map latitude → 0-based row index (0 = northernmost row)."""
    return np.floor((LAT_MAX - lat) / GRID_RES).astype(np.int32)


def _lon_to_col(lon: np.ndarray) -> np.ndarray:
    """Map longitude → 0-based column index (0 = westernmost column)."""
    return np.floor((lon - LON_MIN) / GRID_RES).astype(np.int32)


def station_cell(lat: float, lon: float) -> tuple[int, int, bool]:
    """Return (row, col, in_domain) for a station's 0.05° grid cell."""
    row = int(np.floor((LAT_MAX - lat) / GRID_RES))
    col = int(np.floor((lon - LON_MIN) / GRID_RES))
    return row, col, (0 <= row < NLAT) and (0 <= col < NLON)


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

    Returns a dict with 2-D (NLAT × NLON) arrays:
        aod_mean   – cell mean AOD (NaN where n_valid == 0); NEVER masked by
                     a heterogeneity gate — cv carries the quality signal.
        aod_std    – within-cell std dev (NaN where n_valid < 2)
        n_valid    – pixels with finite, in-range AOD binned into the cell (int16)
        n_total    – sensor-QA-passing pixels arriving at the gridder whose
                     centres fall in the cell, regardless of AOD validity (int16).
                     Counted empirically per slot so it reflects actual swath
                     coverage; NOT a theoretical maximum.
        cv         – aod_std / max(aod_mean, CV_MEAN_FLOOR); NaN where n_valid<2.
                     Downstream consumers gate on `cv <= CV_MAX` to drop
                     heterogeneous cells; choose CV_MAX per use-case.
        vza_mean   – mean VZA (NaN where n_valid == 0, or if vza not provided)
        sza_mean   – mean SZA (similarly)
        ae_mean    – mean Ångström exponent (similarly)
    """
    shape  = (NLAT, NLON)
    minlen = NLAT * NLON

    # Geometric mask: pixel centre inside the spatial domain.  Used as the
    # denominator (n_total) — every pixel reaching the gridder for this slot
    # gets counted regardless of AOD finiteness.
    in_geom = (
        (lat >= LAT_MIN) & (lat < LAT_MAX)
        & (lon >= LON_MIN) & (lon < LON_MAX)
    )

    # Count n_total empirically from the pixels that arrived this slot, so the
    # denominator reflects real swath coverage (including edge geometry,
    # missing scans, etc.) rather than a hard-coded sensor-pixel-size formula.
    rows_g = _lat_to_row(lat[in_geom])
    cols_g = _lon_to_col(lon[in_geom])
    safe_g = (rows_g >= 0) & (rows_g < NLAT) & (cols_g >= 0) & (cols_g < NLON)
    linear_g = (rows_g[safe_g] * NLON + cols_g[safe_g]).astype(np.int64)
    n_total = np.bincount(linear_g, minlength=minlen).reshape(shape).astype(np.int16)

    # Validity mask for the actual binning — adds AOD finiteness / range.
    is_valid = in_geom & np.isfinite(aod) & (aod >= 0) & (aod <= 5.0)
    lat = lat[is_valid]
    lon = lon[is_valid]
    aod = aod[is_valid]
    vza = vza[is_valid] if vza is not None else None
    sza = sza[is_valid] if sza is not None else None
    ae  = ae[is_valid]  if ae  is not None else None

    rows = _lat_to_row(lat)
    cols = _lon_to_col(lon)
    safe = (rows >= 0) & (rows < NLAT) & (cols >= 0) & (cols < NLON)
    rows = rows[safe];  cols = cols[safe];  aod = aod[safe]
    if vza is not None: vza = vza[safe]
    if sza is not None: sza = sza[safe]
    if ae  is not None: ae  = ae[safe]

    # Linearised bincount over flat cell indices — O(N) over pixels.
    aod64  = aod.astype(np.float64)
    linear = (rows * NLON + cols).astype(np.int64)

    n_pix   = np.bincount(linear, minlength=minlen).reshape(shape)
    aod_sum = np.bincount(linear, weights=aod64,      minlength=minlen).reshape(shape)
    aod_sq  = np.bincount(linear, weights=aod64 ** 2, minlength=minlen).reshape(shape)
    vza_sum = (np.bincount(linear, weights=vza.astype(np.float64), minlength=minlen).reshape(shape)
               if vza is not None else None)
    sza_sum = (np.bincount(linear, weights=sza.astype(np.float64), minlength=minlen).reshape(shape)
               if sza is not None else None)
    ae_sum  = (np.bincount(linear, weights=ae.astype(np.float64),  minlength=minlen).reshape(shape)
               if ae  is not None else None)

    has_data = n_pix > 0
    n_safe   = np.where(has_data, n_pix.astype(np.float64), np.nan)

    aod_mean = np.where(has_data, aod_sum / n_safe, np.nan).astype(np.float32)

    variance = np.where(has_data, aod_sq / n_safe - (aod_sum / n_safe) ** 2, np.nan)
    variance = np.clip(variance, 0, None)
    aod_std  = np.where(n_pix >= 2, np.sqrt(variance), np.nan).astype(np.float32)

    def _mean_optional(arr_sum, flag):
        if arr_sum is None:
            return np.full(shape, np.nan, dtype=np.float32)
        return np.where(flag, arr_sum / n_safe, np.nan).astype(np.float32)

    vza_mean = _mean_optional(vza_sum, has_data)
    sza_mean = _mean_optional(sza_sum, has_data)
    ae_mean  = _mean_optional(ae_sum,  has_data)

    # Coefficient of variation — within-cell heterogeneity normalized by the
    # mean. NaN where std is undefined (n_valid < 2); downstream consumers
    # gate on `cv <= CV_MAX`.
    cv = (aod_std / np.maximum(aod_mean, CV_MEAN_FLOOR)).astype(np.float32)

    return {
        'aod_mean':    aod_mean,
        'aod_std':     aod_std,
        'n_valid':     n_pix.astype(np.int16),
        'n_total':     n_total,
        'cv':          cv,
        'vza_mean':    vza_mean,
        'sza_mean':    sza_mean,
        'ae_mean':     ae_mean,
    }


def grid_from_himawari(himawari_result: dict[str, np.ndarray]) -> dict[str, 'np.ndarray | int']:
    """Repackage himawari.read_himawari_slot output into gridder-compatible format.

    Himawari is already on the 0.05° grid, so no binning is needed.
    cv and aod_std are not defined for a pre-gridded source — there is no
    sub-cell native pixel to compute variance over.  n_total is 1 because
    each 0.05° cell corresponds to exactly one Himawari grid value.
    """
    r = himawari_result
    shape = (NLAT, NLON)
    return {
        'aod_mean':   r.get('aot',         np.full(shape, np.nan, dtype=np.float32)),
        'aod_std':    np.full(shape, np.nan, dtype=np.float32),  # pre-gridded → no within-cell std
        'n_valid':    r.get('n_obs',       np.zeros(shape, dtype=np.int16)),
        'n_total':    np.ones(shape, dtype=np.int16),            # one delivered value per cell
        'cv':         np.full(shape, np.nan, dtype=np.float32),  # undefined without sub-cell pixels
        'vza_mean':   r.get('vza',         np.full(shape, np.nan, dtype=np.float32)),
        'sza_mean':   r.get('sza',         np.full(shape, np.nan, dtype=np.float32)),
        'ae_mean':    r.get('ae',          np.full(shape, np.nan, dtype=np.float32)),
        'vza_flag':   r.get('vza_flag',    np.zeros(shape, dtype=np.int8)),
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
    n = len(daily_grids)

    stack = np.full((n, NLAT, NLON), np.nan, dtype=np.float32)
    for i, g in enumerate(daily_grids):
        aod = g.get('aod_mean')
        if aod is not None:
            stack[i] = aod

    n_valid  = np.sum(~np.isnan(stack), axis=0).astype(np.int16)
    aod_mean = np.nanmean(stack, axis=0).astype(np.float32)
    aod_std  = np.nanstd(stack,  axis=0).astype(np.float32)
    aod_max  = np.nanmax(stack,  axis=0).astype(np.float32)
    hour     = np.argmax(
        np.where(np.isnan(stack), -np.inf, stack), axis=0
    ).astype(np.int8)
    hour[n_valid == 0] = -1

    return {
        'aod_daily_mean': aod_mean,
        'aod_daily_std':  aod_std,
        'n_valid_slots':  n_valid,
        'aod_daily_max':  aod_max,
        'hour_of_max':    hour,
    }
