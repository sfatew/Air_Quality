"""Step A3: physics-based AOD normalization for hygroscopic growth and PBLH mixing.

Formula (Kotchenruther & Hobbs 1998; Nguyen et al. 2025 Eq. 1):

    AOD_phys = AOD × (1 − RH/100)^γ / PBLH

where
    γ    = 0.6   (hygroscopic growth exponent, spatially uniform)
    PBLH = planetary boundary layer height (m), constrained ≥ PBLH_MIN

Validated by Nguyen et al. 2025 to improve hourly Himawari–PM₂.₅ correlation
from r = 0.110 to r = 0.162 (1.5× improvement).

ERA5 source
-----------
  File  : ERA5_FILE (Vietnam_ERA5_bbox.nc)
  Grid  : 0.25° × 0.25°, 63 × 33 points, N=23.5 W=102.0 S=8.0 E=110.0
  Time  : UTC+7, hourly, Sep 2022 – Apr 2026
  Vars  : RH (%), PBLH (m)

The ERA5 dataset is opened once as a lazy xarray Dataset and kept in a
module-level cache to avoid re-opening on every call.  RH and PBLH are
bilinearly interpolated from 0.25° to the 0.05° AOD grid using
scipy.interpolate.RegularGridInterpolator.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import xarray as xr
from scipy.interpolate import RegularGridInterpolator

try:
    import cupy as cp
    cp.array([0])  # triggers CUDA init — raises if no GPU device
    _xp = cp
except Exception:
    cp = None
    _xp = np

from config import (
    ERA5_FILE, GAMMA, PBLH_MIN,
    LATS, LONS, NLAT, NLON,
    TZ_OFFSET_HOURS,
)

# ── Module-level ERA5 cache ───────────────────────────────────────────────────
_era5_ds: Optional[xr.Dataset] = None
_era5_lats: Optional[np.ndarray] = None   # ERA5 latitude array (descending)
_era5_lons: Optional[np.ndarray] = None   # ERA5 longitude array (ascending)


def _load_era5() -> xr.Dataset:
    """Open the ERA5 NetCDF lazily (once per process)."""
    global _era5_ds, _era5_lats, _era5_lons
    if _era5_ds is None:
        _era5_ds   = xr.open_dataset(ERA5_FILE, engine='netcdf4')
        _era5_lats = _era5_ds['latitude'].values.astype(np.float64)
        _era5_lons = _era5_ds['longitude'].values.astype(np.float64)
    return _era5_ds


# ── ERA5 nearest-hourly retrieval ─────────────────────────────────────────────

def _nearest_era5_slice(slot_utc: datetime) -> Optional[xr.Dataset]:
    """Return the ERA5 time slice nearest to slot_utc (UTC+7 local time)."""
    ds = _load_era5()
    # Convert slot to local time for ERA5 look-up (ERA5 timestamps are UTC+7)
    slot_local = slot_utc.replace(tzinfo=None) + timedelta(hours=TZ_OFFSET_HOURS)
    ts = np.datetime64(slot_local.replace(minute=0, second=0, microsecond=0))

    # Find the nearest available ERA5 hour
    era5_times = ds['time'].values
    idx = int(np.argmin(np.abs(era5_times - ts)))
    delta_h = abs((era5_times[idx] - ts) / np.timedelta64(1, 'h'))
    if delta_h > 1.0:
        return None   # nearest ERA5 step is more than 1 h away

    return ds.isel(time=idx)


# ── 0.25° → 0.05° interpolation ──────────────────────────────────────────────

def _interp_to_aod_grid(
    era5_lat: np.ndarray,
    era5_lon: np.ndarray,
    field: np.ndarray,
) -> np.ndarray:
    """Bilinearly interpolate an ERA5 field (lat descending) to the 0.05° AOD grid.

    Parameters
    ----------
    era5_lat : 1-D array of ERA5 latitude values (descending, e.g. 23.5→8.0)
    era5_lon : 1-D array of ERA5 longitude values (ascending, e.g. 102.0→110.0)
    field    : 2-D float array (n_lat, n_lon) in ERA5 grid orientation

    Returns (NLAT, NLON) float32 array on the 0.05° AOD grid.
    """
    # RegularGridInterpolator requires ascending axes
    if era5_lat[0] > era5_lat[-1]:
        era5_lat = era5_lat[::-1]
        field    = field[::-1, :]

    interp = RegularGridInterpolator(
        (era5_lat, era5_lon),
        field.astype(np.float64),
        method='linear',
        bounds_error=False,
        fill_value=np.nan,
    )

    # AOD grid points as (lat, lon) pairs
    lat_2d, lon_2d = np.meshgrid(LATS, LONS, indexing='ij')   # (NLAT, NLON)
    pts = np.column_stack([lat_2d.ravel(), lon_2d.ravel()])
    result = interp(pts).reshape(NLAT, NLON).astype(np.float32)
    return result


def get_era5_fields(slot_utc: datetime) -> Optional[dict[str, np.ndarray]]:
    """Return RH and PBLH interpolated to the 0.05° grid for a given UTC slot.

    Returns dict with keys 'RH' and 'PBLH' (both (NLAT, NLON) float32),
    or None if ERA5 data is not available for this slot.
    """
    era5_slice = _nearest_era5_slice(slot_utc)
    if era5_slice is None:
        return None

    lats = _era5_lats
    lons = _era5_lons

    rh_era5   = era5_slice['RH'].values.astype(np.float64)    # (n_lat, n_lon)
    pblh_era5 = era5_slice['PBLH'].values.astype(np.float64)

    rh_grid   = _interp_to_aod_grid(lats, lons, rh_era5)
    pblh_grid = _interp_to_aod_grid(lats, lons, pblh_era5)

    # Clip to physical bounds
    rh_grid   = np.clip(rh_grid,   0.0, 100.0)
    pblh_grid = np.clip(pblh_grid, PBLH_MIN, None)

    return {'RH': rh_grid, 'PBLH': pblh_grid}


# ── Step A3 application ───────────────────────────────────────────────────────

def apply_physics_correction(
    aod: np.ndarray,
    slot_utc: datetime,
    rh_override: Optional[np.ndarray] = None,
    pblh_override: Optional[np.ndarray] = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Apply physics-based AOD normalization (Step A3).

    Parameters
    ----------
    aod           : (NLAT, NLON) float32 raw AOD array (NaN = no data)
    slot_utc      : UTC datetime of the 30-min slot
    rh_override   : optional (NLAT, NLON) RH array [%] to use instead of ERA5
    pblh_override : optional (NLAT, NLON) PBLH array [m] to use instead of ERA5

    Returns
    -------
    aod_phys : (NLAT, NLON) physics-corrected AOD (NaN where input is NaN
               or ERA5 is unavailable)
    rh_used  : (NLAT, NLON) RH field actually used [%]
    pblh_used: (NLAT, NLON) PBLH field actually used [m]

    If ERA5 data is unavailable AND no override is supplied, returns the
    original AOD unchanged with NaN RH/PBLH arrays (caller can check).
    """
    if rh_override is not None and pblh_override is not None:
        rh   = np.clip(rh_override.astype(np.float32),   0.0, 100.0)
        pblh = np.clip(pblh_override.astype(np.float32), PBLH_MIN, None)
    else:
        era5 = get_era5_fields(slot_utc)
        if era5 is None:
            nan_grid = np.full((NLAT, NLON), np.nan, dtype=np.float32)
            return aod.copy(), nan_grid, nan_grid
        rh   = era5['RH']
        pblh = era5['PBLH']

    xp       = _xp
    rh_d     = xp.asarray(rh.astype(np.float64))
    pblh_d   = xp.asarray(pblh.astype(np.float64))
    aod_d    = xp.asarray(aod.astype(np.float64))

    hygro_d  = xp.power(1.0 - rh_d / 100.0, GAMMA)
    hygro_d  = xp.clip(hygro_d, 0.0, 1.0)

    aod_phys_d = aod_d * hygro_d / pblh_d
    aod_phys_d = xp.where(xp.isfinite(aod_d), aod_phys_d, np.nan).astype(np.float32)

    return np.asarray(aod_phys_d), rh, pblh


def close_era5() -> None:
    """Release the ERA5 file handle (call at end of processing run)."""
    global _era5_ds
    if _era5_ds is not None:
        _era5_ds.close()
        _era5_ds = None
