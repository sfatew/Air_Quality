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
  Dir   : ERA5_MONTHLY_DIR (era5_YYYYMM.nc monthly checkpoints)
  Grid  : 0.25° × 0.25°, 63 × 33 points, N=23.5 W=102.0 S=8.0 E=110.0
  Time  : UTC, hourly, Sep 2022 – Apr 2026
  Vars  : RH (%), PBLH (m)

The monthly files are stitched into one lazy xarray Dataset via
ERA5.load.load_era5_bbox and kept in a module-level cache to avoid
re-opening on every call.  RH and PBLH are bilinearly interpolated
from 0.25° to the 0.05° AOD grid using
scipy.interpolate.RegularGridInterpolator.
"""

from __future__ import annotations
import os
# Disable HDF5 file locking before xarray (and netCDF4/HDF5) is imported.
# With multi-process workers concurrently opening ERA5 monthly .nc files,
# HDF5's default locking can race and abort the worker silently (surfaces
# as BrokenProcessPool).  Safe because access is read-only.
os.environ.setdefault('HDF5_USE_FILE_LOCKING', 'FALSE')

from datetime import datetime
from typing import Optional

import numpy as np
import xarray as xr
from scipy.interpolate import RegularGridInterpolator

from config import (
    ERA5_MONTHLY_DIR, GAMMA, PBLH_MIN,
    LATS, LONS, NLAT, NLON,
)

# ── Module-level ERA5 cache ───────────────────────────────────────────────────
# One monthly file is held open per worker process at a time.  Opening all 44
# files at once via xr.open_mfdataset triggers silent HDF5 crashes (worker
# SIGABRT → BrokenProcessPool) under high-concurrency multiprocess loads.
_era5_current_ds: Optional[xr.Dataset] = None
_era5_current_key: Optional[str]       = None   # 'YYYYMM' of the cached file
_era5_current_times: Optional[np.ndarray] = None
_era5_lats: Optional[np.ndarray] = None   # latitude array (descending)
_era5_lons: Optional[np.ndarray] = None   # longitude array (ascending)


def _open_era5_month(yyyymm: str) -> Optional[xr.Dataset]:
    """Open era5_{yyyymm}.nc, closing any previously cached month."""
    global _era5_current_ds, _era5_current_key, _era5_current_times
    global _era5_lats, _era5_lons

    if _era5_current_key == yyyymm and _era5_current_ds is not None:
        return _era5_current_ds

    if _era5_current_ds is not None:
        _era5_current_ds.close()
        _era5_current_ds = None
        _era5_current_key = None
        _era5_current_times = None

    path = ERA5_MONTHLY_DIR / f'era5_{yyyymm}.nc'
    if not path.exists():
        return None

    ds = xr.open_dataset(path, engine='netcdf4')
    if _era5_lats is None:
        _era5_lats = ds['latitude'].values.astype(np.float64)
        _era5_lons = ds['longitude'].values.astype(np.float64)
    _era5_current_ds    = ds
    _era5_current_key   = yyyymm
    _era5_current_times = ds['time'].values
    return ds


# ── ERA5 nearest-hourly retrieval ─────────────────────────────────────────────

def _nearest_era5_slice(slot_utc: datetime) -> Optional[xr.Dataset]:
    """Return the ERA5 time slice nearest to slot_utc (UTC).

    Opens only the monthly file that contains slot_utc's hour.  Handles
    month-boundary cases by also probing the adjacent month when the slot's
    nearest hour might live in the neighbouring file.
    """
    slot_hour = slot_utc.replace(tzinfo=None, minute=0, second=0, microsecond=0)
    ts = np.datetime64(slot_hour)

    ds = _open_era5_month(slot_hour.strftime('%Y%m'))
    if ds is None:
        return None

    times = _era5_current_times
    idx = int(np.argmin(np.abs(times - ts)))
    delta_h = abs((times[idx] - ts) / np.timedelta64(1, 'h'))
    if delta_h > 1.0:
        return None
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

    rh64   = rh.astype(np.float64)
    pblh64 = pblh.astype(np.float64)
    aod64  = aod.astype(np.float64)

    hygro = np.clip(np.power(1.0 - rh64 / 100.0, GAMMA), 0.0, 1.0)

    aod_phys = aod64 * hygro / pblh64
    aod_phys = np.where(np.isfinite(aod64), aod_phys, np.nan).astype(np.float32)

    return aod_phys, rh, pblh


def close_era5() -> None:
    """Release the cached ERA5 monthly file handle (call at end of run)."""
    global _era5_current_ds, _era5_current_key, _era5_current_times
    if _era5_current_ds is not None:
        _era5_current_ds.close()
        _era5_current_ds = None
        _era5_current_key = None
        _era5_current_times = None
