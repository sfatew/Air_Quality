"""MERRA-2 M2T1NXAER hourly AOD loader and 0.05° collocator.

v3.4.0 anchor for §7.4.1 soft calibration and the MERRA-2-bearing triplets in
§7.4.2.  Files are the Vietnam-bbox subsets produced by
`MERRA2/download_m2t1nxaer.py`:

    /home/slow_data/Air_Quality/MERRA2/M2T1NXAER/YYYY/MM/
        MERRA2_400.tavg1_2d_aer_Nx.YYYYMMDD_vnm.nc4

Native grid: 0.5° × 0.625° (32 lat × 13 lon over Vietnam bbox), 24 hourly
timesteps per file.  Variable TOTEXTTAU = total aerosol extinction AOD at 550 nm.

This module:

  1. Reads one file per UTC date with on-disk LRU caching of the most recently
     opened dataset (slot loops over 48 timestamps per day; reopening the file
     for every slot would dominate Stage A wall time).
  2. Nearest-hour matches the MERRA-2 timestep to a 30-min slot centre
     (MERRA-2 is hourly; we don't interpolate in time per §7.4.1 step 1).
  3. Bilinearly resamples the (32 × 13) coarse grid to the (NLAT × NLON) Vietnam
     0.05° grid before returning, matching what the §7.4.1 fit expects: a
     same-grid (sat, MERRA-2) pair per cell.
"""

from __future__ import annotations
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple
from functools import lru_cache

import numpy as np

from config import (
    LATS, LONS, NLAT, NLON,
    MERRA2_DIR, MERRA2_AOD_VAR,
)


def merra2_path(d: date) -> Path:
    """Return the expected per-day Vietnam-subset MERRA2 file path."""
    return (MERRA2_DIR / f'{d.year:04d}' / f'{d.month:02d}'
            / f'MERRA2_400.tavg1_2d_aer_Nx.{d.year:04d}{d.month:02d}{d.day:02d}_vnm.nc4')


# ── Bilinear-resampling weights (precomputed once for the Vietnam grid) ──────

@lru_cache(maxsize=1)
def _resample_weights(
    src_lat: tuple, src_lon: tuple,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray,
           np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Precompute bilinear-interpolation indices and weights.

    The MERRA-2 lat/lon vectors are identical for every file (Vietnam subset),
    so the weights are constant — caching them turns Stage A's per-slot resample
    into pure gather + multiply on cached numpy arrays.

    Returns row0, row1, col0, col1 (int32 indices into the 32×13 source grid)
    and wrow0, wrow1, wcol0, wcol1 (float32 weights summing to 1 along each axis).
    Each output array has shape (NLAT, NLON) so the final resample is one
    fancy-indexed lookup followed by a weighted sum.
    """
    src_lat_arr = np.asarray(src_lat, dtype=np.float64)   # ascending or descending; we re-sort
    src_lon_arr = np.asarray(src_lon, dtype=np.float64)

    # Force ascending source axes for clean searchsorted; remember the permutation.
    lat_order = np.argsort(src_lat_arr)
    lon_order = np.argsort(src_lon_arr)
    s_lat = src_lat_arr[lat_order]
    s_lon = src_lon_arr[lon_order]

    # Clip target coords to source range so edge cells get nearest-edge weight=1
    tgt_lat = np.clip(LATS, s_lat[0], s_lat[-1]).astype(np.float64)
    tgt_lon = np.clip(LONS, s_lon[0], s_lon[-1]).astype(np.float64)

    # 1-D bracket indices on the ascending source axis.
    ilat1 = np.searchsorted(s_lat, tgt_lat, side='left').clip(1, len(s_lat) - 1)
    ilon1 = np.searchsorted(s_lon, tgt_lon, side='left').clip(1, len(s_lon) - 1)
    ilat0 = ilat1 - 1
    ilon0 = ilon1 - 1

    # Lerp weights along each axis.
    dlat = s_lat[ilat1] - s_lat[ilat0]
    dlon = s_lon[ilon1] - s_lon[ilon0]
    wlat1 = (tgt_lat - s_lat[ilat0]) / np.where(dlat > 0, dlat, 1.0)
    wlon1 = (tgt_lon - s_lon[ilon0]) / np.where(dlon > 0, dlon, 1.0)
    wlat0 = 1.0 - wlat1
    wlon0 = 1.0 - wlon1

    # Map back to original (possibly descending) source-axis indices.
    ilat0_orig = lat_order[ilat0]
    ilat1_orig = lat_order[ilat1]
    ilon0_orig = lon_order[ilon0]
    ilon1_orig = lon_order[ilon1]

    # Broadcast to (NLAT, NLON) so the resample is a single elementwise op.
    row0 = np.broadcast_to(ilat0_orig[:, None], (NLAT, NLON)).astype(np.int32).copy()
    row1 = np.broadcast_to(ilat1_orig[:, None], (NLAT, NLON)).astype(np.int32).copy()
    col0 = np.broadcast_to(ilon0_orig[None, :], (NLAT, NLON)).astype(np.int32).copy()
    col1 = np.broadcast_to(ilon1_orig[None, :], (NLAT, NLON)).astype(np.int32).copy()
    w_r0 = np.broadcast_to(wlat0[:, None],      (NLAT, NLON)).astype(np.float32).copy()
    w_r1 = np.broadcast_to(wlat1[:, None],      (NLAT, NLON)).astype(np.float32).copy()
    w_c0 = np.broadcast_to(wlon0[None, :],      (NLAT, NLON)).astype(np.float32).copy()
    w_c1 = np.broadcast_to(wlon1[None, :],      (NLAT, NLON)).astype(np.float32).copy()

    return row0, row1, col0, col1, w_r0, w_r1, w_c0, w_c1


def _bilinear_resample(field: np.ndarray, src_lat: np.ndarray, src_lon: np.ndarray) -> np.ndarray:
    """Resample a (n_src_lat, n_src_lon) MERRA-2 hourly slice to (NLAT, NLON)."""
    row0, row1, col0, col1, w_r0, w_r1, w_c0, w_c1 = _resample_weights(
        tuple(src_lat.tolist()), tuple(src_lon.tolist())
    )
    f = field.astype(np.float32, copy=False)
    v00 = f[row0, col0]
    v01 = f[row0, col1]
    v10 = f[row1, col0]
    v11 = f[row1, col1]
    out = (
        v00 * w_r0 * w_c0
      + v01 * w_r0 * w_c1
      + v10 * w_r1 * w_c0
      + v11 * w_r1 * w_c1
    )
    return out.astype(np.float32)


# ── Per-day file cache ───────────────────────────────────────────────────────

_DAY_CACHE: dict[date, dict] = {}
_CACHE_MAX = 4   # keep ~96 hourly slices in memory (4 days × 24)


def _load_day(d: date) -> Optional[dict]:
    """Open the MERRA-2 file for date `d`; return a dict of resampled hourly fields.

    Returns ``None`` if the file is missing.  Result schema:

        {
            'hours':    array of 24 ints (UTC hour 0..23),
            'aod_hour': (24, NLAT, NLON) float32 — resampled TOTEXTTAU per hour
        }

    The whole day is resampled up-front because Stage A loops over 48 slots
    per day and reads MERRA-2 for each; per-slot resampling would do the same
    bilinear interpolation up to 24 times.
    """
    if d in _DAY_CACHE:
        return _DAY_CACHE[d]
    fpath = merra2_path(d)
    if not fpath.exists():
        return None

    try:
        import netCDF4 as nc
        with nc.Dataset(str(fpath)) as ds:
            src_lat = np.asarray(ds.variables['lat'][:], dtype=np.float64)
            src_lon = np.asarray(ds.variables['lon'][:], dtype=np.float64)
            # 'time' is minutes-since-start-of-day per the GES DISC convention;
            # we only need the UTC hour for nearest-slot matching, so derive
            # it from the timestep index (M2T1NXAER is strictly hourly 0..23).
            n_t = ds.variables['time'].shape[0]
            if n_t != 24:
                # Unusual file; defer to actual time decoding.
                from netCDF4 import num2date
                times = num2date(ds.variables['time'][:],
                                 ds.variables['time'].units,
                                 ds.variables['time'].calendar)
                hours = np.asarray([t.hour for t in times], dtype=np.int16)
            else:
                hours = np.arange(24, dtype=np.int16)

            raw = np.asarray(ds.variables[MERRA2_AOD_VAR][:], dtype=np.float32)
            # Resample every hour up-front.
            out = np.empty((n_t, NLAT, NLON), dtype=np.float32)
            for i in range(n_t):
                out[i] = _bilinear_resample(raw[i], src_lat, src_lon)
    except Exception as exc:
        print(f'  [merra2] failed to read {fpath}: {exc}')
        return None

    entry = {'hours': hours, 'aod_hour': out}

    # Tiny LRU eviction — keep only the most recent CACHE_MAX days.
    if len(_DAY_CACHE) >= _CACHE_MAX:
        oldest = next(iter(_DAY_CACHE))
        _DAY_CACHE.pop(oldest, None)
    _DAY_CACHE[d] = entry
    return entry


# ── Public API ───────────────────────────────────────────────────────────────

def get_slot_grid(slot_utc: datetime) -> Optional[np.ndarray]:
    """Return MERRA-2 AOD at the 0.05° Vietnam grid for the slot's nearest hour.

    Returns a (NLAT, NLON) float32 array or ``None`` if the file is missing.
    MERRA-2 is hourly; the 30-min slot is matched to its nearest hourly step
    (e.g. both 06:00 and 06:30 UTC draw from the 06:00 MERRA-2 hour).
    """
    d = slot_utc.date()
    entry = _load_day(d)
    if entry is None:
        return None
    # Nearest-hour: round to closest hour, with 24:00 → next-day 00:00 rollover.
    rounded_hour = slot_utc.hour + (1 if slot_utc.minute >= 30 else 0)
    if rounded_hour >= 24:
        from datetime import timedelta
        next_entry = _load_day(d + timedelta(days=1))
        if next_entry is None:
            return None
        return next_entry['aod_hour'][0].copy()
    # Find the matching hour index (normally hours == [0,1,...,23]).
    idx_arr = np.where(entry['hours'] == rounded_hour)[0]
    if idx_arr.size == 0:
        # Hourly grid not aligned with integer hours — fall back to nearest.
        idx = int(np.argmin(np.abs(entry['hours'] - rounded_hour)))
    else:
        idx = int(idx_arr[0])
    return entry['aod_hour'][idx].copy()


def clear_cache() -> None:
    """Drop all cached MERRA-2 hourly stacks.  Call after long batch runs."""
    _DAY_CACHE.clear()
