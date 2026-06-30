"""Stage B ancillary covariate loaders (per-slot, v3.4.0+stage_b_fixes).

Every dynamic accessor returns a (NLAT, NLON) float32 grid for one **UTC
slot timestamp** (a `datetime` rounded to a 30-min boundary).  Static
accessors take no time argument and are cached after first call.

Temporal handling (§3 of stage_b_fixes.md):
- ERA5 hourly → 30-min slot via **linear interpolation** between flanking
  hourly fields.  Slot at HH:30 = 0.5 × ERA5(HH:00) + 0.5 × ERA5(HH+1:00).
- CAMS 3-hourly → 30-min via linear interpolation in time between flanking
  3-hour fields.  Both source products (EAC4 reanalysis and the
  near-real-time forecast fallback) share the {00,03,06,09,12,15,18,21} UTC
  grid; only the native spatial resolution differs (0.75° vs 0.4°), which
  is handled transparently by `_interp_to_grid` reading lat/lon from the file.
- IMERG native 30-min — no temporal resampling needed.
- NDVI 16-day composite — nearest-on-or-before in time, cached per (year,DOY).
- Land cover annual — nearest year ≤ slot year, cached per year.

MERRA-2 is intentionally absent from this module (already used in Stage A;
including it as a Stage B predictor would double-dip the same reanalysis
signal, per fix doc §4).
"""

from __future__ import annotations

import os
import warnings
import zipfile
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Optional

import numpy as np
import xarray as xr
import rasterio
from rasterio.warp import reproject, Resampling
from scipy.interpolate import RegularGridInterpolator

from config import (
    LATS, LONS, NLAT, NLON,
    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
    ERA5_MONTHLY_DIR, CAMS_MONTHLY_DIR,
    NDVI_DIR, LANDCOVER_DIR, LANDSCAN_DIR, DEM_DIR, IMERG_DIR,
    FIRMS_ZIP, FIRMS_RADIUS_KM, FIRMS_LOOKBACK_H,
    EARTH_RADIUS_KM,
)

os.environ.setdefault('HDF5_USE_FILE_LOCKING', 'FALSE')

_TARGET_LAT = LATS
_TARGET_LON = LONS
LAT_2D, LON_2D = np.meshgrid(_TARGET_LAT, _TARGET_LON, indexing='ij')


def _interp_to_grid(src_lat: np.ndarray, src_lon: np.ndarray,
                    field: np.ndarray) -> np.ndarray:
    """Bilinearly interpolate a source field to the 0.05° Stage A grid.

    Target cells that fall outside the source bbox are clipped to the source
    edge → nearest-edge extrapolation.  This is needed because CAMS's native
    bbox (8.25°-23.25° lat, 102°-109.5° lon) is narrower than Stage A's
    (8.025°-23.475° lat, 102.025°-109.975° lon) by ~0.25° on each side, so
    without this ~9% of CAMS slot cells would be NaN.  Linear extrapolation
    would be wilder near strong gradients (e.g. dust plumes exiting the
    domain); nearest-edge is the safer choice for a smooth reanalysis.
    """
    if src_lat[0] > src_lat[-1]:
        src_lat = src_lat[::-1]
        field   = field[::-1, :]
    if src_lon[0] > src_lon[-1]:
        src_lon = src_lon[::-1]
        field   = field[:, ::-1]
    interp = RegularGridInterpolator(
        (src_lat, src_lon),
        field.astype(np.float64),
        method='linear',
        bounds_error=False,
        fill_value=np.nan,
    )
    pts_lat = np.clip(LAT_2D.ravel(), src_lat[0], src_lat[-1])
    pts_lon = np.clip(LON_2D.ravel(), src_lon[0], src_lon[-1])
    pts = np.column_stack([pts_lat, pts_lon])
    return interp(pts).reshape(NLAT, NLON).astype(np.float32)


def _nan_grid() -> np.ndarray:
    return np.full((NLAT, NLON), np.nan, dtype=np.float32)


# ── ERA5 (hourly, monthly-stitched) — per-slot via linear interp ─────────────

_ERA5_CACHE: dict[str, xr.Dataset] = {}

# Feature → (ERA5 variable name, do-not-interp flag).  All ERA5 vars are
# linear-interpolated in time per fix doc §3; ERA5 precip is intentionally
# absent — IMERG is used instead.
_ERA5_VARS: dict[str, str] = {
    't2m':   'T2m',
    'dpt':   'Td2m',
    'rh':    'RH',
    'sp':    'Psfc',
    'u10':   'U10',
    'v10':   'V10',
    'blh':   'PBLH',
    'tcc':   'CloudCover',
    'tcwv':  'TCWV',
    'ssrd':  'SolarRad',
    'fal':   'Albedo',
}


def _open_era5_month(yyyymm: str) -> Optional[xr.Dataset]:
    if yyyymm in _ERA5_CACHE:
        return _ERA5_CACHE[yyyymm]
    path = ERA5_MONTHLY_DIR / f'era5_{yyyymm}.nc'
    if not path.exists():
        return None
    ds = xr.open_dataset(path, engine='netcdf4')
    if len(_ERA5_CACHE) >= 3:
        oldest = next(iter(_ERA5_CACHE))
        _ERA5_CACHE.pop(oldest).close()
    _ERA5_CACHE[yyyymm] = ds
    return ds


def _select_era5_hour(ds: xr.Dataset, t: datetime) -> Optional[xr.Dataset]:
    """Return the ERA5 field at an exact hour `t`, opening the next month if needed."""
    try:
        sub = ds.sel(time=t.strftime('%Y-%m-%dT%H:00:00'))
    except (KeyError, ValueError):
        return None
    if sub.sizes.get('time', 1) == 0:
        return None
    return sub


def era5_slot(slot_utc: datetime) -> dict[str, np.ndarray]:
    """ERA5 fields at `slot_utc` (linear interp between flanking hourly steps).

    Slots aligned to HH:00 take the hourly field directly. Slots at HH:30
    use 0.5 × ERA5(HH:00) + 0.5 × ERA5(HH+1:00).  All requested keys are
    populated — missing variables return all-NaN grids.
    """
    out = {key: _nan_grid() for key in _ERA5_VARS}

    minute = slot_utc.minute
    if minute not in (0, 30):
        raise ValueError(f'ERA5 slot timestamp {slot_utc} not aligned to 30-min')

    hour_floor = slot_utc.replace(minute=0, second=0, microsecond=0)
    needs_pair = minute == 30
    hour_ceil  = hour_floor + timedelta(hours=1) if needs_pair else hour_floor

    ds_a = _open_era5_month(hour_floor.strftime('%Y%m'))
    ds_b = _open_era5_month(hour_ceil .strftime('%Y%m')) if needs_pair else ds_a
    if ds_a is None or ds_b is None:
        return out

    sub_a = _select_era5_hour(ds_a, hour_floor)
    sub_b = _select_era5_hour(ds_b, hour_ceil) if needs_pair else sub_a
    if sub_a is None or sub_b is None:
        return out

    lat = sub_a['latitude'].values.astype(np.float64)
    lon = sub_a['longitude'].values.astype(np.float64)
    for feature, var in _ERA5_VARS.items():
        if var not in sub_a or var not in sub_b:
            continue
        try:
            a = sub_a[var].values
            b = sub_b[var].values
        except Exception:
            continue
        if needs_pair:
            field = 0.5 * (a + b)
        else:
            field = a
        out[feature] = _interp_to_grid(lat, lon, field)
    return out


# ── CAMS (3-hourly AOD550) — per-slot via linear interp ──────────────────────

_CAMS_CACHE: dict[str, xr.Dataset] = {}


def _open_cams_month(yyyymm: str) -> Optional[xr.Dataset]:
    if yyyymm in _CAMS_CACHE:
        return _CAMS_CACHE[yyyymm]
    path = CAMS_MONTHLY_DIR / f'cams_{yyyymm}.nc'
    if not path.exists():
        return None
    ds = xr.open_dataset(path, engine='netcdf4')
    if len(_CAMS_CACHE) >= 3:
        _CAMS_CACHE.pop(next(iter(_CAMS_CACHE))).close()
    _CAMS_CACHE[yyyymm] = ds
    return ds


def _cams_flanking_anchors(slot_utc: datetime) -> tuple[datetime, datetime, float]:
    """Return (t_before, t_after, w_after) for linear interpolation.

    CAMS is on a 3-hour grid at 00:00, 03:00, 06:00, ….  The interpolation
    weight `w_after` is the fraction of the way from t_before to t_after.
    """
    h = slot_utc.hour + slot_utc.minute / 60.0
    h_before = (slot_utc.hour // 3) * 3
    h_after  = h_before + 3
    frac = (h - h_before) / 3.0
    base_day = slot_utc.replace(minute=0, second=0, microsecond=0, hour=0)
    t_before = base_day + timedelta(hours=h_before)
    t_after  = base_day + timedelta(hours=h_after)
    return t_before, t_after, frac


# 144 entries ≈ 3 days at 30-min cadence — enough that `cams_aod_slot_lagged`
# (3 h back) always hits when iteration is chronological, and ≥1 day of prior
# slots stay warm for any nearby re-walks.  Each entry is one (NLAT, NLON)
# float32 grid ≈ 200 kB, so 144 × 200 kB ≈ 30 MB total.
@lru_cache(maxsize=144)
def cams_aod_slot(slot_utc: datetime) -> Optional[np.ndarray]:
    t_b, t_a, w = _cams_flanking_anchors(slot_utc)
    ds_b = _open_cams_month(t_b.strftime('%Y%m'))
    ds_a = _open_cams_month(t_a.strftime('%Y%m'))
    if ds_b is None or ds_a is None or 'AOD550' not in ds_b or 'AOD550' not in ds_a:
        return None
    try:
        a_b = ds_b['AOD550'].sel(time=t_b.strftime('%Y-%m-%dT%H:00:00')).values
        a_a = ds_a['AOD550'].sel(time=t_a.strftime('%Y-%m-%dT%H:00:00')).values
    except (KeyError, ValueError):
        return None
    # Reproject each flanking field to the production grid BEFORE combining in
    # time.  The opposite order breaks when the two anchors come from different
    # CAMS sources (EAC4 0.75° → (21,11) vs forecast fallback 0.4° → (39,21))
    # because the native arrays don't share a shape.  Reprojecting first puts
    # both onto (NLAT, NLON) and the temporal interp is then unconditional.
    g_b = _interp_to_grid(ds_b['latitude'].values.astype(np.float64),
                          ds_b['longitude'].values.astype(np.float64), a_b)
    g_a = _interp_to_grid(ds_a['latitude'].values.astype(np.float64),
                          ds_a['longitude'].values.astype(np.float64), a_a)
    return ((1 - w) * g_b + w * g_a).astype(np.float32)


# ── IMERG (30-min GeoTIFFs, native to our slot cadence) ──────────────────────
# The IMERG V07B GIS product ships 9 layers per slot (total/liquid/ice × rate/
# accum, plus liquidPercent and numValid/Precip counts).  We use the
# **total.accum** layer: rainfall accumulation in mm over the 30-min window.
# Pixel values are integer-scaled by 10 per the GES DISC GIS README — divide
# by 10 to recover mm.
#
# Some slots are still zipped (*.zip), others have been pre-extracted into a
# sibling *.V07B/ directory.  The locator below prefers the extracted form.

_IMERG_TARGET_SUFFIX = '.total.accum.tif'
_IMERG_SCALE = 10.0  # pixel value / scale → mm in the 30-min slot


def _imerg_slot_source(slot_utc: datetime) -> Optional[tuple[Path, str]]:
    """Locate the IMERG source for this slot.  Returns (path, kind) where
    kind is 'dir' (extracted *.V07B directory) or 'zip', or None if absent.
    Extracted directories take priority — they're already on disk, no unzip
    needed."""
    folder = IMERG_DIR / f'{slot_utc.year:04d}' / f'{slot_utc.month:02d}' / f'{slot_utc.day:02d}'
    if not folder.exists():
        return None
    hh = slot_utc.strftime('%H%M')
    tag = slot_utc.strftime('%Y%m%d')
    ss = f'S{slot_utc.hour:02d}{slot_utc.minute:02d}00'

    dirs = sorted(p for p in folder.glob(f'*{tag}*{hh}*.V07B') if p.is_dir())
    if not dirs:
        dirs = sorted(p for p in folder.glob(f'*{ss}*.V07B') if p.is_dir())
    if dirs:
        return dirs[0], 'dir'

    zips = sorted(folder.glob(f'*{tag}*{hh}*.zip'))
    if not zips:
        zips = sorted(folder.glob(f'*{ss}*.zip'))
    if zips:
        return zips[0], 'zip'
    return None


def _imerg_open_total_accum(path: Path, kind: str) -> Optional[bytes]:
    """Return the raw bytes of the *.total.accum.tif layer from either an
    extracted directory or a zip.  None if the layer is missing."""
    if kind == 'dir':
        tifs = sorted(path.glob(f'*{_IMERG_TARGET_SUFFIX}'))
        if not tifs:
            return None
        return tifs[0].read_bytes()
    # kind == 'zip'
    with zipfile.ZipFile(path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(_IMERG_TARGET_SUFFIX)]
        if not names:
            return None
        with zf.open(names[0]) as fh:
            return fh.read()


def imerg_slot(slot_utc: datetime) -> Optional[np.ndarray]:
    """IMERG total precipitation accumulation (mm per 30-min slot) at one
    slot, reprojected to the Stage A grid.  Returns None if the slot file
    isn't on disk."""
    src_path = _imerg_slot_source(slot_utc)
    if src_path is None:
        return None
    path, kind = src_path
    try:
        buf = _imerg_open_total_accum(path, kind)
        if buf is None:
            return None
        with rasterio.MemoryFile(buf) as mem, mem.open() as src:
            arr = src.read(1).astype(np.float64)
            nod = src.nodata
            if nod is not None:
                arr = np.where(arr == nod, np.nan, arr)
            arr = np.where(arr < 0, np.nan, arr)
            arr = arr / _IMERG_SCALE  # integer-scaled → mm
            return _imerg_reproject(src, arr)
    except Exception:
        return None


# ── Antecedent precipitation features (§7.8.1 RF-features refresh) ───────────
# Each call to `imerg_slot` reads a TIF + reprojects, so an LRU is essential
# for both the 6h/24h accumulation features and the hours-since-rain back-walk.
# 320 entries × 30 min ≈ 160 h of history — covers a 72 h rain-search at the
# current slot *plus* a comfortable look-back from the previous slot in a
# sequential training walk (so consecutive slot feature-builds re-use almost
# every IMERG read from the prior slot's back-walk).


@lru_cache(maxsize=320)
def _imerg_slot_cached(slot_utc: datetime) -> Optional[np.ndarray]:
    return imerg_slot(slot_utc)


def imerg_accum_hours(slot_utc: datetime, hours: float,
                      step_min: int = 30) -> np.ndarray:
    """Rolling precipitation accumulation (mm) over the previous `hours`
    ending at (and including) `slot_utc`.  Missing IMERG slots inside the
    window contribute 0 (not NaN) — the alternative would NaN out the whole
    grid whenever a single slot is missing, which is harsher than skipping.
    """
    n_steps = int(round(hours * 60.0 / step_min))
    acc = np.zeros((NLAT, NLON), dtype=np.float32)
    any_finite = np.zeros((NLAT, NLON), dtype=bool)
    for k in range(n_steps + 1):
        t = slot_utc - timedelta(minutes=k * step_min)
        arr = _imerg_slot_cached(t)
        if arr is None:
            continue
        finite = np.isfinite(arr)
        acc = acc + np.where(finite, arr.astype(np.float32), 0.0)
        any_finite |= finite
    return np.where(any_finite, acc, np.nan).astype(np.float32)


def hours_since_rain(slot_utc: datetime,
                     threshold_mm: float = 0.1,
                     max_lookback_h: float = 72.0,
                     step_min: int = 30) -> np.ndarray:
    """Per-cell hours since the last 30-min IMERG slot whose rain was ≥
    `threshold_mm`.  Capped at `max_lookback_h` — cells that haven't seen
    rain in the lookback window get that value.

    Stops the back-walk as soon as every cell has been resolved (typical
    for monsoon season; long for the dry haze episodes).
    """
    out   = np.full((NLAT, NLON), np.float32(max_lookback_h), dtype=np.float32)
    found = np.zeros((NLAT, NLON), dtype=bool)
    n_steps = int(round(max_lookback_h * 60.0 / step_min))
    for k in range(n_steps + 1):
        t = slot_utc - timedelta(minutes=k * step_min)
        arr = _imerg_slot_cached(t)
        if arr is None:
            continue
        hits = (arr >= threshold_mm) & np.isfinite(arr) & ~found
        if hits.any():
            out = np.where(hits, np.float32(k * step_min / 60.0), out)
            found |= hits
            if found.all():
                break
    return out


# ── CAMS lag accessor (§7.8.1 RF-features refresh) ──────────────────────────
# CAMS native cadence is 3-hourly, so a 3 h lag picks up the previous CAMS
# anchor cleanly without smearing through the linear interpolator.

def cams_aod_slot_lagged(slot_utc: datetime, lag_hours: float = 3.0
                          ) -> Optional[np.ndarray]:
    return cams_aod_slot(slot_utc - timedelta(hours=lag_hours))


def _imerg_reproject(src, arr_2d) -> np.ndarray:
    dst = np.full((NLAT, NLON), np.nan, dtype=np.float64)
    dst_transform = rasterio.transform.from_origin(
        LON_MIN, LAT_MAX, abs(LONS[1] - LONS[0]), abs(LATS[0] - LATS[1])
    )
    reproject(
        source=np.where(np.isfinite(arr_2d), arr_2d, 0.0),
        destination=dst,
        src_transform=src.transform, src_crs=src.crs,
        dst_transform=dst_transform, dst_crs='EPSG:4326',
        resampling=Resampling.bilinear,
        src_nodata=np.nan, dst_nodata=np.nan,
    )
    return dst.astype(np.float32)


# ── Static layer: DEM ────────────────────────────────────────────────────────

_DEM_CACHE: Optional[np.ndarray] = None


def dem_grid() -> np.ndarray:
    global _DEM_CACHE
    if _DEM_CACHE is not None:
        return _DEM_CACHE
    tiles = sorted(DEM_DIR.glob('output_*.tif'))
    if not tiles:
        _DEM_CACHE = _nan_grid()
        return _DEM_CACHE
    acc = np.full((NLAT, NLON), np.nan, dtype=np.float64)
    weight = np.zeros((NLAT, NLON), dtype=np.float64)
    dst_transform = rasterio.transform.from_origin(
        LON_MIN, LAT_MAX, abs(LONS[1] - LONS[0]), abs(LATS[0] - LATS[1])
    )
    # Decimate on read: DEM is ~30 m native (~0.0003°) but the Stage A grid is
    # ~5 km (0.05°), so a full-tile read is ~100× more data than we need and
    # blows up RAM (3-7 GB per tile as float64). rasterio handles the average
    # downsampling in C at read time; the rescaled affine matches the decimated
    # array exactly.
    TARGET_RES_DEG = abs(LONS[1] - LONS[0]) / 2.0   # ~2× over-sample of target
    for tile in tiles:
        with rasterio.open(tile) as src:
            sx = abs(src.transform.a)
            sy = abs(src.transform.e)
            decim = max(1, int(min(TARGET_RES_DEG / sx, TARGET_RES_DEG / sy)))
            out_h = max(1, src.height // decim)
            out_w = max(1, src.width  // decim)
            arr = src.read(
                1, out_shape=(out_h, out_w),
                resampling=Resampling.average,
            ).astype(np.float64)
            src_transform = src.transform * rasterio.Affine.scale(
                src.width / out_w, src.height / out_h
            )
            nod = src.nodata
            if nod is not None:
                arr = np.where(arr == nod, np.nan, arr)
            dst = np.full((NLAT, NLON), np.nan, dtype=np.float64)
            reproject(
                source=np.where(np.isfinite(arr), arr, 0.0),
                destination=dst,
                src_transform=src_transform, src_crs=src.crs,
                dst_transform=dst_transform, dst_crs='EPSG:4326',
                resampling=Resampling.average,
                src_nodata=np.nan, dst_nodata=np.nan,
            )
            del arr
            valid = np.isfinite(dst)
            acc = np.where(valid, np.where(np.isfinite(acc), (acc * weight + dst) / (weight + 1), dst), acc)
            weight = weight + valid.astype(np.float64)
    _DEM_CACHE = acc.astype(np.float32)
    return _DEM_CACHE


# ── Static layer: LandScan population ────────────────────────────────────────

_POP_CACHE: dict[object, np.ndarray] = {}


def _landscan_year_from_path(p: Path) -> Optional[int]:
    try:
        return int(p.stem.rsplit('-', 1)[-1])
    except ValueError:
        return None


def landscan_grid(year: Optional[int] = None) -> np.ndarray:
    cache_key = year if year is not None else 'latest'
    if cache_key in _POP_CACHE:
        return _POP_CACHE[cache_key]

    tifs = sorted(LANDSCAN_DIR.glob('landscan-global-*.tif'))
    if not tifs:
        out = _nan_grid()
        _POP_CACHE[cache_key] = out
        return out

    if year is None:
        pick = tifs[-1]
    else:
        on_or_before = [p for p in tifs
                        if (y := _landscan_year_from_path(p)) is not None and y <= year]
        pick = on_or_before[-1] if on_or_before else tifs[0]

    try:
        with rasterio.open(pick) as src:
            window = rasterio.windows.from_bounds(
                LON_MIN - 0.1, LAT_MIN - 0.1, LON_MAX + 0.1, LAT_MAX + 0.1,
                transform=src.transform,
            )
            arr = src.read(1, window=window).astype(np.float64)
            src_transform = src.window_transform(window)
            src_crs = src.crs
            nod = src.nodata
        if nod is not None:
            arr = np.where(arr == nod, np.nan, arr)
        dst = np.full((NLAT, NLON), np.nan, dtype=np.float64)
        dst_transform = rasterio.transform.from_origin(
            LON_MIN, LAT_MAX, abs(LONS[1] - LONS[0]), abs(LATS[0] - LATS[1])
        )
        reproject(
            source=np.where(np.isfinite(arr), arr, 0.0),
            destination=dst,
            src_transform=src_transform, src_crs=src_crs,
            dst_transform=dst_transform, dst_crs='EPSG:4326',
            resampling=Resampling.average,
            src_nodata=np.nan, dst_nodata=np.nan,
        )
        out = dst.astype(np.float32)
    except Exception:
        out = _nan_grid()

    _POP_CACHE[cache_key] = out
    return out


# ── Quasi-static: MODIS NDVI (MOD13Q1, 16-day composites) ────────────────────

_NDVI_CACHE: dict[str, np.ndarray] = {}


def _nearest_ndvi_doy(local_day: date) -> Optional[tuple[int, int]]:
    candidates = []
    for year_dir in NDVI_DIR.iterdir():
        if not year_dir.is_dir():
            continue
        try:
            year = int(year_dir.name)
        except ValueError:
            continue
        for doy_dir in year_dir.iterdir():
            if not doy_dir.is_dir():
                continue
            try:
                doy = int(doy_dir.name)
            except ValueError:
                continue
            d = datetime(year, 1, 1) + timedelta(days=doy - 1)
            if d.date() <= local_day:
                candidates.append((d.date(), year, doy))
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1][1], candidates[-1][2]


def ndvi_grid(local_day: date) -> Optional[np.ndarray]:
    pick = _nearest_ndvi_doy(local_day)
    if pick is None:
        return None
    year, doy = pick
    key = f'{year:04d}{doy:03d}'
    if key in _NDVI_CACHE:
        return _NDVI_CACHE[key]

    folder = NDVI_DIR / f'{year:04d}' / f'{doy:03d}'
    hdfs = sorted(folder.glob('MOD13Q1.A*.hdf'))
    if not hdfs:
        return None

    dst = np.full((NLAT, NLON), np.nan, dtype=np.float64)
    dst_transform = rasterio.transform.from_origin(
        LON_MIN, LAT_MAX, abs(LONS[1] - LONS[0]), abs(LATS[0] - LATS[1])
    )
    for hdf in hdfs:
        sds = f'HDF4_EOS:EOS_GRID:"{hdf}":MODIS_Grid_16DAY_250m_500m_VI:250m 16 days NDVI'
        try:
            with rasterio.open(sds) as src:
                arr = src.read(1).astype(np.float64)
                arr = np.where(arr == -3000, np.nan, arr * 0.0001)
                tmp = np.full((NLAT, NLON), np.nan, dtype=np.float64)
                reproject(
                    source=np.where(np.isfinite(arr), arr, 0.0),
                    destination=tmp,
                    src_transform=src.transform, src_crs=src.crs,
                    dst_transform=dst_transform, dst_crs='EPSG:4326',
                    resampling=Resampling.average,
                    src_nodata=np.nan, dst_nodata=np.nan,
                )
            ok = np.isfinite(tmp)
            dst = np.where(ok, tmp, dst)
        except (rasterio.RasterioIOError, OSError) as exc:
            warnings.warn(f'NDVI read failed for {hdf.name}: {exc}')
            continue
    out = dst.astype(np.float32)
    _NDVI_CACHE[key] = out
    if len(_NDVI_CACHE) > 6:
        _NDVI_CACHE.pop(next(iter(_NDVI_CACHE)))
    return out


# ── Quasi-static: MODIS Land Cover ───────────────────────────────────────────

_LC_CACHE: dict[int, np.ndarray] = {}


def landcover_grid(local_day: date) -> Optional[np.ndarray]:
    year = local_day.year
    if year in _LC_CACHE:
        return _LC_CACHE[year]
    year_dir = LANDCOVER_DIR / f'{year:04d}'
    if not year_dir.exists():
        prior = sorted([int(p.name) for p in LANDCOVER_DIR.iterdir()
                        if p.is_dir() and p.name.isdigit() and int(p.name) <= year])
        if not prior:
            return None
        year_dir = LANDCOVER_DIR / f'{prior[-1]:04d}'
        year = prior[-1]
        if year in _LC_CACHE:
            return _LC_CACHE[year]

    doy_dirs = sorted(year_dir.iterdir())
    if not doy_dirs:
        return None
    hdfs = sorted(doy_dirs[0].glob('MCD12Q1.A*.hdf'))
    if not hdfs:
        return None

    dst = np.full((NLAT, NLON), np.nan, dtype=np.float64)
    dst_transform = rasterio.transform.from_origin(
        LON_MIN, LAT_MAX, abs(LONS[1] - LONS[0]), abs(LATS[0] - LATS[1])
    )
    for hdf in hdfs:
        sds = f'HDF4_EOS:EOS_GRID:"{hdf}":MCD12Q1:LC_Type1'
        try:
            with rasterio.open(sds) as src:
                arr = src.read(1).astype(np.float64)
                tmp = np.full((NLAT, NLON), np.nan, dtype=np.float64)
                reproject(
                    source=arr, destination=tmp,
                    src_transform=src.transform, src_crs=src.crs,
                    dst_transform=dst_transform, dst_crs='EPSG:4326',
                    resampling=Resampling.nearest,
                )
            ok = np.isfinite(tmp) & (tmp > 0)
            dst = np.where(ok, tmp, dst)
        except (rasterio.RasterioIOError, OSError):
            continue
    out = dst.astype(np.float32)
    _LC_CACHE[year] = out
    return out


# ── Static layer: coarse region (§8.2.4c follow-up) ─────────────────────────
# 0 = north, 1 = central, 2 = south.  Lat-band boundaries match the rest of
# the pipeline (Stage A validation strata, AERONET fallback strata).

_REGION_CACHE: Optional[np.ndarray] = None


def region_idx_grid() -> np.ndarray:
    """Static (NLAT, NLON) grid encoding region as 0=north, 1=central, 2=south.

    Trees can split this single column at any threshold, so we don't need
    one-hot — `region_idx <= 0.5` isolates north, `>= 1.5` isolates south.
    """
    global _REGION_CACHE
    if _REGION_CACHE is not None:
        return _REGION_CACHE
    lat_grid = LAT_2D.astype(np.float32)
    out = np.where(lat_grid >= NORTH_CENTRAL_LAT, 0.0,
          np.where(lat_grid <  CENTRAL_SOUTH_LAT, 2.0, 1.0)).astype(np.float32)
    _REGION_CACHE = out
    return out


# ── FIRMS active-fire (§8.2.4c follow-up) ───────────────────────────────────
# MODIS C6.1 FIRMS bundle ships fire_archive_* + fire_nrt_* shapefiles inside
# a single ZIP.  check_ancillary_data.ipynb confirms zero date overlap
# (archive ends 2026-02-28, nrt starts 2026-03-01), so concatenation is safe
# without dedup.  We load once, build a timestamp-sorted DataFrame with
# (lat, lon, acq_utc, frp), and re-use it across all slot queries via a
# per-slot LRU.

_FIRMS_TABLE: Optional[pd.DataFrame] = None  # type: ignore[name-defined]


def _load_firms_table() -> 'pd.DataFrame':
    """Lazy-load the archive+nrt FIRMS table.  Columns: lat, lon, acq_utc, frp.

    `acq_utc` is the detection timestamp built from ACQ_DATE + ACQ_TIME (UTC).
    `frp` is in MW; rows missing FRP get 0 so they still flag presence-of-fire
    in the count-based fallback (we don't use the count path right now, but
    keeping FRP non-NaN avoids selectively dropping detections).
    """
    global _FIRMS_TABLE
    if _FIRMS_TABLE is not None:
        return _FIRMS_TABLE

    import geopandas as gpd  # heavy import — keep inside the loader
    import pandas as pd

    if not FIRMS_ZIP.exists():
        warnings.warn(f'FIRMS zip not found at {FIRMS_ZIP} — fire feature will be 0')
        _FIRMS_TABLE = pd.DataFrame({
            'lat': np.array([], dtype=np.float32),
            'lon': np.array([], dtype=np.float32),
            'acq_utc': pd.to_datetime([]),
            'frp': np.array([], dtype=np.float32),
        })
        return _FIRMS_TABLE

    with zipfile.ZipFile(FIRMS_ZIP) as zf:
        stems = sorted({n.rsplit('.', 1)[0] for n in zf.namelist()
                        if n.lower().endswith('.shp')})

    frames = []
    for stem in stems:
        gdf = gpd.read_file(f'zip://{FIRMS_ZIP}!{stem}.shp')
        if 'ACQ_DATE' not in gdf.columns or 'ACQ_TIME' not in gdf.columns:
            continue
        acq_date = pd.to_datetime(gdf['ACQ_DATE'], errors='coerce')
        # ACQ_TIME is 'HHMM' as a string/int in the MODIS C6.1 schema.
        acq_time = gdf['ACQ_TIME'].astype(str).str.zfill(4)
        hours   = pd.to_numeric(acq_time.str[:2], errors='coerce')
        minutes = pd.to_numeric(acq_time.str[2:], errors='coerce')
        acq_utc = (acq_date
                   + pd.to_timedelta(hours,   unit='h')
                   + pd.to_timedelta(minutes, unit='m'))
        df = pd.DataFrame({
            'lat':     gdf['LATITUDE'].to_numpy(np.float32),
            'lon':     gdf['LONGITUDE'].to_numpy(np.float32),
            'acq_utc': acq_utc,
            'frp':     pd.to_numeric(gdf.get('FRP', 0.0),
                                     errors='coerce').fillna(0.0)
                         .astype(np.float32).to_numpy(),
        })
        df = df.dropna(subset=['acq_utc']).reset_index(drop=True)
        frames.append(df)

    if not frames:
        table = pd.DataFrame({
            'lat': np.array([], dtype=np.float32),
            'lon': np.array([], dtype=np.float32),
            'acq_utc': pd.to_datetime([]),
            'frp': np.array([], dtype=np.float32),
        })
    else:
        table = pd.concat(frames, ignore_index=True)
        table = table.sort_values('acq_utc').reset_index(drop=True)
    _FIRMS_TABLE = table
    return table


# Cell-centre lat/lon arrays, flattened — reused by the KD-tree query.
_CELL_LAT_FLAT = LAT_2D.ravel().astype(np.float64)
_CELL_LON_FLAT = LON_2D.ravel().astype(np.float64)

# Mean-Earth approximation for the 25 km radius in degrees: at this latitude
# range (8–24°N) the latitudinal degree is ~111 km and the longitudinal degree
# is ~106 km, so a flat-Earth KD-tree in (lat, lon * cos(mean_lat)) is accurate
# to <1 % over a 25 km radius — well within the plume-scale fuzziness.
_MEAN_LAT_RAD = np.deg2rad(float((LAT_MIN + LAT_MAX) / 2.0))
_LON_SCALE    = float(np.cos(_MEAN_LAT_RAD))    # multiply lon by this before KD-tree
_DEG_PER_KM   = 1.0 / 111.0                     # rough lat-degree per km


@lru_cache(maxsize=192)
def firms_frp_grid(slot_utc: datetime) -> np.ndarray:
    """Per-cell log(1 + Σ FRP) over fires within FIRMS_RADIUS_KM and the
    past FIRMS_LOOKBACK_H ending at (and including) `slot_utc`.

    Returns a finite (NLAT, NLON) float32 grid — cells with no nearby fire
    in the window get 0.0 (not NaN), so the RF can use this as a "smoke
    pressure" intensity without dragging NaNs into the imputer.

    The expensive piece is the spatial join (sum FRP within radius for every
    cell).  We use a cKDTree on (lat, lon*cos(mean_lat)) which is accurate to
    <1 % over a 25 km radius at Vietnam latitudes.
    """
    table = _load_firms_table()
    grid = np.zeros((NLAT, NLON), dtype=np.float32)
    if table.empty:
        return grid

    t_end   = slot_utc
    t_start = slot_utc - timedelta(hours=FIRMS_LOOKBACK_H)

    # Slice the time window first — chops a 305k-row table down to typically
    # a few hundred fires per 24 h window over Vietnam.
    acq = table['acq_utc'].to_numpy()
    mask = (acq > np.datetime64(t_start)) & (acq <= np.datetime64(t_end))
    if not mask.any():
        return grid
    fires = table.loc[mask, ['lat', 'lon', 'frp']].to_numpy(dtype=np.float64)
    fire_lat = fires[:, 0]
    fire_lon = fires[:, 1]
    fire_frp = fires[:, 2].astype(np.float32)

    grid_flat = _firms_aggregate_frp(
        fire_lat, fire_lon, fire_frp,
        cell_lat=_CELL_LAT_FLAT, cell_lon=_CELL_LON_FLAT,
        radius_km=FIRMS_RADIUS_KM,
    )
    grid = np.log1p(grid_flat).reshape(NLAT, NLON).astype(np.float32)
    return grid


def _firms_aggregate_frp(
    fire_lat: np.ndarray, fire_lon: np.ndarray, fire_frp: np.ndarray,
    cell_lat: np.ndarray, cell_lon: np.ndarray,
    radius_km: float,
) -> np.ndarray:
    """For each cell, sum FRP of all fires within `radius_km`.

    Inputs are float64 1D arrays of fire detections (already pre-filtered to
    the time window) and 1D arrays of cell centres (NLAT*NLON entries).
    Output is a 1D float32 array of length NCELL with the per-cell FRP sum
    (0.0 where no fire is within radius).

    Strategy: build the cKDTree on the (small) fire set, query from the
    (large but static) cell array.  Per-fire neighbour-list extraction is
    flattened into (cell_idx, fire_idx) pairs and summed via np.bincount —
    O(total-pairs) with no Python-loop accumulation.
    """
    from scipy.spatial import cKDTree

    n_cells = int(cell_lat.shape[0])
    if fire_lat.size == 0:
        return np.zeros(n_cells, dtype=np.float32)

    fire_pts = np.column_stack([fire_lat, fire_lon * _LON_SCALE])
    cell_pts = np.column_stack([cell_lat, cell_lon * _LON_SCALE])

    tree = cKDTree(fire_pts)
    radius_deg = float(radius_km) * _DEG_PER_KM

    # query_ball_point with a vector of query points returns a list of length
    # n_cells, each element a list of fire indices within radius_deg.
    neighbour_lists = tree.query_ball_point(cell_pts, r=radius_deg)

    # Flatten into parallel arrays (cell_idx repeated, fire_idx concatenated)
    # so the accumulation is a single np.bincount call rather than a Python
    # for-loop over cells.
    counts = np.fromiter((len(lst) for lst in neighbour_lists),
                         dtype=np.int64, count=n_cells)
    total_pairs = int(counts.sum())
    if total_pairs == 0:
        return np.zeros(n_cells, dtype=np.float32)

    cell_ids = np.repeat(np.arange(n_cells, dtype=np.int64), counts)
    fire_ids = np.fromiter(
        (fi for lst in neighbour_lists for fi in lst),
        dtype=np.int64, count=total_pairs,
    )

    out = np.bincount(
        cell_ids,
        weights=fire_frp[fire_ids].astype(np.float64),
        minlength=n_cells,
    )
    return out.astype(np.float32)
