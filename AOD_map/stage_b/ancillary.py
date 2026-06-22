"""Stage B ancillary covariate loaders (per-slot, v3.4.0+stage_b_fixes).

Every dynamic accessor returns a (NLAT, NLON) float32 grid for one **UTC
slot timestamp** (a `datetime` rounded to a 30-min boundary).  Static
accessors take no time argument and are cached after first call.

Temporal handling (§3 of stage_b_fixes.md):
- ERA5 hourly → 30-min slot via **linear interpolation** between flanking
  hourly fields.  Slot at HH:30 = 0.5 × ERA5(HH:00) + 0.5 × ERA5(HH+1:00).
- CAMS 3-hourly → 30-min via linear interpolation in time between flanking
  3-hour fields.
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
    ERA5_MONTHLY_DIR, CAMS_MONTHLY_DIR,
    NDVI_DIR, LANDCOVER_DIR, LANDSCAN_DIR, DEM_DIR, IMERG_DIR,
)

os.environ.setdefault('HDF5_USE_FILE_LOCKING', 'FALSE')

_TARGET_LAT = LATS
_TARGET_LON = LONS
LAT_2D, LON_2D = np.meshgrid(_TARGET_LAT, _TARGET_LON, indexing='ij')


def _interp_to_grid(src_lat: np.ndarray, src_lon: np.ndarray,
                    field: np.ndarray) -> np.ndarray:
    """Bilinearly interpolate a source field to the 0.05° Stage A grid."""
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
    pts = np.column_stack([LAT_2D.ravel(), LON_2D.ravel()])
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
    field = (1 - w) * a_b + w * a_a
    lat = ds_b['latitude'].values.astype(np.float64)
    lon = ds_b['longitude'].values.astype(np.float64)
    return _interp_to_grid(lat, lon, field)


# ── IMERG (30-min GeoTIFFs zipped, native to our slot cadence) ───────────────

def _imerg_slot_zip(slot_utc: datetime) -> Optional[Path]:
    """Locate the IMERG zip for this slot. IMERG file names use the slot's
    start minute (HHMM) as the half-hour anchor."""
    folder = IMERG_DIR / f'{slot_utc.year:04d}' / f'{slot_utc.month:02d}' / f'{slot_utc.day:02d}'
    if not folder.exists():
        return None
    tag = slot_utc.strftime('%Y%m%d')
    minute_of_day = slot_utc.hour * 60 + slot_utc.minute
    hh = slot_utc.strftime('%H%M')
    candidates = sorted(folder.glob(f'*{tag}*{hh}*.zip'))
    if not candidates:
        # IMERG often labels by start minute-of-day "S{HHMMSS}-E…".  Fall back
        # to a broader match on start-second.
        ss = f'S{slot_utc.hour:02d}{slot_utc.minute:02d}00'
        candidates = sorted(folder.glob(f'*{ss}*.zip'))
    return candidates[0] if candidates else None


def imerg_slot(slot_utc: datetime) -> Optional[np.ndarray]:
    """IMERG precipitation rate (mm/hr) at one 30-min slot, reprojected to the
    Stage A grid.  Returns None if the file isn't on disk for that slot."""
    z = _imerg_slot_zip(slot_utc)
    if z is None:
        return None
    try:
        with zipfile.ZipFile(z) as zf:
            tifs = [n for n in zf.namelist() if n.lower().endswith('.tif')]
            if not tifs:
                return None
            with zf.open(tifs[0]) as fh:
                buf = fh.read()
        with rasterio.MemoryFile(buf) as mem, mem.open() as src:
            arr = src.read(1).astype(np.float64)
            nod = src.nodata
            if nod is not None:
                arr = np.where(arr == nod, np.nan, arr)
            arr = np.where(arr < 0, np.nan, arr)
            return _imerg_reproject(src, arr)
    except Exception:
        return None


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
