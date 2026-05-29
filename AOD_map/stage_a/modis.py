"""Read MODIS MCD19A2 MAIAC HDF files and apply Step A1 QA filters.

Primary SDS used:
    Optical_Depth_055  – MAIAC AOD at 550 nm (1 km, multi-orbit)
    AOD_QA             – quality bitmask (16-bit)
    AngstromExp_470-780
    cosSZA / cosVZA    – 5 km grid (resampled to 1 km below)
    Injection_Height   – (unused in A1 but loaded for future use)

Step A1 filter:
    • AOD_QA bits 3–4 = 0 (not cloud-adjacent, best algorithm quality)
    • Valid AOD range: 0 ≤ AOD ≤ 5

File naming:
    MCD19A2.A{YYYY}{DOY}.h{HH}v{VV}.061.{proc_time}.hdf

Tile coverage for Vietnam domain (8–24°N, 100–110°E):
    h27v06, h27v07, h27v08,
    h28v06, h28v07, h28v08
"""

from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import glob
import warnings
from typing import Optional

import numpy as np
import pyproj
import rasterio
import rioxarray
from rasterio.transform import rowcol

from config import (
    MODIS_DIR,
    MODIS_QA_BIT_MASK,
    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
)

# MODIS sinusoidal projection
_SINU_CRS = '+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs'
_WGS84_TO_SINU = pyproj.Transformer.from_crs('EPSG:4326', _SINU_CRS, always_xy=True)
_SINU_TO_WGS84 = pyproj.Transformer.from_crs(_SINU_CRS, 'EPSG:4326', always_xy=True)

# Vietnam tile list
_VN_TILES = ('h27v06', 'h27v07', 'h27v08', 'h28v06', 'h28v07', 'h28v08')

# 5 km SDS names (sub-sampled from 1 km grid)
_GRID5KM_SDS = frozenset({'cosSZA', 'cosVZA', 'RelAZ', 'Scattering_Angle', 'Glint_Angle'})


def _modis_files_for_date(date: datetime) -> list[Path]:
    """Return all MCD19A2 HDF files covering Vietnam for the given date."""
    year = date.strftime('%Y')
    doy  = f'{date.timetuple().tm_yday:03d}'
    pattern = str(MODIS_DIR / year / doy / 'MCD19A2.*.hdf')
    all_files = glob.glob(pattern)
    return [
        Path(f) for f in all_files
        if any(tile in Path(f).name for tile in _VN_TILES)
    ]


def _load_sds(hdf_path: str, sds_name: str) -> tuple[np.ndarray, dict]:
    """Load an SDS from a MAIAC HDF file, applying scale/fill and returning float array."""
    grid = 'grid5km' if sds_name in _GRID5KM_SDS else 'grid1km'
    path = f'HDF4_EOS:EOS_GRID:"{hdf_path}":{grid}:{sds_name}'
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', message='.*no geotransform.*')
        da = rioxarray.open_rasterio(path, masked=False, lock=False)
    attrs = dict(da.attrs)
    data  = da.values.astype(float)  # (bands, H, W) or (H, W)
    if data.ndim == 3 and data.shape[0] == 1:
        data = data.squeeze(0)

    fill_val = attrs.get('_FillValue')
    vrange   = attrs.get('valid_range')
    scale    = float(attrs.get('scale_factor', 1.0))
    offset   = float(attrs.get('add_offset',   0.0))

    if fill_val is not None:
        data[data == float(fill_val)] = np.nan
    if vrange is not None:
        try:
            lo, hi = float(vrange[0]), float(vrange[1])
            data[(data < lo) | (data > hi)] = np.nan
        except (TypeError, IndexError):
            pass

    data = data * scale + offset
    return data, attrs


def _tile_pixel_latlon(hdf_path: str) -> tuple[np.ndarray, np.ndarray]:
    """Build per-pixel WGS-84 lat/lon arrays for the 1 km grid of a tile."""
    ref_sds = f'HDF4_EOS:EOS_GRID:"{hdf_path}":grid1km:Optical_Depth_055'
    with rasterio.open(ref_sds) as src:
        rows, cols = np.indices((src.height, src.width))
        rows_flat  = rows.ravel()
        cols_flat  = cols.ravel()
        sinu_x, sinu_y = rasterio.transform.xy(src.transform, rows_flat, cols_flat)
    lon_flat, lat_flat = _SINU_TO_WGS84.transform(np.array(sinu_x), np.array(sinu_y))
    lat = lat_flat.reshape(rows.shape).astype(np.float32)
    lon = lon_flat.reshape(cols.shape).astype(np.float32)
    return lat, lon


def _read_modis_tile(hdf_path: str) -> Optional[dict[str, np.ndarray]]:
    """Extract QA-filtered pixel data from one MCD19A2 HDF tile.

    Returns None if no valid pixels overlap the Vietnam domain.
    Returns a dict with flat 1-D arrays: lat, lon, aod, ae, sza, vza, orbit_idx.
    """
    try:
        aod_3d, _  = _load_sds(hdf_path, 'Optical_Depth_055')   # (n_orbits, H, W) or (H, W)
        qa_3d, _   = _load_sds(hdf_path, 'AOD_QA')
        ae_3d, _   = _load_sds(hdf_path, 'AngstromExp_470-780')
        cos_sza, _ = _load_sds(hdf_path, 'cosSZA')  # 5 km → same shape after resize
        cos_vza, _ = _load_sds(hdf_path, 'cosVZA')
    except Exception:
        return None

    # Ensure 3-D (orbits, H, W) for multi-orbit SDS
    for name, arr in (('aod', aod_3d), ('qa', qa_3d), ('ae', ae_3d)):
        pass  # just checking; reshape below
    if aod_3d.ndim == 2:
        aod_3d = aod_3d[np.newaxis]
        qa_3d  = qa_3d[np.newaxis]
        ae_3d  = ae_3d[np.newaxis]

    n_orbits, H, W = aod_3d.shape

    # Resize 5 km SZA/VZA to 1 km (repeat nearest, integer factor ~5×)
    from scipy.ndimage import zoom
    factor_h = H / cos_sza.shape[0]
    factor_w = W / cos_sza.shape[1]
    cos_sza = zoom(cos_sza, (factor_h, factor_w), order=0)
    cos_vza = zoom(cos_vza, (factor_h, factor_w), order=0)
    cos_sza = cos_sza[:H, :W]
    cos_vza = cos_vza[:H, :W]

    # Build lat/lon for tile pixels
    try:
        lat, lon = _tile_pixel_latlon(hdf_path)
    except Exception:
        return None

    # Domain mask (pre-filter before looping over orbits)
    domain = (
        (lat >= LAT_MIN) & (lat <= LAT_MAX)
        & (lon >= LON_MIN) & (lon <= LON_MAX)
    )
    if not np.any(domain):
        return None

    parts: list[dict] = []
    for orb in range(n_orbits):
        aod_2d = aod_3d[orb]
        qa_2d  = qa_3d[orb].astype(np.int32)
        ae_2d  = ae_3d[orb]

        # Step A1: bits 3–4 of AOD_QA must both be 0
        qa_ok = (qa_2d & MODIS_QA_BIT_MASK) == 0

        # Valid AOD range
        aod_ok = ~np.isnan(aod_2d) & (aod_2d >= 0.0) & (aod_2d <= 5.0)

        valid = domain & qa_ok & aod_ok

        if not np.any(valid):
            continue

        sza_deg = np.degrees(np.arccos(np.clip(cos_sza, -1, 1)))
        vza_deg = np.degrees(np.arccos(np.clip(cos_vza, -1, 1)))

        parts.append({
            'lat': lat[valid].ravel(),
            'lon': lon[valid].ravel(),
            'aod': aod_2d[valid].ravel().astype(np.float32),
            'ae':  ae_2d[valid].ravel().astype(np.float32),
            'sza': sza_deg[valid].ravel().astype(np.float32),
            'vza': vza_deg[valid].ravel().astype(np.float32),
            'orbit_idx': np.full(valid.sum(), orb, dtype=np.int8),
        })

    if not parts:
        return None

    combined: dict[str, np.ndarray] = {}
    for key in parts[0]:
        combined[key] = np.concatenate([p[key] for p in parts])
    return combined


def read_modis_date(date: datetime) -> Optional[dict[str, np.ndarray]]:
    """Return QA-filtered MODIS MAIAC pixel arrays for all Vietnam tiles on a date.

    Returns a dict with flat 1-D arrays:
        lat, lon, aod, ae, sza, vza, orbit_idx
    or None if no valid data found.
    """
    files = _modis_files_for_date(date)
    if not files:
        return None

    parts = []
    for fpath in files:
        px = _read_modis_tile(str(fpath))
        if px is not None:
            parts.append(px)

    if not parts:
        return None

    combined: dict[str, np.ndarray] = {}
    for key in parts[0]:
        combined[key] = np.concatenate([p[key] for p in parts])
    return combined
