"""Read MODIS MCD19A2 MAIAC HDF files.

Primary SDS used:
    Optical_Depth_055  – MAIAC AOD at 550 nm (1 km, multi-orbit)
    AngstromExp_470-780
    cosSZA / cosVZA    – 5 km grid (resampled to 1 km below)

Pixel validity filter:
    • Valid AOD range: 0 ≤ AOD ≤ 5
    (AOD_QA bit-mask filtering was removed — see commit c8be990; box heterogeneity
     rejection at extraction time has proven sufficient for MAIAC over Vietnam.)

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

import re

import numpy as np
import pyproj
import rasterio
import rioxarray
from rasterio.transform import rowcol

from config import (
    MODIS_DIR,
    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
)

# MODIS sinusoidal projection
_SINU_CRS = '+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs'
_WGS84_TO_SINU = pyproj.Transformer.from_crs('EPSG:4326', _SINU_CRS, always_xy=True)
_SINU_TO_WGS84 = pyproj.Transformer.from_crs(_SINU_CRS, 'EPSG:4326', always_xy=True)

# Cache lat/lon grids per tile ID (e.g. h27v06). MODIS tiles sit on a fixed
# sinusoidal grid, so the per-pixel lat/lon never changes across dates.
# Without this cache, _tile_pixel_latlon transforms ~1.44M pixels per call.
_TILE_LATLON_CACHE: dict[str, tuple[np.ndarray, np.ndarray]] = {}

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


def _read_modis_orbit_times(hdf_path: str) -> list[datetime]:
    """Parse per-orbit UTC datetimes from MCD19A2 HDF global attribute.

    The ``Orbit_time_stamp`` attribute looks like::

        20250010220T  20250010700A  20250010705A

    Each whitespace-delimited token has the format YYYYDDDHHMM{T|A}, where
    DDD is day-of-year, and the trailing letter identifies the platform
    (T = Terra, A = Aqua).  Returns an empty list on any error or if the
    attribute is absent (caller should fall back to daily matching).
    """
    stamp = ''

    # Preferred path: pyhdf gives direct access to HDF4 global attributes
    try:
        from pyhdf.SD import SD, SDC
        hdf = SD(hdf_path, SDC.READ)
        _raw = hdf.attributes().get('Orbit_time_stamp', '')
        stamp = (_raw.decode('utf-8') if isinstance(_raw, bytes) else str(_raw)).strip()
        hdf.end()
    except Exception:
        pass

    # Fallback: GDAL metadata via rasterio on the raw HDF4 file.
    # Must open the raw file path (not an EOS_GRID subdataset path) because
    # Orbit_time_stamp is a file-level global attribute; subdataset handles
    # only expose grid-scoped metadata.
    if not stamp:
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', message='.*no geotransform.*')
                with rasterio.open(hdf_path) as src:
                    for domain in (None, 'EOS_METADATA', 'HDF_GLOBAL'):
                        tags = src.tags() if domain is None else src.tags(domain)
                        if 'Orbit_time_stamp' in tags:
                            _raw = tags['Orbit_time_stamp']
                            stamp = (_raw.decode('utf-8') if isinstance(_raw, bytes) else str(_raw)).strip()
                            break
        except Exception:
            pass

    times: list[datetime] = []
    for token in stamp.split():
        # Token: YYYYDDDHHMM{T|A} — 12 characters total
        if len(token) < 12:
            continue
        try:
            year   = int(token[0:4])
            doy    = int(token[4:7])
            hour   = int(token[7:9])
            minute = int(token[9:11])
            times.append(
                datetime(year, 1, 1)
                + timedelta(days=doy - 1, hours=hour, minutes=minute)
            )
        except (ValueError, IndexError):
            continue
    return times


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
        except (TypeError, IndexError, ValueError):
            pass

    data = data * scale + offset
    return data, attrs


def _tile_pixel_latlon(hdf_path: str) -> tuple[np.ndarray, np.ndarray]:
    """Build per-pixel WGS-84 lat/lon arrays for the 1 km grid of a tile.

    Results are cached by tile ID (e.g. h27v06): the sinusoidal grid position
    of a tile is fixed across all acquisition dates, so this expensive
    ~1.44M-pixel transform only runs once per unique tile per process.
    """
    m = re.search(r'\.(h\d{2}v\d{2})\.', Path(hdf_path).name)
    cache_key = m.group(1) if m else hdf_path
    if cache_key in _TILE_LATLON_CACHE:
        return _TILE_LATLON_CACHE[cache_key]

    ref_sds = f'HDF4_EOS:EOS_GRID:"{hdf_path}":grid1km:Optical_Depth_055'
    with rasterio.open(ref_sds) as src:
        rows, cols = np.indices((src.height, src.width))
        rows_flat  = rows.ravel()
        cols_flat  = cols.ravel()
        sinu_x, sinu_y = rasterio.transform.xy(src.transform, rows_flat, cols_flat)
    lon_flat, lat_flat = _SINU_TO_WGS84.transform(np.array(sinu_x), np.array(sinu_y))
    lat = lat_flat.reshape(rows.shape).astype(np.float32)
    lon = lon_flat.reshape(cols.shape).astype(np.float32)

    _TILE_LATLON_CACHE[cache_key] = (lat, lon)
    return lat, lon


def _read_modis_tile(
    hdf_path: str,
    orbit_indices: Optional[list[int]] = None,
) -> Optional[dict[str, np.ndarray]]:
    """Extract QA-filtered pixel data from one MCD19A2 HDF tile.

    Parameters
    ----------
    hdf_path      : path to the MCD19A2 HDF file
    orbit_indices : if given, only process these orbit layers (0-based index
                    matching the order in ``_read_modis_orbit_times``).
                    Pass ``None`` to include all orbits.

    Returns None if no valid pixels overlap the Vietnam domain.
    Returns a dict with flat 1-D arrays: lat, lon, aod, ae, sza, vza, orbit_idx, orbit_utc_sec.
    """
    try:
        aod_3d, _  = _load_sds(hdf_path, 'Optical_Depth_055')   # (n_orbits, H, W) or (H, W)
        ae_3d, _   = _load_sds(hdf_path, 'AngstromExp_470-780')
        cos_sza, _ = _load_sds(hdf_path, 'cosSZA')  # 5 km → same shape after resize
        cos_vza, _ = _load_sds(hdf_path, 'cosVZA')
    except Exception:
        return None

    # Ensure 3-D (orbits, H, W) for multi-orbit SDS
    if aod_3d.ndim == 2:
        aod_3d = aod_3d[np.newaxis]
        ae_3d  = ae_3d[np.newaxis]

    n_orbits, H, W = aod_3d.shape

    # Guard against out-of-range indices silently
    if orbit_indices is not None:
        orbit_indices = [i for i in orbit_indices if 0 <= i < n_orbits]
        if not orbit_indices:
            return None

    # Resize 5 km SZA/VZA to 1 km (repeat nearest, integer factor ~5×)
    from scipy.ndimage import zoom
    if cos_sza.ndim == 3:
        cos_sza = np.nanmean(cos_sza, axis=0)
    if cos_vza.ndim == 3:
        cos_vza = np.nanmean(cos_vza, axis=0)
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

    orbit_utc_list = _read_modis_orbit_times(hdf_path)

    parts: list[dict] = []
    _orb_iter = orbit_indices if orbit_indices is not None else range(n_orbits)
    for orb in _orb_iter:
        aod_2d = aod_3d[orb]
        ae_2d  = ae_3d[orb]

        valid = domain & ~np.isnan(aod_2d) & (aod_2d >= 0.0) & (aod_2d <= 5.0)

        if not np.any(valid):
            continue

        sza_deg = np.degrees(np.arccos(np.clip(cos_sza, -1, 1)))
        vza_deg = np.degrees(np.arccos(np.clip(cos_vza, -1, 1)))

        orb_t = orbit_utc_list[orb] if orb < len(orbit_utc_list) else None
        orb_t_sec = orb_t.timestamp() if orb_t is not None else np.nan

        parts.append({
            'lat': lat[valid].ravel(),
            'lon': lon[valid].ravel(),
            'aod': aod_2d[valid].ravel().astype(np.float32),
            'ae':  ae_2d[valid].ravel().astype(np.float32),
            'sza': sza_deg[valid].ravel().astype(np.float32),
            'vza': vza_deg[valid].ravel().astype(np.float32),
            'orbit_idx': np.full(valid.sum(), orb, dtype=np.int8),
            'orbit_utc_sec': np.full(valid.sum(), orb_t_sec, dtype=np.float64),
        })

    if not parts:
        return None

    combined: dict[str, np.ndarray] = {}
    for key in parts[0]:
        combined[key] = np.concatenate([p[key] for p in parts])
    return combined


def _tile_station_rowcol(
    hdf_path: str,
    station_lat: float,
    station_lon: float,
) -> 'tuple[int, int, int, int] | None':
    """Return (row, col, height, width) of station's grid cell in this tile, or None.

    Converts WGS-84 station coordinates to MODIS sinusoidal projection and
    looks up the 1 km pixel row/col via the tile's rasterio transform.
    Returns None if the station falls outside the tile bounds.
    """
    ref_sds = f'HDF4_EOS:EOS_GRID:"{hdf_path}":grid1km:Optical_Depth_055'
    try:
        sinu_x, sinu_y = _WGS84_TO_SINU.transform(station_lon, station_lat)
        with rasterio.open(ref_sds) as src:
            rows, cols = rowcol(src.transform, [sinu_x], [sinu_y])
            r, c = int(rows[0]), int(cols[0])
            if 0 <= r < src.height and 0 <= c < src.width:
                return r, c, src.height, src.width
    except Exception:
        pass
    return None


def _read_modis_tile_aod2d(
    hdf_path: str,
) -> 'tuple[list, list[np.ndarray], list[tuple[np.ndarray, np.ndarray]]] | None':
    """Read per-orbit 2D QA-filtered AOD arrays from one MAIAC tile.

    Returns (orbit_times, aod_2d_list, angle_list) or None on failure:
        orbit_times[i]  : UTC datetime or None (no timestamp available)
        aod_2d_list[i]  : (H, W) float32 — NaN where QA-rejected or out of range
        angle_list[i]   : (vza_2d, sza_2d), both (H, W) float32; NaN if unavailable

    Orbit timestamps come from _read_modis_orbit_times; when unavailable the list
    is padded with None so callers can use a single fallback key for the day.
    VZA/SZA are shared across orbits (5 km grid resampled to 1 km via nearest-neighbour).
    """
    orbit_times = _read_modis_orbit_times(hdf_path)
    try:
        aod_3d, _ = _load_sds(hdf_path, 'Optical_Depth_055')
    except Exception:
        return None

    if aod_3d.ndim == 2:
        aod_3d = aod_3d[np.newaxis]

    n_orbits, H, W = aod_3d.shape

    # Resize 5 km VZA/SZA grids to 1 km (shared across orbits)
    try:
        cos_vza, _ = _load_sds(hdf_path, 'cosVZA')
        cos_sza, _ = _load_sds(hdf_path, 'cosSZA')
        from scipy.ndimage import zoom as _zoom
        fh = H / cos_vza.shape[0]
        fw = W / cos_vza.shape[1]
        vza_2d = np.degrees(np.arccos(np.clip(
            _zoom(cos_vza, (fh, fw), order=0)[:H, :W], -1.0, 1.0
        ))).astype(np.float32)
        sza_2d = np.degrees(np.arccos(np.clip(
            _zoom(cos_sza, (fh, fw), order=0)[:H, :W], -1.0, 1.0
        ))).astype(np.float32)
    except Exception:
        vza_2d = np.full((H, W), np.nan, dtype=np.float32)
        sza_2d = np.full((H, W), np.nan, dtype=np.float32)

    # Align orbit_times length to n_orbits
    if len(orbit_times) < n_orbits:
        orbit_times = orbit_times + [None] * (n_orbits - len(orbit_times))
    else:
        orbit_times = orbit_times[:n_orbits]

    aod_2d_list: list[np.ndarray] = []
    angle_list: list[tuple[np.ndarray, np.ndarray]] = []

    for orb in range(n_orbits):
        aod_2d = aod_3d[orb].copy().astype(np.float32)
        valid  = ~np.isnan(aod_2d) & (aod_2d >= 0.0) & (aod_2d <= 5.0)
        aod_2d[~valid] = np.nan
        aod_2d_list.append(aod_2d)
        angle_list.append((vza_2d, sza_2d))

    return orbit_times, aod_2d_list, angle_list


def read_modis_date(date: datetime) -> Optional[dict[str, np.ndarray]]:
    """Return valid MODIS MAIAC pixel arrays for all Vietnam tiles on a date.

    Returns a dict with flat 1-D arrays:
        lat, lon, aod, ae, sza, vza, orbit_idx, orbit_utc_sec
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


def filter_modis_slot(
    pixels: dict[str, np.ndarray],
    slot_utc: datetime,
    window_min: int = 30,
) -> Optional[dict[str, np.ndarray]]:
    """Return the subset of pixels whose orbit time falls within ±window_min of slot_utc.

    Falls back to returning all pixels when orbit timestamps are unavailable
    (NaN orbit_utc_sec for every pixel), preserving pre-fix behaviour for files
    where Orbit_time_stamp is absent or unreadable.
    Returns None when timestamps are available but no pixel falls in the window.
    """
    utc_sec = pixels.get('orbit_utc_sec')
    if utc_sec is None:
        return pixels

    has_time = np.isfinite(utc_sec)
    if not np.any(has_time):
        return pixels  # no orbit info — include all pixels (fallback)

    slot_sec = slot_utc.timestamp()
    window_sec = window_min * 60.0
    in_window = has_time & (np.abs(utc_sec - slot_sec) <= window_sec)
    if not np.any(in_window):
        return None

    return {k: v[in_window] for k, v in pixels.items()}
