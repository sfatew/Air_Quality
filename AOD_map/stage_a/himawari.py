"""Read Himawari-8/9 AHI L2 and L3 GeoTIFFs and apply Step A1 QA filters.

L2 GeoTIFF band layout (6 bands):
    1: AOT        – AOD at 500 nm (NaN = no retrieval)
    2: Uncertainty – signed retrieval uncertainty
    3: AE          – Ångström exponent
    4: QA_flag     – bitmask (100% populated)
    5: SSA         – single-scattering albedo
    6: RF          – fine-mode fraction

L3 GeoTIFF band layout (15 bands, hourly composite):
    2:  AOT_Merged
    3:  AOT_Pure
    4:  AOT_Merged_uncertainty
    5:  AOT_Pure_uncertainty
    6:  AE_Merged
    7:  AE_Pure
    8:  QA_flag_Merged
    9:  QA_flag_Pure
    10: AOT_L2_Mean
    11: AOT_L2_SDV
    12: AOT_L2_Num
    13: AE_L2_Mean
    14: AE_L2_SDV
    15: AE_L2_Num
"""

from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import glob
import re
from typing import Optional

import numpy as np
import rasterio

from config import (
    HIMAWARI_L2_DIR, HIMAWARI_L3_DIR,
    HIMAWARI_RF_MIN, HIMAWARI_UNC_MAX,
    HIMAWARI_WET_RF_MIN, HIMAWARI_WET_UNC_MAX,
    HIMAWARI_SZA_MAX, HIMAWARI_VZA_MAX, HIMAWARI_VZA_SOFT,
    HIMAWARI_SAT_LON, EARTH_RADIUS_KM, GEO_ORBIT_KM,
    WET_MONTHS,
    LATS, LONS, NLAT, NLON, GRID_RES, LAT_MAX, LON_MIN,
)


def _himawari_qa_thresholds(month: int) -> tuple[float, float]:
    """Return (rf_min, unc_max) for the given calendar month.

    Wet months (May–Sep, config.WET_MONTHS) use tighter thresholds to suppress
    cloud-edge contamination that dominates Himawari error during monsoon
    season (thesis §7.5 wet-season QA, replaces the v3.0 weight-factor band-aid).
    """
    if month in WET_MONTHS:
        return HIMAWARI_WET_RF_MIN, HIMAWARI_WET_UNC_MAX
    return HIMAWARI_RF_MIN, HIMAWARI_UNC_MAX

# ── TIF → config grid embedding ───────────────────────────────────────────────
# The Himawari TIF covers a subdomain (102.1–109.5°E, 8.35–23.4°N) and its
# pixel centres align with the config 0.05° grid at a fixed offset.
# Computed once at module load from a representative file.
_TIF_ROW_OFFSET: Optional[int] = None
_TIF_COL_OFFSET: Optional[int] = None
_TIF_HEIGHT: Optional[int] = None
_TIF_WIDTH: Optional[int] = None


def _get_tif_grid_offset(fpath: str) -> tuple[int, int, int, int]:
    """Return (row_start, col_start, H, W) for embedding TIF into the config grid."""
    global _TIF_ROW_OFFSET, _TIF_COL_OFFSET, _TIF_HEIGHT, _TIF_WIDTH
    if _TIF_ROW_OFFSET is not None:
        return _TIF_ROW_OFFSET, _TIF_COL_OFFSET, _TIF_HEIGHT, _TIF_WIDTH

    with rasterio.open(fpath) as src:
        H, W = src.height, src.width
        # First pixel centre (top-left)
        xs, ys = rasterio.transform.xy(src.transform, [0], [0], offset='center')
        tif_top_lat = float(ys[0])
        tif_left_lon = float(xs[0])

    row_start = int(round((LAT_MAX - GRID_RES / 2 - tif_top_lat) / GRID_RES))
    col_start = int(round((tif_left_lon - LON_MIN - GRID_RES / 2) / GRID_RES))

    _TIF_ROW_OFFSET = row_start
    _TIF_COL_OFFSET = col_start
    _TIF_HEIGHT     = H
    _TIF_WIDTH      = W
    return row_start, col_start, H, W

# Fill sentinel values used in the GeoTIFF
_FILL_VALUES = {-9999.0, -999.0, 9999.0}

# ── Geometry helpers ──────────────────────────────────────────────────────────

def compute_himawari_vza(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    """Viewing zenith angle (degrees) for Himawari pixels.

    Uses the exact geostationary projection formula.
    Satellite at (0°N, HIMAWARI_SAT_LON°E), altitude GEO_ORBIT_KM from Earth centre.
    """
    Re = EARTH_RADIUS_KM
    H  = GEO_ORBIT_KM
    lat_r  = np.radians(lat)
    dlon_r = np.radians(lon - HIMAWARI_SAT_LON)
    # Central angle between sub-satellite point and pixel
    gamma = np.arccos(np.clip(np.cos(lat_r) * np.cos(dlon_r), -1.0, 1.0))
    # Slant range from satellite to pixel
    slant = np.sqrt(Re**2 + H**2 - 2 * Re * H * np.cos(gamma))
    sin_vza = H * np.sin(gamma) / slant
    return np.degrees(np.arcsin(np.clip(sin_vza, -1.0, 1.0)))


def compute_sza(lat: np.ndarray, lon: np.ndarray, utc_dt: datetime) -> np.ndarray:
    """Approximate solar zenith angle (degrees) using the Iqbal (1983) formula."""
    doy = utc_dt.timetuple().tm_yday
    B   = 2 * np.pi * (doy - 1) / 365.0
    # Solar declination
    delta = np.radians(
        23.45 * np.sin(np.radians(360.0 * (284 + doy) / 365.0))
    )
    # Equation of time (minutes)
    eot = 229.18 * (
        0.000075 + 0.001868 * np.cos(B) - 0.032077 * np.sin(B)
        - 0.014615 * np.cos(2 * B) - 0.04089 * np.sin(2 * B)
    )
    utc_h  = utc_dt.hour + utc_dt.minute / 60.0
    solar_h = utc_h + lon / 15.0 + eot / 60.0
    omega   = np.radians(15.0 * (solar_h - 12.0))
    lat_r   = np.radians(lat)
    cos_sza = np.sin(lat_r) * np.sin(delta) + np.cos(lat_r) * np.cos(delta) * np.cos(omega)
    return np.degrees(np.arccos(np.clip(cos_sza, -1.0, 1.0)))


# ── Filename helpers ──────────────────────────────────────────────────────────

def _parse_l2_utc(fname: str) -> Optional[datetime]:
    """Parse UTC datetime from L2 filename.

    Pattern: aod_vietnam_NC_H0{8,9}_{YYYYMMDD}_{HHMM}_L2ARP031_FLDK.*.tif
    """
    m = re.search(r'_(\d{8})_(\d{4})_L2ARP', fname)
    if not m:
        return None
    return datetime.strptime(m.group(1) + m.group(2), '%Y%m%d%H%M')


def _parse_l3_utc(fname: str) -> Optional[datetime]:
    """Parse UTC datetime from L3 filename.

    Pattern: aod_vietnam_H0{8,9}_{YYYYMMDD}_{HHMM}_1HARP031_FLDK.*.tif
    """
    m = re.search(r'_(\d{8})_(\d{4})_1HARP', fname)
    if not m:
        return None
    return datetime.strptime(m.group(1) + m.group(2), '%Y%m%d%H%M')


def _l2_files_in_window(slot_utc: datetime, window_min: int = 15) -> list[Path]:
    """Return all L2 TIF files whose UTC timestamp falls within ±window_min of slot_utc."""
    ym  = slot_utc.strftime('%Y%m')
    day = slot_utc.strftime('%d')
    # Glob across all hour subdirectories for the date
    pattern = str(HIMAWARI_L2_DIR / f'{ym}' / f'{day}' / '*' /
                  f'aod_vietnam_NC_H0?_{slot_utc.strftime("%Y%m%d")}_????_L2ARP031_FLDK.*.tif')
    candidates = glob.glob(pattern)
    delta = timedelta(minutes=window_min)
    result = []
    for p in candidates:
        t = _parse_l2_utc(Path(p).name)
        if t is not None and abs((t - slot_utc).total_seconds()) <= window_min * 60:
            result.append(Path(p))
    return sorted(result)


def _l3_files_in_window(slot_utc: datetime, window_min: int = 30) -> list[Path]:
    """Return L3 TIF files whose UTC timestamp falls within ±window_min of slot_utc."""
    ym  = slot_utc.strftime('%Y%m')
    day = slot_utc.strftime('%d')
    pattern = str(HIMAWARI_L3_DIR / f'{ym}' / f'{day}' /
                  f'aod_vietnam_H0?_{slot_utc.strftime("%Y%m%d")}_????_1HARP031_FLDK.*.tif')
    candidates = glob.glob(pattern)
    result = []
    for p in candidates:
        t = _parse_l3_utc(Path(p).name)
        if t is not None and abs((t - slot_utc).total_seconds()) <= window_min * 60:
            result.append(Path(p))
    return sorted(result)


# ── Band reading helpers ──────────────────────────────────────────────────────

def _read_band(src: rasterio.DatasetReader, band_idx: int) -> np.ndarray:
    """Read a rasterio band as float32, replacing fill sentinels with NaN."""
    arr = src.read(band_idx).astype(np.float32)
    for fv in _FILL_VALUES:
        arr[arr == fv] = np.nan
    return arr


# ── L2 reader ─────────────────────────────────────────────────────────────────

def read_l2_slot(
    slot_utc: datetime,
    window_min: int = 15,
    apply_vza_filter: bool = True,
) -> dict[str, np.ndarray] | None:
    """Read and QA-filter all L2 TIF files within the ±window_min slot.

    Step A1 filters applied:
        • AOT not NaN (implicit retrieval-valid check)
        • RF  ≥ HIMAWARI_RF_MIN  (fine-mode fraction; tightened in wet months)
        • |Uncertainty| ≤ HIMAWARI_UNC_MAX  (tightened in wet months)
        • SZA < HIMAWARI_SZA_MAX
        • VZA < HIMAWARI_VZA_MAX  (hard cut; VZA > HIMAWARI_VZA_SOFT flagged)

    During wet months (May–Sep) RF_MIN and UNC_MAX are replaced by the
    stricter HIMAWARI_WET_* constants to suppress cloud-edge contamination.

    Step A2: Multiple 10-min files within the slot are averaged.

    Returns a dict with 2-D arrays (NLAT × NLON) aligned to config grid:
        aot        – mean AOD 500 nm (NaN = no valid retrieval in slot)
        ae         – mean Ångström exponent
        uncertainty – mean signed uncertainty
        ssa        – mean SSA
        rf         – mean fine-mode fraction
        vza        – mean VZA (degrees)
        sza        – mean SZA (degrees)
        n_obs      – number of valid 10-min observations contributing to each cell
        vza_flag   – 1 where VZA > HIMAWARI_VZA_SOFT (lower confidence), else 0

    Returns None if no files are found for the slot.
    """
    files = _l2_files_in_window(slot_utc, window_min)
    if not files:
        return None

    rf_min, unc_max = _himawari_qa_thresholds(slot_utc.month)

    # Determine TIF subdomain position within the full config grid
    r0, c0, TH, TW = _get_tif_grid_offset(str(files[0]))
    tif_shape = (TH, TW)

    # Build VZA/SZA grids at TIF resolution (pixel centres in TIF subdomain)
    tif_lats = LATS[r0:r0 + TH]
    tif_lons = LONS[c0:c0 + TW]
    tif_lat_2d, tif_lon_2d = np.meshgrid(tif_lats, tif_lons, indexing='ij')
    vza_tif = compute_himawari_vza(tif_lat_2d, tif_lon_2d)

    # Accumulators at TIF resolution.  AE and SSA have their own counters because
    # NaN values for those bands occur independently of the AOT-valid mask; using
    # the AOT count would dilute their means toward zero.
    acc: dict[str, np.ndarray] = {
        k: np.zeros(tif_shape, dtype=np.float64)
        for k in ('aot', 'ae', 'unc', 'ssa', 'rf', 'vza', 'sza')
    }
    cnt    = np.zeros(tif_shape, dtype=np.int32)
    cnt_ae = np.zeros(tif_shape, dtype=np.int32)
    cnt_ss = np.zeros(tif_shape, dtype=np.int32)

    for fpath in files:
        fdt = _parse_l2_utc(fpath.name)
        with rasterio.open(str(fpath)) as src:
            aot = _read_band(src, 1)
            unc = _read_band(src, 2)
            ae  = _read_band(src, 3)
            ssa = _read_band(src, 5)
            rf  = _read_band(src, 6)

        # Step A1 QA filters on TIF-shaped arrays
        valid = ~np.isnan(aot)
        valid &= ~np.isnan(rf)  & (rf  >= rf_min)
        valid &= ~np.isnan(unc) & (np.abs(unc) <= unc_max)

        if apply_vza_filter:
            valid &= vza_tif < HIMAWARI_VZA_MAX

        if fdt is not None:
            sza_tif = compute_sza(tif_lat_2d, tif_lon_2d, fdt)
            valid &= sza_tif < HIMAWARI_SZA_MAX
        else:
            sza_tif = np.full(tif_shape, np.nan, dtype=np.float32)

        acc['aot'][valid] += aot[valid]
        acc['unc'][valid] += unc[valid]
        acc['rf'][valid]  += rf[valid]
        acc['vza'][valid] += vza_tif[valid]
        acc['sza'][valid] += sza_tif[valid]
        cnt[valid] += 1

        # AE/SSA tracked separately so a NaN AE on an otherwise-valid pixel
        # doesn't pull the mean toward zero.
        ae_ok = valid & ~np.isnan(ae)
        acc['ae'][ae_ok] += ae[ae_ok]
        cnt_ae[ae_ok]    += 1

        ssa_ok = valid & ~np.isnan(ssa)
        acc['ssa'][ssa_ok] += ssa[ssa_ok]
        cnt_ss[ssa_ok]     += 1

    has_tif = cnt > 0

    # Embed TIF-resolution results into the full (NLAT, NLON) config grid
    full_shape = (NLAT, NLON)
    result: dict[str, np.ndarray] = {}

    # Per-channel divisor: AE and SSA have their own counters (see accumulation)
    cnt_for = {'aot': cnt, 'unc': cnt, 'rf': cnt, 'vza': cnt, 'sza': cnt,
               'ae': cnt_ae, 'ssa': cnt_ss}
    key_map = {'aot': 'aot', 'ae': 'ae', 'unc': 'uncertainty',
               'ssa': 'ssa', 'rf': 'rf', 'vza': 'vza', 'sza': 'sza'}

    for k_acc, k_out in key_map.items():
        c    = cnt_for[k_acc]
        full = np.full(full_shape, np.nan, dtype=np.float32)
        tif_mean = np.where(c > 0,
                            acc[k_acc] / np.maximum(c, 1),
                            np.nan).astype(np.float32)
        full[r0:r0 + TH, c0:c0 + TW] = tif_mean
        result[k_out] = full

    cnt_full = np.zeros(full_shape, dtype=np.int16)
    cnt_full[r0:r0 + TH, c0:c0 + TW] = cnt.astype(np.int16)
    result['n_obs'] = cnt_full

    vza_flag_full = np.zeros(full_shape, dtype=np.int8)
    vza_tif_flag = ((vza_tif > HIMAWARI_VZA_SOFT) & has_tif).astype(np.int8)
    vza_flag_full[r0:r0 + TH, c0:c0 + TW] = vza_tif_flag
    result['vza_flag'] = vza_flag_full

    return result


# ── L3 reader ─────────────────────────────────────────────────────────────────

def read_l3_slot(slot_utc: datetime, window_min: int = 30) -> dict[str, np.ndarray] | None:
    """Read and QA-filter the nearest L3 hourly composite within ±window_min.

    Returns the same grid layout as read_l2_slot, but sourced from L3 AOT_Merged.
    Uses AOT_Merged (Band 2); falls back to AOT_L2_Mean (Band 10) if all-fill.

    Returns None if no files are found.
    """
    files = _l3_files_in_window(slot_utc, window_min)
    if not files:
        return None

    fpath = files[0]
    fdt   = _parse_l3_utc(fpath.name)

    _, unc_max = _himawari_qa_thresholds(slot_utc.month)

    r0, c0, TH, TW = _get_tif_grid_offset(str(fpath))
    tif_lats = LATS[r0:r0 + TH]
    tif_lons = LONS[c0:c0 + TW]
    tif_lat_2d, tif_lon_2d = np.meshgrid(tif_lats, tif_lons, indexing='ij')
    vza_tif = compute_himawari_vza(tif_lat_2d, tif_lon_2d)

    with rasterio.open(str(fpath)) as src:
        aot_merged = _read_band(src, 2)   # AOT_Merged
        unc_merged = _read_band(src, 4)   # AOT_Merged_uncertainty
        ae_merged  = _read_band(src, 6)   # AE_Merged
        aot_l2mean = _read_band(src, 10)  # AOT_L2_Mean (fallback)

    aot = np.where(~np.isnan(aot_merged), aot_merged, aot_l2mean)

    valid = ~np.isnan(aot)
    if not np.all(np.isnan(unc_merged)):
        valid &= ~np.isnan(unc_merged) & (np.abs(unc_merged) <= unc_max)
    valid &= vza_tif < HIMAWARI_VZA_MAX

    if fdt is not None:
        sza_tif = compute_sza(tif_lat_2d, tif_lon_2d, fdt)
        valid &= sza_tif < HIMAWARI_SZA_MAX
    else:
        sza_tif = np.full((TH, TW), np.nan, dtype=np.float32)

    full_shape = (NLAT, NLON)
    nan_full   = np.full(full_shape, np.nan, dtype=np.float32)

    def _embed(tif_arr):
        out = nan_full.copy()
        out[r0:r0 + TH, c0:c0 + TW] = np.where(valid, tif_arr, np.nan).astype(np.float32)
        return out

    n_obs_full    = np.zeros(full_shape, dtype=np.int16)
    n_obs_full[r0:r0 + TH, c0:c0 + TW] = valid.astype(np.int16)

    vza_flag_full = np.zeros(full_shape, dtype=np.int8)
    vza_flag_full[r0:r0 + TH, c0:c0 + TW] = ((vza_tif > HIMAWARI_VZA_SOFT) & valid).astype(np.int8)

    return {
        'aot':         _embed(aot),
        'ae':          _embed(ae_merged),
        'uncertainty': _embed(unc_merged),
        'ssa':         nan_full.copy(),
        'rf':          nan_full.copy(),
        'vza':         _embed(vza_tif),
        'sza':         _embed(sza_tif),
        'n_obs':       n_obs_full,
        'vza_flag':    vza_flag_full,
    }


