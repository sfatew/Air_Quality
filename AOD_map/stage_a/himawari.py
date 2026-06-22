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
    HIMAWARI_RF_MIN, HIMAWARI_UNC_MAX, HIMAWARI_AOT_MIN,
    NLAT, NLON, GRID_RES, LAT_MAX, LON_MIN,
)

# Ångström interpolation 500 nm (JAXA AHI native) → 550 nm (MODIS/VIIRS/AERONET).
# τ_550 = τ_500 × (550 / 500)^(−AE).  Pixels with AE unavailable are dropped:
# silently passing 500 nm AOT downstream would mix wavelengths in fusion.
_HIMAWARI_WL_RATIO = 550.0 / 500.0


def _wavelength_correct(aot_500: np.ndarray, ae: np.ndarray) -> np.ndarray:
    """Return AOT at 550 nm, NaN where AE is missing."""
    out = np.full_like(aot_500, np.nan, dtype=np.float32)
    ok  = np.isfinite(aot_500) & np.isfinite(ae)
    out[ok] = aot_500[ok] * np.power(_HIMAWARI_WL_RATIO, -ae[ok], dtype=np.float32)
    return out

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

# ── JAXA strict QA bit-mask (Band 4 L2 / Band 8 L3 QA_flag_Merged) ───────────

def _qa_passes(qa: np.ndarray) -> np.ndarray:
    """Vectorised strict JAXA quality screening (returns 2-D bool array).

    Pixel passes iff ALL of:
        bit 0   data_avail          = 0
        bit 2   cloud               = 0
        bit 3   retrieval ok        = 0
        bit 4-5 AOT confidence      = 00  (very good)
        bit 8   additional cloud    = 0
        bit 10  Solz/Satz > 70°     = 0   (replaces the prior SZA+VZA filters)
        bit 11  surface refl bad    = 0
        bit 12  snow/ice            = 0   (L2-only; 0 on L3 by design)
        bit 13  turbid water        = 0   (L2-only; 0 on L3 by design)

    Source: Himawari/extract_aod/extract_stations_aod.py::qa_passes_array.
    """
    qa_i = qa.astype(np.int32)
    return (
        (((qa_i >> 0)  & 1)    == 0)
        & (((qa_i >> 2)  & 1)    == 0)
        & (((qa_i >> 3)  & 1)    == 0)
        & (((qa_i >> 4)  & 0b11) == 0)
        & (((qa_i >> 8)  & 1)    == 0)
        & (((qa_i >> 10) & 1)    == 0)
        & (((qa_i >> 11) & 1)    == 0)
        & (((qa_i >> 12) & 1)    == 0)
        & (((qa_i >> 13) & 1)    == 0)
    )


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
) -> dict[str, np.ndarray] | None:
    """Read and QA-filter all L2 TIF files within the ±window_min slot.

    Step A1 filters applied:
        • JAXA strict QA bit-mask on Band 4 (QA_flag) — see _qa_passes.
          Bit 10 already gates Solz/Satz > 70°, so VZA and SZA are no
          longer computed per pixel.
        • AOT > HIMAWARI_AOT_MIN  (strict-zero gate)
        • RF  ≥ HIMAWARI_RF_MIN  (fine-mode fraction, Band 6)
        • |Uncertainty| ≤ HIMAWARI_UNC_MAX  (Band 2)

    Step A2: Multiple 10-min files within the slot are averaged.

    Returns a dict with 2-D arrays (NLAT × NLON) aligned to config grid:
        aot         – mean AOD 550 nm (NaN = no valid retrieval in slot)
        ae          – mean Ångström exponent
        uncertainty – mean signed uncertainty
        ssa         – mean SSA
        rf          – mean fine-mode fraction
        n_obs       – number of valid 10-min observations per cell

    Returns None if no files are found for the slot.
    """
    files = _l2_files_in_window(slot_utc, window_min)
    if not files:
        return None

    # Determine TIF subdomain position within the full config grid
    r0, c0, TH, TW = _get_tif_grid_offset(str(files[0]))
    tif_shape = (TH, TW)

    # Accumulators at TIF resolution.  AE and SSA have their own counters because
    # NaN values for those bands occur independently of the AOT-valid mask; using
    # the AOT count would dilute their means toward zero.
    acc: dict[str, np.ndarray] = {
        k: np.zeros(tif_shape, dtype=np.float64)
        for k in ('aot', 'ae', 'unc', 'ssa', 'rf')
    }
    cnt    = np.zeros(tif_shape, dtype=np.int32)
    cnt_ae = np.zeros(tif_shape, dtype=np.int32)
    cnt_ss = np.zeros(tif_shape, dtype=np.int32)

    for fpath in files:
        with rasterio.open(str(fpath)) as src:
            aot = _read_band(src, 1)
            unc = _read_band(src, 2)
            ae  = _read_band(src, 3)
            qa  = src.read(4).astype(np.int32)  # raw bitmask — no fill substitution
            ssa = _read_band(src, 5)
            rf  = _read_band(src, 6)

        # Step A1 QA filters on TIF-shaped arrays
        valid = ~np.isnan(aot) & (aot > HIMAWARI_AOT_MIN)
        valid &= _qa_passes(qa)
        valid &= ~np.isnan(rf)  & (rf  >= HIMAWARI_RF_MIN)
        valid &= ~np.isnan(unc) & (np.abs(unc) <= HIMAWARI_UNC_MAX)

        acc['aot'][valid] += aot[valid]
        acc['unc'][valid] += unc[valid]
        acc['rf'][valid]  += rf[valid]
        cnt[valid] += 1

        # AE/SSA tracked separately so a NaN AE on an otherwise-valid pixel
        # doesn't pull the mean toward zero.
        ae_ok = valid & ~np.isnan(ae)
        acc['ae'][ae_ok] += ae[ae_ok]
        cnt_ae[ae_ok]    += 1

        ssa_ok = valid & ~np.isnan(ssa)
        acc['ssa'][ssa_ok] += ssa[ssa_ok]
        cnt_ss[ssa_ok]     += 1

    # Embed TIF-resolution results into the full (NLAT, NLON) config grid
    full_shape = (NLAT, NLON)
    result: dict[str, np.ndarray] = {}

    cnt_for = {'aot': cnt, 'unc': cnt, 'rf': cnt, 'ae': cnt_ae, 'ssa': cnt_ss}
    key_map = {'aot': 'aot', 'ae': 'ae', 'unc': 'uncertainty',
               'ssa': 'ssa', 'rf': 'rf'}

    for k_acc, k_out in key_map.items():
        c    = cnt_for[k_acc]
        full = np.full(full_shape, np.nan, dtype=np.float32)
        tif_mean = np.where(c > 0,
                            acc[k_acc] / np.maximum(c, 1),
                            np.nan).astype(np.float32)
        full[r0:r0 + TH, c0:c0 + TW] = tif_mean
        result[k_out] = full

    # Ångström 500→550 nm.  AHI AOT is at 500 nm; everything downstream
    # (MAIAC, VIIRS, AERONET aod_550) is at 550 nm.
    result['aot'] = _wavelength_correct(result['aot'], result['ae'])

    cnt_full = np.zeros(full_shape, dtype=np.int16)
    cnt_full[r0:r0 + TH, c0:c0 + TW] = cnt.astype(np.int16)
    result['n_obs'] = cnt_full

    return result


# ── L3 reader ─────────────────────────────────────────────────────────────────

def read_l3_slot(slot_utc: datetime, window_min: int = 30) -> dict[str, np.ndarray] | None:
    """Read and QA-filter the nearest L3 hourly composite within ±window_min.

    Returns the same grid layout as read_l2_slot, sourced from L3 AOT_Merged
    (Band 2) and screened with QA_flag_Merged (Band 8) using the JAXA strict
    bit-mask.  No fallback to AOT_L2_Mean: mixing the two would mean blending
    JAXA's merged retrieval with a raw 10-min average, which are different
    products with different quality characteristics.

    Step A1 filters:
        • JAXA strict QA bit-mask on QA_flag_Merged — see _qa_passes
          (subsumes the prior SZA/VZA gates via bit 10)
        • AOT > HIMAWARI_AOT_MIN  (strict-zero gate)
        • |AOT_Merged_uncertainty| ≤ HIMAWARI_UNC_MAX

    Returns None if no files are found.
    """
    files = _l3_files_in_window(slot_utc, window_min)
    if not files:
        return None

    fpath = files[0]
    r0, c0, TH, TW = _get_tif_grid_offset(str(fpath))

    with rasterio.open(str(fpath)) as src:
        aot        = _read_band(src, 2)              # AOT_Merged
        unc_merged = _read_band(src, 4)              # AOT_Merged_uncertainty
        ae         = _read_band(src, 6)              # AE_Merged
        qa         = src.read(8).astype(np.int32)    # QA_flag_Merged (raw bitmask)

    valid = ~np.isnan(aot) & (aot > HIMAWARI_AOT_MIN)
    valid &= _qa_passes(qa)
    if not np.all(np.isnan(unc_merged)):
        valid &= ~np.isnan(unc_merged) & (np.abs(unc_merged) <= HIMAWARI_UNC_MAX)

    full_shape = (NLAT, NLON)
    nan_full   = np.full(full_shape, np.nan, dtype=np.float32)

    def _embed(tif_arr):
        out = nan_full.copy()
        out[r0:r0 + TH, c0:c0 + TW] = np.where(valid, tif_arr, np.nan).astype(np.float32)
        return out

    n_obs_full = np.zeros(full_shape, dtype=np.int16)
    n_obs_full[r0:r0 + TH, c0:c0 + TW] = valid.astype(np.int16)

    aot_full = _embed(aot)
    ae_full  = _embed(ae)
    # Ångström 500→550 nm.  AHI L3 AOT_Merged is at 500 nm.
    aot_full = _wavelength_correct(aot_full, ae_full)

    return {
        'aot':         aot_full,
        'ae':          ae_full,
        'uncertainty': _embed(unc_merged),
        'ssa':         nan_full.copy(),
        'rf':          nan_full.copy(),
        'n_obs':       n_obs_full,
    }


