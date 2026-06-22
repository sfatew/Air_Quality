"""Stage A configuration (v3.4.0): domain, grid, paths, soft-calibration & TC constants.

v3.4.0 architectural pivot — AERONET is reserved for held-out validation; both
calibration anchors are AERONET-independent:

  • Bias correction (§7.4.1): linear `MERRA2 = α · sat + β` per
    (sensor, region, season) stratum, anchored against spatially-complete
    MERRA-2 (Ding et al. 2025 form).  Persisted to `soft_calibration.json`.

  • Fusion weights (§7.4.2, §7.5): triple-collocation σ² per stratum
    (Stoffelen 1998; McColl 2014), floored at the Sayer/Levy EE envelope.
    Persisted to `tc_error_variance.json`.

The v3.3 structures dissolved here: per-AERONET-site CDF, LEO–Himawari offset
map, post_correction_rmse.json, MODIS_SOUTH_WEIGHT_FACTOR, HIMAWARI_WET_WEIGHT_FACTOR.
"""

from datetime import date
from pathlib import Path
import numpy as np

# ── Spatial domain ────────────────────────────────────────────────────────────
# Matched to ERA5 bounding box [N=23.5, W=102.0, S=8.0, E=110.0]
# and to the native Himawari L2/L3 GeoTIFF extent.
LAT_MIN, LAT_MAX = 8.0, 23.5     # degrees N
LON_MIN, LON_MAX = 102.0, 110.0  # degrees E
GRID_RES = 0.05                   # degrees (≈ 5.5 km)

# Grid coordinate arrays: cell centres, north→south, west→east
LATS = np.arange(LAT_MAX - GRID_RES / 2, LAT_MIN, -GRID_RES)
LONS = np.arange(LON_MIN + GRID_RES / 2, LON_MAX, GRID_RES)
NLAT = len(LATS)   # 310
NLON = len(LONS)   # 160

# ── Region latitude boundaries (degrees N) ───────────────────────────────────
# North  : lat ≥ NORTH_CENTRAL_LAT
# Central: CENTRAL_SOUTH_LAT ≤ lat < NORTH_CENTRAL_LAT
# South  : lat < CENTRAL_SOUTH_LAT
NORTH_CENTRAL_LAT = 16.0
CENTRAL_SOUTH_LAT = 11.5

# Cosine-taper half-width (degrees) around each region boundary at the
# soft-cal apply step. (α, β) are blended in calibration space across
# [boundary − half_width, boundary + half_width] to remove the step
# discontinuity that hard region masks otherwise stamp into the merged AOD.
SOFTCAL_BLEND_HALF_WIDTH = 0.5

REGIONS = ('north', 'central', 'south')
SEASONS = ('dry', 'wet')

# ── Season definitions ────────────────────────────────────────────────────────
DRY_MONTHS = frozenset({10, 11, 12, 1, 2, 3, 4})   # Oct–Apr
WET_MONTHS = frozenset({5, 6, 7, 8, 9})             # May–Sep

# ── Study-period splits (§8.0) ────────────────────────────────────────────────
# All calibration (soft cal + TC σ²) consumes only the training window.
# The held-out window is never touched until §8 validation.
TRAIN_START = date(2022, 9, 1)
TRAIN_END   = date(2024, 12, 31)
TEST_START  = date(2025, 1, 1)
TEST_END    = date(2026, 4, 30)

# ── AERONET sites (held-out validation only; §7.0, §8.1.1) ────────────────────
AERONET_SITES = {
    'NGHIA_DO': {'lat': 21.048, 'lon': 105.800, 'region': 'north'},
    'Bac_Lieu': {'lat': 9.280,  'lon': 105.730, 'region': 'south'},
}

# ── Soft calibration (§7.4.1) ─────────────────────────────────────────────────
# Linear fit `MERRA2 = α · sat + β` per (sensor, region, season).
# CV-time guard rail: out-of-range α or |β| → route stratum to 'none',
# with a NONE_PENALTY_FACTOR × σ² penalty at fusion (weight drops by factor²).
SOFT_CAL_ALPHA_MIN  = 0.5
SOFT_CAL_ALPHA_MAX  = 2.0
SOFT_CAL_BETA_ABSMAX = 0.2
SOFT_CAL_CV_FOLDS   = 5

# Minimum collocated (sat, MERRA-2) pairs to even attempt a fit per stratum.
# Below this we skip the regression and route to 'none'.
SOFT_CAL_MIN_PAIRS  = 100

# §8.1.2 gate A — minimum CV RMSE drop for the soft cal to be worth applying.
# If `rmse_before − rmse_after_cv < this`, the correction is statistical noise
# and the stratum is re-routed to 'none'.  Gate B (held-out inflation) lives
# in validate_bias_correction.ipynb and never feeds back into the JSON.
GATE_A_RMSE_DROP_MIN = 0.02

# ── AERONET-anchored fallback for 'none'-routed strata (§7.4.1.1) ────────────
# When the MERRA-2 anchor routes a (sensor, region, season) stratum to 'none'
# (guard-rail or gate A failure), we attempt a documented one-shot fallback:
# fit `AERONET = α · sat + β` on AERONET pairs from the full training window
# [TRAIN_START, TRAIN_END], gated on k-fold CV (mirrors the MERRA-2 anchor's
# CV gate).  The §8 hold-out (Jan 2025 – Apr 2026) remains untouched.
#
# This is the only place AERONET enters training; it only kicks in when the
# MERRA-2 anchor fails. Central-region strata have no AERONET station and so
# can never use this fallback — they stay 'none'.
#
# Why k-fold CV and not a temporal sub-window: a diagnostic on Himawari
# north|dry showed the temporal split was rejecting fits that random-fold
# validates by wide margins (Δ drop ≈ +0.16/0.19), because partial-dry-season
# holdouts collapse to a single year's haze events and over-penalise normal
# inter-annual variability.  The §8 held-out window is the true temporal-
# generalisation test; the fallback gate's job is fit quality.
AERONET_FALLBACK_MIN_PAIRS = 40    # need ≥ CV_FOLDS × 2 + headroom

# Widened α/β bounds for the AERONET fallback only.  Tropical monsoon strata
# can exhibit scale biases (α ≈ 0.3–0.4 wet-season south, where satellites
# over-retrieve by 2.5–5×) that the tight MERRA-2-anchor bounds reject as
# out-of-range — but AERONET ground truth says those corrections are real and
# transfer on CV.  The wider bounds are compensated by a stricter RMSE-drop
# requirement below: gain a larger correction window in exchange for proving
# more improvement.
AERONET_FALLBACK_ALPHA_MIN   = 0.3
AERONET_FALLBACK_ALPHA_MAX   = 3.0
AERONET_FALLBACK_BETA_ABSMAX = 0.4

# Stricter CV RMSE-drop gate for the AERONET fallback (vs 0.02 on the MERRA-2
# anchor).  Couples with the widened α/β bounds above: a wider parameter box
# without this tighter drop requirement would let in spurious fits driven by
# a handful of high-AOD events.
AERONET_FALLBACK_RMSE_DROP_MIN = 0.05

# Apply-time output clipping (mirrors AERONET observed range; protects against
# extrapolation when α·sat+β projects below 0 or far above the observed cap).
SOFT_CAL_OUTPUT_MIN = 0.0
SOFT_CAL_OUTPUT_MAX = 5.0

# Penalty applied to σ²_TC when a stratum's correction routes to 'none':
#   σ²_used = σ²_TC × NONE_PENALTY_FACTOR     (fusion weight drops ~4× at 2.0)
NONE_PENALTY_FACTOR = 2.0

# ── Triple-collocation σ² floor (§7.4.2) ──────────────────────────────────────
# Sayer/Levy EE envelope: σ²_floor = (EE_OFFSET + slope × AOD_ref)²
# Used as the gate-fail fallback in run_collocate.tc_variance, and as the
# missing-stratum fallback in fusion._sigma2_grid.  NOT applied as a hard
# clamp on TC values that passed the gate (we trust the data once it passes
# TC_MIN_TRIPLETS + TC_MIN_COLLOCATIONS).
SENSOR_RMSE_EE_OFFSET = 0.05
SENSOR_RMSE_EE_SLOPE  = 0.15   # flat default; per-sensor overrides below
SENSOR_RMSE_EE_REFAOD = 0.3

# Per-sensor EE slope overrides — sourced from the same peer-reviewed envelopes
# as BOX_STD_SLOPE below, extended with the merged 'himawari' production key
# (consumed by fusion) and MERRA-2.  Missing sensor → SENSOR_RMSE_EE_SLOPE.
#   • modis_maiac : 0.10  Lyapustin 2018; Falah 2021
#   • viirs_*     : 0.20  Sayer 2019; Hsu 2019
#   • himawari_*  : 0.20  Zhang 2019 (note: ~55% within EE, optimistic)
#   • merra2      : 0.17  Randles 2017 — reanalysis RMSE ≈ 0.10–0.12 vs AERONET,
#                         interpreted as (0.05 + 0.17·0.3) at AOD_ref=0.3.
SENSOR_EE_SLOPE = {
    'himawari_l2':  0.20,
    'himawari_l3':  0.20,
    'himawari':     0.20,
    'viirs_snpp':   0.20,
    'viirs_noaa20': 0.20,
    'modis_maiac':  0.10,
    'merra2':       0.17,
}

# Minimum triplets-and-collocations per (sensor, stratum) before a σ² estimate
# is trusted; below either gate the stratum inherits the EE-envelope floor.
#   • TC_MIN_TRIPLETS     — distinct 3-sensor combinations the sensor participates
#     in within the stratum.  Bounded above by C(N_sensors-1, 2); with 6 members
#     that's 10, so values above ~6 are effectively unreachable.
#   • TC_MIN_COLLOCATIONS — total per-cell-per-slot samples summed across those
#     triplets.  Ensures each triplet's σ² estimate rests on a real sample size.
TC_MIN_TRIPLETS     = 3
TC_MIN_COLLOCATIONS = 500

# ── Sensor codes (Stage A output `dominant_sensor`) ──────────────────────────
# 1 = Himawari (merged L2/L3), 3 = MAIAC, 4 = SNPP, 5 = NOAA-20
SENSOR_CODES = {
    'himawari':     1,
    'modis_maiac':  3,
    'viirs_snpp':   4,
    'viirs_noaa20': 5,
}

# ── Confidence flag codes (§7.5 Stage A output) ───────────────────────────────
# 0=no data, 1=Himawari only, 2=LEO only, 3=Himawari+LEO, 4=multi-LEO+Himawari
CONFIDENCE_FLAG = {
    'no_data':            0,
    'himawari_only':      1,
    'leo_only':           2,
    'himawari_plus_leo':  3,
    'multi_leo_himawari': 4,
}

# ── Physics correction (Step A3) ─────────────────────────────────────────────
GAMMA    = 0.6     # hygroscopic growth exponent (Kotchenruther & Hobbs 1998)
PBLH_MIN = 50.0    # m — minimum PBLH to prevent near-zero division

# ── QA filter thresholds ──────────────────────────────────────────────────────
# Himawari L2 / L3 — strict JAXA bit-mask in himawari.py (bit 10 subsumes SZA/VZA).
HIMAWARI_RF_MIN  = 0.5    # fine-mode fraction minimum (Band 6, L2 only)
HIMAWARI_UNC_MAX = 0.5    # absolute retrieval uncertainty maximum
HIMAWARI_AOT_MIN = 0.0    # strict-zero gate: pixels with aot ≤ this are dropped

# VIIRS Deep Blue L2 — uniform threshold, no land/ocean asymmetry.
VIIRS_QA_MIN = 2   # 0=no retrieval, 1=poor, 2=moderate, 3=good

# MODIS MAIAC (MCD19A2) — AOD_QA bits 8–11 ≤ this.
# 0=Best, 4=Marginal, >4=Poor & rejected.
MODIS_QA_BITS_MAX = 4

# ── Temporal parameters ───────────────────────────────────────────────────────
TZ_OFFSET_HOURS = 7    # Vietnam local time = UTC + 7
SLOT_MINUTES    = 30   # 30-min merged product cadence
LEO_WINDOW_MIN   = 30   # ±minutes window for Himawari L2 / VIIRS–AERONET co-location
MODIS_WINDOW_MIN = 30   # ±minutes window for per-orbit MODIS–AERONET co-location

# ── Collocation spatial sampling (Ichoku et al. 2002; Levy et al. 2010) ───────
# Flag values written to the spatial_flag column of collocated CSVs
COLLOCATE_SPATIAL_FLAG_EXACT = 0   # station falls in the exact pixel/grid cell
COLLOCATE_SPATIAL_FLAG_3X3   = 1   # 3×3 native-cell neighbourhood mean (fallback)
COLLOCATE_SPATIAL_FLAG_5X5   = 2   # 5×5 native-cell neighbourhood (low-N stratum fallback)

# Reject matchup if within-neighbourhood AOD std exceeds the retrieval's own
# expected-error (EE) envelope at the observed AOD level.
# threshold = max(BOX_STD_ABS_FLOOR, BOX_STD_SLOPE[sensor] × mean_box_aod)
BOX_STD_ABS_FLOOR = 0.05   # absolute floor shared across sensors

# One-sigma EE slopes per product (envelope: ±(0.05 + slope·AOD))
# Reference DT envelope: Levy et al. 2013 — slope 0.15 (NOT Sayer)
# MODIS MAIAC:       Lyapustin 2018; Falah 2021
# VIIRS Deep Blue:   Sayer 2019; Hsu 2019
# Himawari AHI:      Zhang 2019  (note: ~55% within EE, so 0.20 is optimistic)
BOX_STD_SLOPE = {
    'himawari_l2':  0.20,
    'himawari_l3':  0.20,
    'viirs_snpp':   0.20,
    'viirs_noaa20': 0.20,
    'modis_maiac':  0.10,
}

# ── Data paths ────────────────────────────────────────────────────────────────
DATA_ROOT        = Path('/home/slow_data/Air_Quality')
HIMAWARI_L2_DIR  = DATA_ROOT / 'Himawari' / 'L2_AOD'
HIMAWARI_L3_DIR  = DATA_ROOT / 'Himawari' / 'L3_AOD'
VIIRS_SNPP_DIR   = DATA_ROOT / 'VIIRS' / 'L2' / 'AERDB_L2_VIIRS_SNPP'
VIIRS_N20_DIR    = DATA_ROOT / 'VIIRS' / 'L2' / 'AERDB_L2_VIIRS_NOAA20'
MODIS_DIR        = DATA_ROOT / 'MODIS_MCD19A2' / 'raw'
AERONET_DIR      = DATA_ROOT / 'AERONET' / 'raw' / '10'
ERA5_MONTHLY_DIR = DATA_ROOT / 'ERA5' / '_monthly_raw'

# MERRA-2 M2T1NXAER (hourly, 0.5° × 0.625°) — Vietnam-bbox subset files only,
# named `MERRA2_400.tavg1_2d_aer_Nx.YYYYMMDD_vnm.nc4` per MERRA2/download_m2t1nxaer.py.
MERRA2_DIR       = DATA_ROOT / 'MERRA2' / 'M2T1NXAER'
MERRA2_AOD_VAR   = 'TOTEXTTAU'   # total aerosol extinction AOD at 550 nm

OUTPUT_DIR       = DATA_ROOT / 'Stage_A'
EXTRACT_DIR      = OUTPUT_DIR / 'extracted'     # satellite AOD time series at stations (validation)
COLLOCATE_DIR    = OUTPUT_DIR / 'collocated'    # satellite–AERONET matched pairs (validation)
GRIDDED_DIR      = OUTPUT_DIR / 'gridded'       # per-sensor 30-min A2 NetCDFs
MERGED_DIR       = OUTPUT_DIR / 'merged'        # final TC-merged 30-min NetCDFs
BIASC_DIR        = OUTPUT_DIR / 'bias_corr'     # soft_calibration.json, tc_error_variance.json

SOFT_CAL_FILE    = BIASC_DIR / 'soft_calibration.json'
TC_VARIANCE_FILE = BIASC_DIR / 'tc_error_variance.json'

# ── VIIRS overpass grouping ───────────────────────────────────────────────────
# Consecutive granules whose UTC timestamps differ by ≤ this threshold are
# considered the same overpass and pooled before spatial extraction.
VIIRS_OVERPASS_GAP_MIN = 5
