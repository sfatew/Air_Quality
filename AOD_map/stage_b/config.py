"""Stage B configuration (v3.4.0+stage_b_fixes rev 2) — paths, hyperparameters, constants.

Stage B runs at **30-min slot cadence** end-to-end. The daytime window per
UTC day is **data-driven from Stage A filenames** (§0 of stage_b_fixes):

  1. List `MERGED_DIR/YYYY/MM/DD/merged_YYYYMMDD_HHMM.nc`
  2. Parse the HHMM → observed-slot list
  3. If ≥10 observed slots: window = [min(HHMM), max(HHMM)]
  4. If <10 observed slots: 7-day median fallback (see slots.py)

Outputs go to **two parallel product trees**, one per method:
  output/st_kriging/YYYY/MM/DD/aod_YYYYMMDD_HHMM.nc
  output/rf/YYYY/MM/DD/aod_YYYYMMDD_HHMM.nc
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

# ── Reuse Stage A configuration ──────────────────────────────────────────────
_STAGE_A_DIR = Path(__file__).resolve().parent.parent / 'stage_a'
_STAGE_A_CFG_PATH = _STAGE_A_DIR / 'config.py'
_spec = importlib.util.spec_from_file_location('stage_a_config', _STAGE_A_CFG_PATH)
_stage_a_cfg = importlib.util.module_from_spec(_spec)
sys.modules['stage_a_config'] = _stage_a_cfg
_spec.loader.exec_module(_stage_a_cfg)

if str(_STAGE_A_DIR) not in sys.path:
    sys.path.append(str(_STAGE_A_DIR))

LAT_MIN = _stage_a_cfg.LAT_MIN
LAT_MAX = _stage_a_cfg.LAT_MAX
LON_MIN = _stage_a_cfg.LON_MIN
LON_MAX = _stage_a_cfg.LON_MAX
GRID_RES = _stage_a_cfg.GRID_RES
LATS = _stage_a_cfg.LATS
LONS = _stage_a_cfg.LONS
NLAT = _stage_a_cfg.NLAT
NLON = _stage_a_cfg.NLON
NORTH_CENTRAL_LAT = _stage_a_cfg.NORTH_CENTRAL_LAT
CENTRAL_SOUTH_LAT = _stage_a_cfg.CENTRAL_SOUTH_LAT
DRY_MONTHS = _stage_a_cfg.DRY_MONTHS
WET_MONTHS = _stage_a_cfg.WET_MONTHS
TRAIN_START = _stage_a_cfg.TRAIN_START
TRAIN_END = _stage_a_cfg.TRAIN_END
TEST_START = _stage_a_cfg.TEST_START
TEST_END = _stage_a_cfg.TEST_END
AERONET_SITES = _stage_a_cfg.AERONET_SITES
TZ_OFFSET_HOURS = _stage_a_cfg.TZ_OFFSET_HOURS
SLOT_MINUTES = _stage_a_cfg.SLOT_MINUTES
DATA_ROOT = _stage_a_cfg.DATA_ROOT
MERGED_DIR = _stage_a_cfg.MERGED_DIR
ERA5_MONTHLY_DIR = _stage_a_cfg.ERA5_MONTHLY_DIR

# ── Slot geometry (data-driven; only the slot grid itself is fixed) ──────────
# Universal slot index: hhmm → idx in 0..47 (every possible 30-min slot in a day).
SLOTS_PER_DAY      = 48
WINDOW_MEDIAN_HALF_DAYS = 3   # 7-day window for the median fallback (±3)
WINDOW_MIN_SLOTS_PRESENT = 10  # < this triggers the 7-day median fallback

# ── Stage B paths ─────────────────────────────────────────────────────────────
STAGE_B_DIR     = DATA_ROOT / 'Stage_B'
ST_KRIGING_DIR  = STAGE_B_DIR / 'output' / 'st_kriging'   # parallel product tree
RF_OUTPUT_DIR   = STAGE_B_DIR / 'output' / 'rf'           # parallel product tree
MODELS_DIR      = STAGE_B_DIR / 'models'
VALIDATION_DIR  = STAGE_B_DIR / 'validation'

# Daily aggregations for the §8 RQ4 comparison (post-processing — not a Stage B step).
AGGREGATED_DIR = STAGE_B_DIR / 'output_aggregated'

# ── Ancillary data ───────────────────────────────────────────────────────────
CAMS_MONTHLY_DIR  = DATA_ROOT / 'CAM' / '_monthly_raw'
NDVI_DIR          = DATA_ROOT / 'MODIS_MOD13Q1' / 'raw'
LANDCOVER_DIR     = DATA_ROOT / 'MODIS_MCD12Q1'
LANDSCAN_DIR      = DATA_ROOT / 'LandScan'
DEM_DIR           = DATA_ROOT / 'DEM'
IMERG_DIR         = DATA_ROOT / 'GIS'

# ── B1 ST kriging parameters (§7.7 / Yang & Hu 2018) ─────────────────────────
B1_W_SPACE_KM  = 100.0
B1_W_TIME_H    = 12.0
B1_MAX_NEIGHBOURS = 40
B1_MIN_NEIGHBOURS = 8
B1_VARIOGRAM_SUBSAMPLE = 100_000

B1_VARIOGRAM_SPEC = {
    'spatial':  'Exponential',
    'temporal': 'Gaussian',
    'joint':    'Gaussian',
}

B1_VARIOGRAM_INIT = {
    'spatial':  {'var': 0.05, 'len_scale_km':       60.0,  'nugget': 0.0},
    'temporal': {'var': 0.05, 'len_scale_hours':     6.0,  'nugget': 0.0},
    'joint':    {'var': 0.10, 'len_scale_km':     1_000.0, 'nugget': 0.0},
    'k_km_per_hour': 0.6,
}

EARTH_RADIUS_KM = 6371.0

# ── B2 RF hyperparameters ────────────────────────────────────────────────────
RF_N_ESTIMATORS     = 100
RF_MAX_DEPTH        = 20
RF_MIN_SAMPLES_LEAF = 5
RF_N_JOBS           = -1
RF_RANDOM_STATE     = 42

# stage_b_fixes §0.5: explicitly DO NOT use OOB-R² — AOD temporal autocorrelation
# makes the IID-bootstrap OOB estimate optimistic.  Always set oob_score=False.
RF_OOB_SCORE = False

RF_GRID = {
    'n_estimators':     [100, 200],
    'max_depth':        [15, 20],
    'min_samples_leaf': [5, 10],
    'max_features':     ['sqrt', 0.5],
}

# Stratified random subsample of ~2×10⁶ rows over (month, slot_idx).
RF_TRAIN_TARGET_ROWS    = 2_000_000
RF_STRATIFY_BINS        = ('month', 'slot_idx')

# §0.5: 5-fold contiguous temporal CV (fold assignment by UTC date).
RF_CV_FOLDS               = 5
RF_INTERNAL_TEST_FRACTION = 0.20

RMSE_CONSISTENCY_TOLERANCE = 0.15

# ── B2 RF predictor configuration (19 predictors) ───────────────────────────
RF_FEATURES_DYNAMIC = [
    'cams_aod',
    't2m', 'dpt', 'rh', 'sp', 'u10', 'v10',
    'blh', 'tcc', 'tcwv', 'ssrd', 'fal',
    'tp',
]
RF_FEATURES_STATIC = [
    'elevation', 'land_cover', 'population', 'lat_rad', 'lon_rad',
]
RF_FEATURES_QUASISTATIC = ['ndvi']
RF_FEATURES = RF_FEATURES_DYNAMIC + RF_FEATURES_STATIC + RF_FEATURES_QUASISTATIC

# ── §8 — slots_since_last_observed audit bins (validation-side only) ─────────
# Counted in 30-min steps; nighttime gap ≈ 26-30 steps (13-15 h) depending on day.
SSO_BINS   = [-1, 0, 1, 4, 19, 49, 1_000_000]
SSO_LABELS = ['observed', '1 slot', '≤2 h', '≤10 h', '≤25 h (one night)', '>25 h']
