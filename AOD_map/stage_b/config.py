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
# Regression-kriging (B3): RF drift + kriged RF-residual.  RF_PRED_DIR holds the
# full-grid RF prediction ŷ_rf per slot (drift term, needed at observed cells to
# form residuals — the `rf` product only keeps observations there).  RF_RK_DIR is
# the final RK product tree.
RF_PRED_DIR     = STAGE_B_DIR / 'output' / 'rf_pred'      # full-grid ŷ_rf per slot
RF_RK_DIR       = STAGE_B_DIR / 'output' / 'rf_rk'        # RK product tree
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
FIRMS_ZIP         = DATA_ROOT / 'DL_FIRE_M-C61_759406.zip'

# FIRMS aggregation: per cell, log(1 + sum of FRP within FIRMS_RADIUS_KM over
# the past FIRMS_LOOKBACK_H ending at slot_utc).  25 km matches typical
# fresh-smoke plume scale; 24 h matches smoke residence time in BLH.
FIRMS_RADIUS_KM   = 25.0
FIRMS_LOOKBACK_H  = 24.0

# ── B1 ST kriging parameters (§7.7 / Yang & Hu 2018) ─────────────────────────
B1_W_SPACE_KM  = 100.0
B1_W_TIME_H    = 12.0
B1_MAX_NEIGHBOURS = 40
B1_MIN_NEIGHBOURS = 8
B1_VARIOGRAM_SUBSAMPLE = 100_000

# ── Variogram model: single-component **metric** (Yang & Hu 2018, §3.2) ──────
# γ(h_s, h_t) = γ_metric( √(h_s² + (k · h_t)²) )
# Sum-metric was dropped after the §4 diagnostic showed the empirical surface
# couldn't constrain three components (most bins <30 pairs; temporal axis has
# a structural day-night hole). One CovModel + anisotropy k → 4 fit params.
B1_VARIOGRAM_SPEC = {'metric': 'Exponential'}

B1_VARIOGRAM_INIT = {
    'metric': {'var': 0.05, 'len_scale_km': 80.0, 'nugget': 0.005},
    'k_km_per_hour': 0.6,
}

# Target the kriger predicts. CAMS already absorbs the large-scale spatial
# trend, so kriging the residual (AOD − CAMS) gives a more stationary, smaller-
# range field with a sill that actually matches its variance.
B1_VARIOGRAM_TARGET = 'aod_minus_cams'   # {'aod', 'aod_minus_cams'}

# Empirical-variogram sampling/binning controls.
#   PAIRS_PER_CELL: cap on same-cell pairs per (lat, lon) — populates h_s≈0.
#   PAIRS_PER_SLOT: cap on same-slot pairs per timestamp — populates h_t≈0.
#   MIN_BIN_PAIRS:  a (h_s, h_t) bin must have at least this many pairs to
#                    enter the fit (drops the cross-day hotspot outliers).
#   T_BAND_H:       cap pairs at h_t ≤ this — avoids the structural day-night
#                    gap (Stage A only writes daytime slots; cross-day lags
#                    cluster near 17 h with nothing in between).
B1_VARIOGRAM_PAIRS_PER_CELL = 200
B1_VARIOGRAM_PAIRS_PER_SLOT = 200
B1_VARIOGRAM_MIN_BIN_PAIRS  = 30
B1_VARIOGRAM_T_BAND_H       = 10.0
B1_VARIOGRAM_N_BINS_S       = 12
B1_VARIOGRAM_N_BINS_T       = 12

EARTH_RADIUS_KM = 6371.0

# ── B2 RF hyperparameters ────────────────────────────────────────────────────
RF_N_ESTIMATORS     = 200
RF_MAX_DEPTH        = 20
RF_MIN_SAMPLES_LEAF = 5
RF_MAX_FEATURES     = 'sqrt'
RF_N_JOBS           = -1
RF_RANDOM_STATE     = 42

# stage_b_fixes §0.5: explicitly DO NOT use OOB-R² — AOD temporal autocorrelation
# makes the IID-bootstrap OOB estimate optimistic.  Always set oob_score=False.
RF_OOB_SCORE = False

# RF_GRID = {
#     'n_estimators':     [100, 200],
#     'max_depth':        [15, 20],
#     'min_samples_leaf': [5, 10],
#     'max_features':     ['sqrt', 0.5],
# }

RF_GRID = {
    'n_estimators':     [200],
    'max_depth':        [20],
    'min_samples_leaf': [5],
    'max_features':     ['sqrt', 0.5],
}

# Stratified random subsample of ~2×10⁶ rows over (month, slot_idx, aod_decile).
# AOD-decile is the §10 #21 stratification axis — see features.build_training_table.
RF_TRAIN_TARGET_ROWS    = 2_000_000
RF_STRATIFY_BINS        = ('month', 'slot_idx', 'aod_decile')
RF_AOD_DECILES          = 10  # number of equal-population AOD bins

# §8.2.4c follow-up: equal-budget stratification over-represents top deciles
# in the trained-on sample relative to natural prevalence (which is 0.1 by
# decile construction).  When True, train_rf passes sample_weight =
# natural_prevalence / sample_prevalence per decile to RF.fit — restores the
# unconditional residual distribution without losing high-AOD coverage.
RF_USE_DECILE_REBALANCE_WEIGHTS = False # The +0.053 prior is real, not a sampling artifact.** Decile bins are equal-population by construction, and your decile shares were already close to uniform (0.081–0.110) before reweighting. Bumping them to exact uniformity moves the weighted mean residual by only a few hundredths. The +0.053 is the **actual natural Vietnam CAMS underestimation** — CAMS systematically runs ~0.05 low against the merged AOD across the train window, and that's what the RF learned as its prior. Weighting can't remove a real signal in the data.

# §0.5: 5-fold contiguous temporal CV (fold assignment by UTC date).
RF_CV_FOLDS               = 5
RF_INTERNAL_TEST_FRACTION = 0.20

# When True, `tune_rf` also predicts on the fold-train subset of each combo
# to report `train_rmse_mean` alongside the CV val metrics — useful to spot
# overfitting per hyperparameter (large train/CV gap → tree depth too high
# or min_samples_leaf too low).  Adds ~10–20 % wall-clock to the grid sweep
# (predict on ~4× more rows per fold) so leave off for routine tuning.
RF_TUNE_REPORT_TRAIN_RMSE = False

RMSE_CONSISTENCY_TOLERANCE = 0.15

# ── B2 RF training target (§7.8.1 fix-doc refresh) ──────────────────────────
# 'aod_minus_cams' predicts the residual (Stage A − CAMS) → forces the RF to
# learn what CAMS gets wrong rather than re-deriving CAMS, kills the 66 %
# CAMS dominance reported in §8.1.4, and flattens the tree-regression-to-
# the-mean envelope on AOD > 1.5 events.  'aod' is the legacy direct-target
# behaviour used by rf_tuned.joblib.
RF_TARGET_KIND = 'aod_minus_cams'   # {'aod', 'aod_minus_cams'}

# Residual-target bundle name (kept separate from rf_tuned to preserve the
# baseline for A/B comparison in §8 validation notebooks).
RF_RESIDUAL_BUNDLE_NAME = 'rf_tuned_residual'

# ── B2 RF predictor configuration (post-§7.8.1 refresh) ─────────────────────
# Removed: lat_rad, lon_rad (RF was using them as a thinly-veiled lookup table),
#          tp (replaced by antecedent precipitation that carries memory of
#          prior rain instead of a single 30-min snapshot).
# Added:   precip_6h, precip_24h, hours_since_rain_0p1mm
#          (antecedent precipitation — replaces tp)
#          hour_sin, hour_cos, month_sin, month_cos
#          (cyclical temporal context — RF without these has to re-discover
#           the diurnal cycle through ssrd+t2m proxies)
#          cams_aod_lag_3h
#          (transport dynamics — CAMS native cadence is 3-hourly)
RF_FEATURES_DYNAMIC = [
    'cams_aod', 'cams_aod_lag_3h',
    't2m', 'dpt', 'rh', 'sp', 'u10', 'v10',
    'blh', 'tcc', 'tcwv', 'ssrd', 'fal',
    'precip_6h', 'precip_24h', 'hours_since_rain_0p1mm',
    'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
    # ── ST-AOD-context features — REVERTED to the OG covariate-only set.
    # Both the spatial mean and the temporal persistence share one fatal flaw:
    # they are near-oracle on TRAINING rows (observed cells with dense/recent
    # obs) but collapse to a median-imputed constant at the real gaps we fill
    # (clouds are spatially and temporally contagious).  Net effect on blind
    # AERONET:
    #   - obs_aod_mean_5x5 / obs_aod_count_5x5 (28-feat run rf_residual_stctx):
    #       38 % importance, regression-to-mean collapse, R² 0.52→0.43.
    #   - persistence_decay / slots_since_obs (26-feat run rf_residual_persist):
    #       milder same disease — still #1 at 30 % importance, +0.20 clean-air
    #       bias, positive rf_added in every stratum, within-site R WORSE at
    #       every Bac_Lieu stratum (pooled R² only ticked up via Simpson's
    #       paradox across the two sites' different AOD ranges).
    # Both helper grids stay in features.py (`_obs_neighborhood_grids`,
    # `_persistence_lookback_grids`) for the leave-one-out kriging-as-feature
    # / gap-simulated-training experiments that would make ST context honest.
    #   'obs_aod_mean_5x5', 'obs_aod_count_5x5',
    #   'persistence_decay', 'slots_since_obs',
    # ── Dropped post-validation (§8.2.4c follow-up):
    # 'fire_frp_log_24h' — 99 % zero in the training table, importance 0.008,
    #   no movement on the AOD>1.5 envelope.  Per-pixel coincidence of MODIS
    #   FIRMS detections with the Stage A 5 km grid at 25 km / 24 h is too
    #   sparse to drive RF splits; would need plume-transport (HYSPLIT) or
    #   smoke-column reanalysis (CAMS bcaod) to revive.  Loader stays in
    #   ancillary.py for re-enablement; see also `firms_frp_grid()`.
]
RF_FEATURES_STATIC = [
    'elevation', 'land_cover', 'population',
    # ── Dropped post-validation (§8.2.4c follow-up):
    # 'region_idx' — CV R² 0.30→0.40 with it, but blind AERONET R² 0.54→0.44
    #   and wet-season strata residuals drifted apart (Bac_Lieu wet
    #   +0.065→+0.103, Nghia_Do wet +0.045→+0.093).  The RF used the 3-level
    #   categorical as a per-region offset memoriser rather than a
    #   generaliser.  Loader stays in ancillary.py (`region_idx_grid()`).
]
RF_FEATURES_QUASISTATIC = ['ndvi']
RF_FEATURES = RF_FEATURES_DYNAMIC + RF_FEATURES_STATIC + RF_FEATURES_QUASISTATIC

# Antecedent-precip hyperparameters (referenced by features.build_feature_grid).
ANTECEDENT_RAIN_THRESHOLD_MM = 0.1
ANTECEDENT_RAIN_LOOKBACK_H   = 72.0
CAMS_LAG_HOURS               = 3.0

# ── ST-AOD-context features (Stage B fix — close the kriging-baseline gap) ──
# The kriging baseline beats the pure-covariate RF on the AERONET-blind panel
# because kriging interpolates the *actual Stage A AOD field*, while the RF
# (pre-this-change) sees only met covariates + CAMS — no signal from the AOD
# field itself.  Three additions feed the RF spatial and temporal summaries
# of that field so it can both match kriging on short gaps AND keep its
# nonstationary edge.
#
# Spatial — obs_aod_mean_5x5, obs_aod_count_5x5
#   Mean of valid Stage A AOD in an OBS_NEIGHBORHOOD_BOX × OBS_NEIGHBORHOOD_BOX
#   window centred on the cell, EXCLUDING the centre (at training time the
#   centre is the target y — including it would leak the label).
#
# Temporal — persistence_decay, slots_since_obs
#   `persistence_decay = last_obs_aod * exp(-slots_since_obs / PERSISTENCE_TAU_SLOTS)`
#   where `last_obs_aod` is the most recent STRICTLY PRIOR observation at the
#   cell within PERSISTENCE_LOOKBACK_SLOTS slots back.  τ is the lever — small τ
#   forgets fast (trust only very recent), large τ trusts older obs.  Tune on
#   blind AERONET R² at NGHIA_DO (the site where the kriging gap lives).
OBS_NEIGHBORHOOD_BOX        = 5   # 5x5 ≈ 25 km at 0.05° grid (matches kriging scale)
PERSISTENCE_TAU_SLOTS       = 6   # e-folding 3 h; matches CAMS native cadence
PERSISTENCE_LOOKBACK_SLOTS  = 24  # hard cap on backward search (~1/2 daytime window)

# ── §8 — slots_since_last_observed audit bins (validation-side only) ─────────
# Counted in 30-min steps; nighttime gap ≈ 26-30 steps (13-15 h) depending on day.
SSO_BINS   = [-1, 0, 1, 4, 19, 49, 1_000_000]
SSO_LABELS = ['observed', '1 slot', '≤2 h', '≤10 h', '≤25 h (one night)', '>25 h']
