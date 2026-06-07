"""Stage A configuration: domain, grid, paths, and calibration constants."""

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

# ── Season definitions ────────────────────────────────────────────────────────
DRY_MONTHS = frozenset({10, 11, 12, 1, 2, 3, 4})   # Oct–Apr
WET_MONTHS = frozenset({5, 6, 7, 8, 9})             # May–Sep

# ── AERONET sites used as bias-correction anchors ────────────────────────────
AERONET_SITES = {
    'NGHIA_DO': {'lat': 21.048, 'lon': 105.800, 'region': 'north'},
    'Bac_Lieu':  {'lat': 9.280,  'lon': 105.730, 'region': 'south'},
}

# ── Pre-correction sensor RMSE (actual training collocation, Sep 2022–Dec 2024) ─
# Used as initial ICW weight priors; replaced by post_correction_rmse.json once
# `run_collocate.py train` has been executed.
# Values are dry-season pre-correction RMSE from collocated pairs — more reliable
# than the original Nguyen 2025 figures which do not match our data distribution.
# Key: (sensor_key, region)
SENSOR_RMSE_PRIOR = {
    # Single Himawari entry: L3 wins per pixel, L2 fills L3 gaps.
    # Values seeded from the previous himawari_l3 priors since L3 dominates
    # most pixels.  Updated by post_correction_rmse.json after training.
    ('himawari',    'north'):   0.477,
    ('himawari',    'central'): 0.399,
    ('himawari',    'south'):   0.321,
    ('modis_maiac', 'north'):   0.385,
    ('modis_maiac', 'central'): 0.254,
    ('modis_maiac', 'south'):   0.122,
    ('viirs_snpp',  'north'):   0.301,
    ('viirs_snpp',  'central'): 0.244,
    ('viirs_snpp',  'south'):   0.187,
    ('viirs_noaa20','north'):   0.243,
    ('viirs_noaa20','central'): 0.206,
    ('viirs_noaa20','south'):   0.169,
}

# MODIS MAIAC gets an extra down-weighting factor in the south (R=0.41 → unreliable)
MODIS_SOUTH_WEIGHT_FACTOR = 0.1  # effectively removes MAIAC from south fusion

# Himawari gets an extra ICW down-weighting factor in wet months (May–Sep) to
# suppress cloud-edge contamination during monsoon season.  Replaces the
# previous wet-season QA tightening (HIMAWARI_WET_RF_MIN/UNC_MAX), which
# stripped 70%+ of wet-season retrievals and biased the remainder toward
# high-AOD events.  v3.0-era band-aid restored.
HIMAWARI_WET_WEIGHT_FACTOR = 0.5

# Minimum RMSE used when computing ICW weights (1/RMSE²).
# Wet-season CDF corrections trained on N<20 pairs can reach RMSE≈0.04
# (overfitting), which gives near-infinite weights and unstable fusion.
SENSOR_RMSE_FLOOR = 0.05

# Sayer/Levy expected-error envelope used as an additional RMSE floor:
#   ee(aod) = SENSOR_RMSE_EE_OFFSET + SENSOR_RMSE_EE_SLOPE * mean_aod
# Combined as max(rmse_after_cv, ee(0.3)) so the fusion never trusts
# overfit in-sample RMSE values for low-N strata.
SENSOR_RMSE_EE_OFFSET = 0.05
SENSOR_RMSE_EE_SLOPE  = 0.15
SENSOR_RMSE_EE_REFAOD = 0.3   # representative AOD used to evaluate the envelope

# ── Sensor fusion inclusion rules ────────────────────────────────────────────
# Confidence flag assigned to each merged cell based on contributing sensors.
# 0=no data, 1=Himawari only, 2=LEO only, 3=Himawari+LEO, 4=multi-LEO+Himawari
CONFIDENCE_FLAG = {
    'no_data':          0,
    'himawari_only':    1,
    'leo_only':         2,
    'himawari_plus_leo':3,
    'multi_leo_himawari':4,
}

# ── Physics correction (Step A3) ─────────────────────────────────────────────
GAMMA    = 0.6     # hygroscopic growth exponent (Kotchenruther & Hobbs 1998)
PBLH_MIN = 50.0    # m — minimum PBLH to prevent near-zero division

# ── QA filter thresholds ──────────────────────────────────────────────────────
# Himawari L2
HIMAWARI_RF_MIN  = 0.5    # fine-mode fraction minimum (Band 6)
HIMAWARI_UNC_MAX = 0.5    # absolute retrieval uncertainty maximum (Band 2)
HIMAWARI_SZA_MAX = 70.0   # solar zenith angle maximum (degrees)
HIMAWARI_VZA_MAX = 60.0   # viewing zenith angle hard cut (degrees)
HIMAWARI_VZA_SOFT = 55.0  # VZA > 55° flagged lower confidence (not discarded)

# Wet-season Himawari QA tightening removed (v3.2) — the strict thresholds
# stripped ~70% of monsoon retrievals and biased the survivors toward extreme
# events.  Wet-season cloud-edge contamination is now handled in fusion.py via
# HIMAWARI_WET_WEIGHT_FACTOR (ICW down-weight), which preserves coverage.

# VIIRS Deep Blue L2
VIIRS_QA_MIN_LAND  = 2   # 0=no retrieval, 1=poor, 2=moderate, 3=good
VIIRS_QA_MIN_OCEAN = 1

# ── Satellite geometry ────────────────────────────────────────────────────────
HIMAWARI_SAT_LON = 140.7     # sub-satellite longitude (degrees E)
EARTH_RADIUS_KM  = 6371.0
GEO_ORBIT_KM     = 42164.0   # geostationary orbit radius from Earth centre

# ── Temporal parameters ───────────────────────────────────────────────────────
TZ_OFFSET_HOURS = 7    # Vietnam local time = UTC + 7
SLOT_MINUTES    = 30   # 30-min merged product cadence
LEO_WINDOW_MIN   = 30   # ±minutes window for Himawari L2 / VIIRS–AERONET co-location
MODIS_WINDOW_MIN = 30   # ±minutes window for per-orbit MODIS–AERONET co-location

# ── CDF bias-correction parameters (Ahn et al. 2021) ─────────────────────────
CDF_N_QUANTILES = 200   # quantile points used to build empirical CDF
CDF_MIN_PAIRS   = 100   # minimum matched pairs to fit CDF; fall back to linear below

# ── Collocation spatial sampling (Ichoku et al. 2002; Levy et al. 2010) ───────
# Flag values written to the spatial_flag column of collocated CSVs
COLLOCATE_SPATIAL_FLAG_EXACT = 0   # station falls in the exact pixel/grid cell
COLLOCATE_SPATIAL_FLAG_3X3   = 1   # 3×3 native-cell neighbourhood mean (fallback)
COLLOCATE_SPATIAL_FLAG_5X5   = 2   # 5×5 native-cell neighbourhood (low-N stratum fallback)

# Reject matchup if within-neighbourhood AOD std exceeds the retrieval's own
# expected-error (EE) envelope at the observed AOD level.
# threshold = max(BOX_STD_ABS_FLOOR, BOX_STD_SLOPE[sensor] × mean_box_aod)
BOX_STD_ABS_FLOOR = 0.05   # absolute floor shared across sensors

# One-sigma EE slopes per product (used for box heterogeneity rejection)
# MODIS MAIAC:   Lyapustin 2018 / Falah 2021 — tighter algorithm
# VIIRS Deep Blue: Sayer 2019
# Himawari AHI:  Zhang 2019 (AHI-specific validation)
BOX_STD_SLOPE = {
    'himawari_l2':  0.20,
    'himawari_l3':  0.20,
    'viirs_snpp':   0.20,
    'viirs_noaa20': 0.20,
    'modis_maiac':  0.10,
}

# Himawari and MODIS MAIAC are on fixed grids; neighbourhoods use cell-index half-widths
# (half_width 0 = exact cell, 1 = 3×3 box, 2 = 5×5 box).

# VIIRS uses raw swath data → distance-based neighbourhood using aggregated pixel size
VIIRS_PIXEL_KM     = 6.0               # aggregated L2 pixel at nadir (~6 km × 6 km)
VIIRS_EXACT_MAX_KM = VIIRS_PIXEL_KM / 2        #  3.0 km — exact pixel
VIIRS_3X3_MAX_KM   = VIIRS_PIXEL_KM * 1.5      #  9.0 km — 3×3 VIIRS cells
VIIRS_5X5_MAX_KM   = VIIRS_PIXEL_KM * 2.5      # 15.0 km — 5×5 VIIRS cells

# MODIS MAIAC is on a 1 km sinusoidal grid; distance thresholds ≡ ±N cells
MODIS_PIXEL_KM     = 1.0               # MAIAC native 1 km pixel
MODIS_EXACT_MAX_KM = MODIS_PIXEL_KM / 2        # 0.5 km — exact 1 km cell
MODIS_3X3_MAX_KM   = MODIS_PIXEL_KM * 1.5      # 1.5 km — ±1 cell (3×3)
MODIS_5X5_MAX_KM   = MODIS_PIXEL_KM * 2.5      # 2.5 km — ±2 cells (5×5)

# ── Data paths ────────────────────────────────────────────────────────────────
DATA_ROOT        = Path('/home/slow_data/Air_Quality')
HIMAWARI_L2_DIR  = DATA_ROOT / 'Himawari' / 'L2_AOD'
HIMAWARI_L3_DIR  = DATA_ROOT / 'Himawari' / 'L3_AOD'
VIIRS_SNPP_DIR   = DATA_ROOT / 'VIIRS' / 'L2' / 'AERDB_L2_VIIRS_SNPP'
VIIRS_N20_DIR    = DATA_ROOT / 'VIIRS' / 'L2' / 'AERDB_L2_VIIRS_NOAA20'
MODIS_DIR        = DATA_ROOT / 'MODIS_MCD19A2' / 'raw'
AERONET_DIR      = DATA_ROOT / 'AERONET' / '10'
ERA5_MONTHLY_DIR = DATA_ROOT / 'ERA5' / '_monthly_raw'

OUTPUT_DIR       = DATA_ROOT / 'Stage_A'
EXTRACT_DIR      = OUTPUT_DIR / 'extracted'     # raw satellite AOD time series at stations
COLLOCATE_DIR    = OUTPUT_DIR / 'collocated'    # satellite–AERONET matched pairs
GRIDDED_DIR      = OUTPUT_DIR / 'gridded'       # per-sensor 30-min gridded arrays
MERGED_DIR       = OUTPUT_DIR / 'merged'        # final ICW-merged 30-min NetCDFs
BIASC_DIR        = OUTPUT_DIR / 'bias_corr'     # bias-correction coefficient files

# Thesis §7.4.2: LEO–Himawari spatial-offset map (NetCDF), populated by
# `run_collocate.py leo_offset` from previously merged Stage A files.
LEO_HIMAWARI_OFFSET_FILE = BIASC_DIR / 'leo_himawari_offset.nc'
LEO_HIMAWARI_MIN_PAIRS   = 30          # cells with fewer paired observations are masked
LEO_HIMAWARI_SMOOTH_SIGMA = 3.0        # Gaussian sigma in cells (~15 km at 0.05°)

# ── VIIRS overpass grouping ───────────────────────────────────────────────────
# Consecutive granules whose UTC timestamps differ by ≤ this threshold are
# considered the same overpass and pooled before spatial extraction.
VIIRS_OVERPASS_GAP_MIN = 5
