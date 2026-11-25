"""Shared settings for the China→China spatial transfer-learning (TFL) pilot.

Unit of analysis: one 0.05° grid cell × one UTC hour, only cells that contain CNEMC stations
(training labels and evaluation both live there; no full-grid maps are needed for the pilot).

Grid: same convention as the Vietnam grid / Stage A (row 0 at the north edge, cell centre at +res/2),
extended to China: 73–135°E, 18–54°N → 720 × 1240 cells.

Regions (Gupta et al. 2024 transfer setting: data-rich source → distinct, data-poor target):
    target   South China, lat < 25°N, lon ≥ 97°E (Guangdong, Guangxi, Hainan, Yunnan, south Fujian) —
             the Chinese region most similar to Vietnam
    central  source 1: 26–34°N, lon ≥ 105°E
    north    source 2: 34–42°N, lon ≥ 105°E
The 1° gap between target and `central` keeps source cells out of the target's immediate neighbourhood,
so transfer cannot work through plain spatial autocorrelation.

Time: PM labels are hour-ENDING Beijing time; time_utc = time_bjt − 8 h is the END of the 1-h window,
paired with ERA5 at that UTC hour (accumulations cover [HH−1, HH)) and with satellite AOD observed within
±30 min of HH:00 — the convention of the Vietnam hourly frame (pm25_map/model/_data.py).
"""
from pathlib import Path

SLOW = Path("/home/slow_data/Air_Quality")
OUT = SLOW / "tfl"                                   # data products of this pilot

PM25_DIR = SLOW / "AQI/china/pm25"                    # china_pm25_YYYY.csv.gz (time_bjt, station_code, pm25)
STATIONS = SLOW / "AQI/china/stations.csv"
ERA5_DIR = SLOW / "ERA5/china/_monthly_raw"           # era5_YYYYMM.nc, hourly UTC, 0.25°
MAIAC_DIR = SLOW / "MODIS_MCD19A2/china/raw"          # YYYY/DDD/MCD19A2.*.hdf
DEM_DIR = SLOW / "DEM/china/raw"                      # Copernicus GLO-30 1°×1° tiles

# ── grid ────────────────────────────────────────────────────────────────────────
RES = 0.05
LON_MIN, LON_MAX, LAT_MIN, LAT_MAX = 73.0, 135.0, 18.0, 54.0
NROW = round((LAT_MAX - LAT_MIN) / RES)
NCOL = round((LON_MAX - LON_MIN) / RES)

# ── period / hours ──────────────────────────────────────────────────────────────
START, END = "2020-01-01", "2020-12-31"               # pilot year; ERA5-China currently covers 2020-01 → 2021-05
UTC_HOURS = range(0, 10)                              # 00–09 UTC = 08:00–17:00 Beijing time (Wang et al. 2024)

# ── regions ─────────────────────────────────────────────────────────────────────
REGIONS = {
    "target": dict(lat=(18.0, 25.0), lon=(97.0, 135.0)),
    "central": dict(lat=(26.0, 34.0), lon=(105.0, 135.0)),
    "north": dict(lat=(34.0, 42.0), lon=(105.0, 135.0)),
}

# ── station trust (stations.csv from Data_Collection/AQI/china_stations.py) ─────
MAX_SHIFT_KM = 5.0          # drop codes whose two coordinate sources disagree by more (possible relocation)

# ── predictors ──────────────────────────────────────────────────────────────────
ERA5_VARS = ["T2m", "RH", "Psfc", "U10", "V10", "PBLH", "CloudCover", "TCWV", "SolarRad", "Precip", "CAPE"]
DEM_VARS = ["elev", "elev_std"]
# MAIAC AOD_QA (MCD19 user guide): bits 0–2 cloud mask (1 = clear), bits 5–7 adjacency mask (0 = normal/clear).
# Wang et al. 2024 keep "CloudMask = Clear and AdjacencyMask = Clear".
MAIAC_CLOUD_CLEAR, MAIAC_ADJ_CLEAR = 1, 0
AOD_VALID = (0.0, 5.0)
AOD_SLOT_MIN = 30           # an orbit counts for HH:00 UTC if it lies within ±30 min (Vietnam Stage A convention)

# ── PM2.5 QC (CNEMC-specific; the three reference papers state no outlier rules beyond dropping nulls) ──
PM_RANGE = (1.0, 1000.0)    # µg/m³; raw CNEMC has impossible values up to 200,000
SPIKE_RATIO, SPIKE_MIN_JUMP = 3.0, 100.0     # isolated spike: ≥3× AND ≥100 above both neighbouring hours
CONST_RUN_HOURS = 12        # identical consecutive integer values for ≥12 h (stuck analyser) …
CONST_RUN_MIN_VALUE = 10.0  # … above this level (repeats of small integers at clean sites are plausible)
