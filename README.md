# Air_Quality — High-Resolution AOD Mapping over Vietnam

Codebase for the thesis **"Building a High Spatiotemporal Resolution AOD Map for Vietnam
from Multiple Satellite Observations."**

The project builds a **0.05° (~5.5 km), 30-minute** merged and gap-filled Aerosol Optical
Depth (AOD) product over Vietnam (8°N–23.5°N, 102°E–110°E) for **Sep 2022 – Apr 2026**, by
fusing four satellite sensors, correcting their biases against reanalysis, and filling the
large monsoon-cloud gaps with two independent gap-fillers that are compared head-to-head.

---

## Motivation

Vietnam is among the most air-pollution-affected countries in Southeast Asia, yet no
operational-quality, high-resolution AOD product exists specifically for it. The domain is
hard: Himawari sees northern Vietnam at very high viewing-zenith angle, monsoon cloud cover
leaves valid retrievals on only ~10% of hourly observations, and there are just **two
AERONET stations** (Nghia Do in the north, Bac Lieu in the south) with none in the centre.
Gap-filling is therefore not auxiliary — it is the dominant component of any usable product.

The work is organised around four research questions:

- **RQ1** — Which retrieval per sensor (Himawari L2 vs L3; VIIRS SNPP vs NOAA-20) performs
  best over Vietnam, and does the best choice differ by region/season?
- **RQ2** — Does region/season-aware bias correction against MERRA-2 plus triple-collocation
  inverse-variance fusion beat an equal-weight merge or a single best sensor?
- **RQ3** — How well do ML (Random Forest + reanalysis covariates) and spatiotemporal kriging
  recover AOD where satellites see nothing, and how do they compare?
- **RQ4** — Does the merged + gap-filled product, rolled up to daily means, improve
  AOD–PM2.5 coupling over the Himawari-only baseline (R² = 0.293)?

---

## Pipeline at a glance

```
       L2 Granules                         Reanalysis & ancillary
[Himawari, MAIAC, VIIRS-SNPP,                  [CAMS, MERRA-2, ERA5,
 VIIRS-NOAA20]                                  IMERG, NDVI, elev, land cover]
       │                                            │
       ▼                                            │
[A1: QA filtering]                                  │
       ▼                                            │
[A2: Regrid to 0.05°, 30-min slots]  ← persisted, shared intermediate
       ├──────────────────────────────┐
       ▼                              ▼
  CALIBRATION TRACK            PRODUCTION TRACK (per slot)
  (offline, once)              read A2 grids
  soft-cal vs MERRA-2 ───────► [A4: soft calibration  α·sat + β]
  triple-collocation σ² ─────► [A5: TC-weighted fusion  1/σ²_TC]
                                        ▼
                     ═══ Stage A: 30-min merged AOD ═══
                                        ▼
                        ┌───────────────┴───────────────┐
                        ▼                               ▼
              [B1: ST kriging]                [B2: Random Forest]
        Yang & Hu 2018 metric variogram   per-slot RF on the same
        on the (AOD − CAMS) residual      (AOD − CAMS) residual
                        └───────────────┬───────────────┘
                                        ▼
              ═══ Stage B: two parallel 30-min gap-filled products ═══
                                        ▼
        [Validation: held-out AERONET + coverage/SSO audit + case studies]
                                        ▼
        [Post-processing: physics normalisation → daily roll-up → PM2.5 R²]
```

**Stage A** (`AOD_map/stage_a/`) turns raw per-sensor granules into a single TC-fused 30-min
merged field. It splits into a one-time **calibration track** (fit soft-calibration `α, β`
against MERRA-2 and triple-collocation error variance `σ²` per _(sensor, region, season)_
stratum) and a per-slot **production track** (apply those tables and fuse by `1/σ²`).

**Stage B** (`AOD_map/stage_b/`) fills the gaps in each day's observation window with two
**parallel products** — spatiotemporal kriging (B1) and a Random Forest with reanalysis +
meteorology covariates (B2) — both predicting the `AOD − CAMS` residual and adding CAMS back.
They are evaluated head-to-head, never blended.

---

## Repository layout

### Core pipeline

| Path                                 | What it does                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [AOD_map/stage_a/](AOD_map/stage_a/) | **Stage A** — QA, gridding, MERRA-2 soft calibration, triple-collocation σ², TC-weighted fusion. Driver: [run_stage_a.py](AOD_map/stage_a/run_stage_a.py); calibration CLI: [run_collocate.py](AOD_map/stage_a/run_collocate.py); config: [config.py](AOD_map/stage_a/config.py).                                                                                                                    |
| [AOD_map/stage_b/](AOD_map/stage_b/) | **Stage B** — B1 spatiotemporal kriging ([kriging.py](AOD_map/stage_b/kriging.py)) and B2 Random Forest gap-fill ([rf_gapfill.py](AOD_map/stage_b/rf_gapfill.py)) + feature builder ([features.py](AOD_map/stage_b/features.py)). Driver: [run_stage_b.py](AOD_map/stage_b/run_stage_b.py) / [run_stage_b.ipynb](AOD_map/stage_b/run_stage_b.ipynb); config: [config.py](AOD_map/stage_b/config.py). |
| [models/](models/)                   | Trained/fitted artefacts (see [below](#trained-artefacts)).                                                                                                                                                                                                                                                                                                                                          |

Both stages ship `validate_*.ipynb` notebooks alongside the code (held-out AERONET,
intercomparison, coverage, robustness, per-sensor, case studies).

### Data ingest, processing & EDA (one folder per source)

Each source folder holds its own **download → process → EDA** scripts.

| Folder                 | Source                                 | Role in the product                                                             |
| ---------------------- | -------------------------------------- | ------------------------------------------------------------------------------- |
| [Himawari/](Himawari/) | Himawari-8/9 AHI L2 + L3 (JAXA)        | Temporal backbone; `download_nc/`, `process_nc_to_tif/`, `extract_aod/`, `EDA/` |
| [MODIS/](MODIS/)       | MODIS MAIAC (MCD19A2)                  | Mid-morning/afternoon LEO anchor; EDA + statistics                              |
| [VIIRS/](VIIRS/)       | VIIRS Deep Blue L2 (SNPP + NOAA-20)    | Afternoon LEO anchors; crawl + download + reformat                              |
| [MERRA2/](MERRA2/)     | MERRA-2 M2T1NXAER hourly AOD           | Bias-correction anchor + TC triplet member                                      |
| [CAM/](CAM/)           | CAMS reanalysis AOD                    | Stage B residual target / RF predictor                                          |
| [ERA5/](ERA5/)         | ERA5 meteorology                       | Physics normalisation + RF met covariates                                       |
| [GIS/](GIS/)           | GPM IMERG precipitation (PPS/GES DISC) | Precip covariate + wet-flag diagnostics                                         |
| [AERONET/](AERONET/)   | AERONET V3 L2.0                        | **Held-out** ground truth (validation only)                                     |
| [AQI/](AQI/)           | IQAir / Envisoft PM2.5 stations        | Downstream PM2.5 validation context                                             |
| [EDA/](EDA/)           | —                                      | Cross-sensor comparison notebooks (`AOD_Comparision/`)                          |

### Support

| Path                                                                     | Contents                                                                                                |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| [config/](config/)                                                       | `config.yaml` credential template + loader (fill in your own keys, see [Configuration](#configuration)) |
| [Masterdata/](Masterdata/)                                               | Station metadata CSVs (AERONET, IQAir, Envisoft, meteostat)                                             |
| [util/](util/)                                                           | Shared helpers                                                                                          |
| [environment.yml](environment.yml), [requirements.txt](requirements.txt) | Environment specs                                                                                       |

---

## The product

| Parameter       | Value                                                        |
| --------------- | ------------------------------------------------------------ |
| Spatial domain  | 8°N–23.5°N, 102°E–110°E (Vietnam + ~2° buffer)               |
| Output grid     | 0.05° × 0.05° (~5.5 km); 310 × 160 cells                     |
| Cadence         | 30-min slots (48/day); daytime window per day is data-driven |
| Study period    | Sep 2022 – Apr 2026                                          |
| Training window | Sep 2022 – Dec 2024 (calibration + model fitting)            |
| Held-out window | Jan 2025 – Apr 2026 (never touched until validation)         |
| Ground truth    | AERONET: Nghia Do (Hanoi) + Bac Lieu                         |

**Sensors fused:** Himawari L2, Himawari L3, MODIS MAIAC, VIIRS SNPP, VIIRS NOAA-20 — five
first-class channels, each soft-calibrated independently and arbitrated per pixel by
triple-collocation weights.

---

## Getting started

### Environment

```bash
# conda (recommended — pins GDAL/netCDF/rasterio stack)
conda env create -f environment.yml
conda activate Airqua_env

# or pip
pip install -r requirements.txt
```

Python 3.12, scientific stack (numpy, scipy, xarray, netCDF4, rasterio, geopandas,
scikit-learn, gstools/pykrige for kriging).

### Running Stage A

Stage A reads data from `DATA_ROOT` configured in
[AOD_map/stage_a/config.py](AOD_map/stage_a/config.py) (`/home/slow_data/Air_Quality` by
default — adjust for your machine). The A1+A2 grid is built once, then reused.

```bash
cd AOD_map/stage_a

# 1. Grid raw granules to 0.05° / 30-min (once per date range)
python run_collocate.py grid      --start 2022-09-01 --end 2024-12-31

# 2. Fit calibration tables on the TRAINING window only
python run_collocate.py soft_cal     --start 2022-09-01 --end 2024-12-31   # → soft_calibration.json
python run_collocate.py tc_variance  --start 2022-09-01 --end 2024-12-31   # → tc_error_variance.json

# 3. Production: apply tables + fuse, one NetCDF per 30-min slot
python run_stage_a.py --start 2022-09-01 --end 2026-04-30 --workers 4
```

### Running Stage B

```bash
cd AOD_map/stage_b

# B1 kriging + B2 RF + validation, end-to-end
python run_stage_b.py --all

# individual phases
python run_stage_b.py --b1                 # ST kriging (fit variogram + apply)
python run_stage_b.py --b2                 # RF tune/train + gap-fill
python run_stage_b.py --validate           # head-to-head vs held-out AERONET

# re-infer on new slots without retraining (e.g. after a CAMS backfill)
python run_stage_b.py --b1 --b2 --infer-only --overwrite
```

Validation artefacts are written to a timestamped folder under `STAGE_B_DIR/validation/`.

---

## Trained artefacts

[models/](models/) holds the fitted tables and models the production pipeline loads:

| File                                                 | Stage | Meaning                                              |
| ---------------------------------------------------- | ----- | ---------------------------------------------------- |
| `StageA/soft_calibration.json`                       | A4    | Linear `α, β` per (sensor, region, season)           |
| `StageA/tc_error_variance.json`                      | A5    | Triple-collocation `σ²` per stratum → fusion weights |
| `StageB/rf_default_residual.joblib` (+ `.meta.json`) | B2    | Random Forest predicting the `AOD − CAMS` residual   |
| `StageB/st_variogram.json`, `st_variogram_rf.json`   | B1    | Fitted single-component metric variogram(s)          |

---

## Configuration

Data-download scripts read credentials from [config/config.yaml](config/config.yaml) for
NASA Earthdata (MODIS / VIIRS / MERRA-2), JAXA P-Tree FTP (Himawari), NASA PPS/GES DISC
(GPM IMERG), and ECMWF CDS (ERA5 / CAMS). The committed file ships with **empty
placeholders** — fill in your own accounts before running any downloader. Pipeline paths
(`DATA_ROOT` and friends) are set in each stage's `config.py`.

---
