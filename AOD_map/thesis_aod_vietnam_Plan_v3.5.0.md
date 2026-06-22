# High spatiotemporal AOD Mapping of Vietnam: Multi-Source Satellite Fusion with Bias Correction and Spatiotemporal Gap-Filling

### Thesis Framework & Methodology — Draft 3.5.0

> **What moved from v3.4.0 → v3.5.0.** This revision absorbs `stage_b_fixes.md` and reconciles the plan with the implementation actually living in `stage_a/` and `stage_b/`. The architectural pivot of v3.4.0 (AERONET-independent bias correction via MERRA-2 and AERONET-independent fusion weights via triple collocation) is unchanged. Stage B has been substantially rewritten: the v3.4 daily aggregation (old §7.6) is **deleted**, and Stage B now runs end-to-end at 30-min cadence as **two parallel products** — B1 spatiotemporal kriging (Yang & Hu 2018 sum-metric) and B2 Random Forest gap-fill (Youn 2024 / Chen 2023 SIM, at 30-min). The Chen 2023 TIM upsampler and the old v3.4 §7.8.2 RF + DNN "Candidate 2" are dropped. The output schema becomes two parallel product trees. Validation is re-cut for slot-level head-to-head comparison.
>
> A short **§14 Implementation drift register** at the end of this document records places where the live code deliberately differs from a literal reading of `stage_b_fixes.md` (notably the ST-kriging temporal window and neighbour cap, and the RF training subsample strategy). These are configuration choices, not architectural disagreements; the design rationale and the as-built constants are both quoted.

---

## 1. Context, Motivation, and Research Questions

### 1.1 The problem

Vietnam is among the most air-pollution-affected countries in Southeast Asia, with Hanoi consistently ranking among the most polluted cities globally. A reliable, high-resolution, high spatiotemporal aerosol optical depth (AOD) map is the essential upstream input for any operational PM2.5 estimation system over Vietnam. No such product currently exists at operational quality specifically for Vietnam.

Two recent products come closest to what is needed and define the gap this thesis fills:

- **Gupta et al. (2024)** produced a global merged Dark Target AOD product at 0.25°/30-min from six LEO and GEO sensors. It applies one retrieval algorithm uniformly to every sensor, uses equal-weight averaging across sensors with no AERONET correction, and serves a global audience. Over Vietnam this resolution is too coarse (~28 km), and Dark Target is not the best algorithm for every sensor over Vietnam's surface and aerosol types.
- **Ahn et al. (2021)** produced an hourly composite AOD over Northeast Asia by CDF-matching each sensor to AERONET, extending the per-site corrections spatially via Inverse Distance Weighting (IDW), and merging sensors via Inverse Composite Weighting (1/RMSE², "ICW"). Their domain includes two Vietnamese AERONET sites (Nghia Do, Son La). It is the methodological template closest to this thesis. However, their domain centres on Korea/Japan/China, their product is hourly (not 30-min), and they did not address dense gap-filling for the cloud-prone tropics.

This thesis adapts and extends those approaches for Vietnam's specific conditions, addressing four characteristics of the Vietnam domain that distinguish it from the regions where those products were developed:

1. **Himawari operates at very high viewing zenith angle (VZA) over northern Vietnam** (Vietnam is on the edge of the AHI disk), elongating the atmospheric path and enlarging pixel footprints by 2–5× over Hanoi (Gupta et al., 2024).
2. **Monsoon cloud cover is severe.** Valid Himawari L2 retrievals exist for only **10.3% of all hourly observations** across Vietnam, dropping to 6.9% in July (Nguyen et al., 2025). This means gap-filling is not optional — it is the dominant component of any usable product.
3. **Only two AERONET stations** (Nghia Do in the north, Bac Lieu in the south) are available, with no station in Central Vietnam. Bias correction must therefore be region-aware but spatially extrapolated where ground truth is absent.
4. **Aerosol regimes differ sharply between regions.** The north is dominated by anthropogenic + transboundary biomass burning, with shallow boundary layers and dry-season inversions; the south is influenced by marine aerosols and convective mixing; the centre has mixed orographic/maritime regimes. A single sensor or single bias correction cannot serve all three.

### 1.2 Research questions

- **RQ1.** Which AOD retrieval product per sensor (Himawari L2 vs. L3; VIIRS SNPP vs. NOAA-20) performs best over Vietnam, and does the best choice differ by region or season?
- **RQ2.** Can region- and season-aware bias correction against MERRA-2, combined with inverse-variance fusion derived from triple collocation, produce a fused AOD product that is more accurate over Vietnam than either an equal-weight merge (Gupta 2024 style) or a single best sensor?
- **RQ3.** How effectively can ML-aided spatiotemporal gap-filling (RF with reanalysis covariates) and spatiotemporal kriging (Yang & Hu 2018) recover usable AOD on slots/regions where satellites see nothing, and how do those two methods compare to each other?
- **RQ4.** Does the resulting merged + gap-filled product, aggregated to daily means as a post-processing step, improve daily AOD–PM2.5 coupling over Vietnam compared to the Himawari-only baseline of R² = 0.293 established by Nguyen et al. (2025)?

### 1.3 Claimed contributions

1. The first published Vietnam-specific multi-sensor merged AOD product at 0.05°/30-min covering Sep 2022 – Apr 2026, **delivered as a gap-filled dense product within each day's observation window** (Stage B) rather than only as the sparse Stage A merge.
2. A regional/seasonal **soft-calibration table** (linear α, β per sensor primarily against MERRA-2, with a documented AERONET-anchored fallback on a temporal sub-split of the training window for strata where the MERRA-2 anchor fails — §7.4.1.1) and an **AERONET-independent triple-collocation error-variance table** per sensor. Both are reusable by future studies; the §8 held-out window (Jan 2025 – Apr 2026) is preserved for validation, untouched by either calibration step.
3. An empirical, slot-level **head-to-head comparison** over Vietnam of two gap-fill strategies: spatiotemporal kriging (Yang & Hu 2018 sum-metric variogram) and Random Forest with reanalysis + meteorology covariates (Youn 2024 / Chen 2023 SIM, at 30-min cadence) — the first such comparison for this domain.
4. An evidence-based set of recommendations for upstream PM2.5 mapping in Vietnam, including the central-Vietnam AERONET gap.

---

## 2. Related Work and Methodological Positioning

The thesis builds directly on three product/methodology families. Section 7 maps each methodological choice to its source.

| Predecessor                                | Domain                         | Spatial / Temporal | Bias correction                                       | Sensor fusion      | Gap-filling                             | Why insufficient for Vietnam                                                     |
| ------------------------------------------ | ------------------------------ | ------------------ | ----------------------------------------------------- | ------------------ | --------------------------------------- | -------------------------------------------------------------------------------- |
| **Gupta et al. 2024** (NASA LEO-GEO DT)    | Global                         | 0.25° / 30-min     | None                                                  | Equal-weight mean  | None                                    | Coarse; single algorithm not optimised per sensor for Vietnam; no AERONET tuning |
| **Ahn et al. 2021** (NE Asia composite)    | NE Asia incl. Nghia Do, Son La | ~5 km / hourly     | Per-site CDF cubic polynomial → IDW spatial extension | ICW (1/RMSE²)      | None                                    | Centred on Korea; no dense gap-filling; hourly not 30-min                        |
| **Chen et al. 2023** (Himawari ML gap-fill)| BTH/YRD/PRD China              | 0.05° / 10-min     | None (uses raw AHI)                                   | Single sensor      | SIM (RF, per slot) + TIM (DNN upsampler)| Single sensor; trained on temperate China not tropical monsoon                   |
| **Youn et al. 2024** (RF Himawari gap-fill)| South Korea                    | 0.05° / hourly     | None                                                  | Single sensor      | RF + CAMS + ERA5 met (12 vars)          | Single sensor; temperate climate                                                 |
| **Yang & Hu 2018** (ST kriging AOD)        | Beijing                        | 1 km / daily       | None                                                  | Single sensor      | Sum-metric ST kriging                   | Single sensor, daily, mid-latitude                                               |
| **Nguyen et al. 2025** (this team)         | Vietnam                        | Point sites        | Validation only                                       | None               | None                                    | Validation study only — no gridded product produced                              |

**This thesis = Ahn 2021's inverse-variance fusion framework adapted to Vietnam with region/season strata, with per-sensor bias correction anchored against spatially complete MERRA-2 reanalysis (not per-AERONET-site CDFs), fusion weights derived from triple-collocation σ² (not AERONET-RMSE), and a Stage B gap-filling step that ships two parallel products — Yang & Hu 2018 sum-metric ST kriging as the geostatistical baseline, and Youn 2024 / Chen 2023 SIM-style RF as the ML candidate — both running at 30-min cadence and compared head-to-head. The empirical anchors come from Nguyen et al. 2025.**

What is genuinely new:

- **Vietnam-specific sensor algorithm selection per region**, not a single retrieval algorithm everywhere (departure from Gupta 2024).
- **Region- and season-stratified bias correction anchored against spatially complete MERRA-2 reanalysis** rather than discrete AERONET sites, eliminating the central-Vietnam ground-truth gap that constrained Ahn 2021 (and that earlier drafts of this plan tried to patch with a LEO-anchored offset map).
- **AERONET-independent fusion weights** derived from triple-collocation σ² (Stoffelen 1998; McColl 2014), so AERONET is preserved entirely for held-out validation.
- **Two-stage product design with two parallel Stage B methods**: a bias-corrected fused product (analogous to Ahn 2021) followed by two ML/geostatistics gap-fillers (analogous to Chen 2023 SIM and Yang & Hu 2018 respectively) producing complete 30-min dense products within each day's observation window. The literature evaluates one gap-fill method at a time; doing two side-by-side over the same domain is uncommon.
- **Validation tied to the downstream PM2.5 application** with documented RANSAC-based daily-aggregate metrics from Nguyen et al. 2025 as the baseline to beat.

---

## 3. Study Domain, Period, and Output Specification

| Parameter                  | Value                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------ |
| Spatial domain             | Vietnam + ~2° buffer (8°N–23.5°N, 102°E–110°E)                                         |
| Output grid                | 0.05° × 0.05° (~5.5 km); `LATS` × `LONS` = **310 × 160**                              |
| Native output cadence      | 30-min slots; per-day daytime window is **data-driven** from Stage A filenames (§7.6.0), typical ~20–22 slots/day |
| Optional downstream aggregations | Daily / monthly / seasonal (post-processing; used only for the §8 RQ4 PM2.5 comparison) |
| Study period               | Sep 2022 – Apr 2026 (~3.7 years, matching AERONET availability at Nghia Do/Bac Lieu) |
| Training partition         | Sep 2022 – Dec 2024 (~28 months) — used by §7.4 calibration and §7.7/§7.8 model fitting |
| Held-out partition         | Jan 2025 – Apr 2026 (~16 months) — never touched until §8                            |
| Validation sites           | AERONET: Nghia Do (Hanoi, 21.048°N) and Bac Lieu (9.28°N)                            |
| Indirect validation        | 27 Envisoft PM2.5 stations (10 north / 8 central / 9 south, ≥85% completeness)       |

The domain extends ~2° beyond Vietnam's borders because key aerosol sources (mainland-SEA biomass burning, southern Chinese dust/industrial transport) are transboundary. Cutting at the border would corrupt edge retrievals and edge-weighted gap-fills.

---

## 4. Data Sources

### 4.1 Satellite AOD products (kept)

| Sensor           | Algorithm                                    | λ      | Native res. | Temporal           | Role in fusion                                           |
| ---------------- | -------------------------------------------- | ------ | ----------- | ------------------ | -------------------------------------------------------- |
| Himawari-8/9 AHI | JAXA Standard V3 (L2 + L3)                   | 500 nm | 0.05°       | 10 min continuous  | **Temporal backbone** — only source during most LEO gaps |
| MODIS Terra+Aqua | MAIAC (MCD19A2, combined Terra+Aqua product) | 550 nm | 1 km        | ~10:30 & ~13:30 LT | Mid-morning / afternoon LEO accuracy anchor              |
| VIIRS SNPP       | Deep Blue L2 (AERDB_L2_VIIRS_SNPP)           | 550 nm | 6 km        | ~13:30 LT          | High-accuracy afternoon anchor                           |
| VIIRS NOAA-20    | Deep Blue L2 (AERDB_L2_VIIRS_NOAA20)         | 550 nm | 6 km        | ~13:30 LT (offset) | Second afternoon pass; increases LEO coverage            |

Empirical notes carried over from v3.4 are unchanged: Himawari 8 → 9 transition treated as one record; Himawari 500 nm not Ångström-harmonised (negligible accuracy gain per Nguyen 2025); Himawari L2 vs L3 soft-calibrated independently and merged per-pixel by σ²_TC (§7.4.1, §7.5); MAIAC kept (1 km coverage); MODIS Deep Blue rejected (too sparse over Vietnam); VIIRS L3 daily 1° rejected (destroys spatial structure).

### 4.2 Ground truth — AERONET V3 L2.0

| Site             | Lat / Lon           | Period covered      | N observations | Mean AOD₅₅₀ |
| ---------------- | ------------------- | ------------------- | -------------- | ----------- |
| Nghia Do (Hanoi) | 21.048°N, 105.800°E | Feb 2022 – Apr 2026 | 13,172         | 0.699       |
| Bac Lieu         | 9.28°N, 105.73°E    | Feb 2022 – Apr 2026 | 15,690         | 0.212       |

AERONET is interpolated from 500/675 nm to 550 nm using the site-specific Ångström exponent, matching the LEO retrieval wavelength.

### 4.3 Ground PM2.5 — Envisoft (validation context only)

27 stations selected from 63 available on a ≥85% completeness threshold over the full Nguyen 2025 study window (10/8/9 across north/central/south). Hourly PM2.5, 451,082 records, 91.4% overall completeness. Used here only to validate that the merged AOD product captures pollution episodes visible in the PM2.5 record (§8.4); the actual AOD → PM2.5 regression is a downstream project.

**Validation-window completeness relaxation.** No Envisoft station meets the ≥85% bar across the held-out validation window alone (Jan 2025 – Apr 2026 ≈ 510 days). The §8.1.6 case-study analysis therefore relaxes the per-station completeness threshold to ≥50% for that window only; the ≥85% bar still applies to the headline 27-station figure quoted above. This relaxation is flagged in §10.

### 4.4 Ancillary data (covariates for bias correction and gap-filling)

| Dataset                                           | Stage A role                              | Stage B role                                                  |
| ------------------------------------------------- | ----------------------------------------- | ------------------------------------------------------------- |
| ERA5 reanalysis (0.25°, hourly)                   | RH and PBLH for §7.3 physics normalisation | 11 ERA5 variables (T2m, Td2m, RH, SP, U10, V10, PBLH, total cloud cover, TCWV, surface solar, albedo) bilinearly resampled to 0.05° and linearly interpolated in time to the 30-min slot centre — RF features (§7.8.1) |
| CAMS global reanalysis AOD (0.4° × 0.4°, 3-hourly)| —                                         | Bilinear → 0.05°, linear time interp → 30-min — RF feature `cams_aod`. Sole reanalysis-AOD predictor in Stage B (MERRA-2 excluded — already consumed by Stage A; §7.8.1)                            |
| MERRA-2 AOD (0.5° × 0.625°, hourly, TOTEXTTAU)    | Soft-calibration anchor (§7.4.1) and TC triplet member (§7.4.2) | **Excluded** as a predictor (would double-dip the Stage A anchor) |
| GPM IMERG precipitation (0.1°, 30-min)            | Wet-flag validation diagnostic            | Bilinear → 0.05° (native cadence) — RF feature `tp`             |
| MODIS NDVI (MOD13Q1, 0.05°, 16-day composite)     | —                                         | Nearest-on-or-before in time, cached per (year, DOY) — RF feature `ndvi` (quasi-static) |
| Copernicus GLO-30 DEM (30 m → 0.05° mean)         | —                                         | Static RF feature `elevation`                                  |
| MODIS Land Cover (MCD12Q1)                        | Surface-type stratification (informational) | Annual, nearest year ≤ slot year — RF feature `land_cover`     |
| LandScan Global population (~1 km, annual)        | —                                         | Annual — RF feature `population`                                |
| FIRMS active fire                                 | Validation flag for extreme burning days  | Not a model input                                              |

Choice of ancillary predictors mirrors Chen et al. 2023 and Youn et al. 2024 (their RF gap-fill validated this set on AHI) with two Vietnam-specific substitutions documented in §7.8.1.

---

## 5. Empirical Baseline (from Nguyen et al. 2025)

### 5.1 Per-sensor validation against AERONET

| Source        | Station  | N      | R     | Bias   | RMSE  | %EE   |
| ------------- | -------- | ------ | ----- | ------ | ----- | ----- |
| Himawari L2   | Nghia Do | 9,756  | 0.701 | −0.117 | 0.481 | 34.5% |
| Himawari L3   | Nghia Do | 182    | 0.869 | −0.316 | 0.444 | 24.7% |
| MODIS MAIAC   | Nghia Do | 239    | 0.856 | −0.146 | 0.295 | 56.9% |
| VIIRS NOAA-20 | Nghia Do | 103    | 0.915 | +0.022 | 0.271 | 65.0% |
| Himawari L2   | Bac Lieu | 14,092 | 0.733 | −0.021 | 0.150 | 53.6% |
| Himawari L3   | Bac Lieu | 295    | 0.824 | +0.005 | 0.120 | 67.8% |
| MODIS MAIAC   | Bac Lieu | 413    | 0.411 | +0.013 | 0.155 | 42.4% |
| VIIRS NOAA-20 | Bac Lieu | 207    | 0.845 | +0.085 | 0.143 | 45.4% |

### 5.2 Empirical findings that shape the methodology

1. **VIIRS is the most accurate L2 product** at both stations.
2. **Himawari has a strongly asymmetric regional bias** — large negative in the north, near-zero in the south. Single-coefficient correction will not work.
3. **The optimal Himawari product level is region-dependent**: L2 in the high-AOD north preserves events; L3 in the low-AOD south reduces scan noise. Both levels are soft-calibrated against MERRA-2 independently (§7.4.1) and merged per-pixel by σ²_TC into one Himawari fusion input.
4. **MAIAC fails at Bac Lieu against AERONET** (R = 0.411) over reflective Mekong agricultural surfaces. v3.4 removed the v3.3 hand-set 0.1× MAIAC-south down-weight; the §7.4.2 σ²_TC for MAIAC-south now sets its weight automatically.
5. **Inter-sensor agreement degrades north → south** for MODIS–VIIRS but is U-shaped for VIIRS–Himawari; central Vietnam is the weakest spot for sensor consistency.
6. **AOD availability is only 10.3% of hourly slots** (6.9% in July, 16.7% in April). Stage B gap-filling is dominant, not auxiliary, and runs at 30-min cadence throughout (§7.6.0).
7. **Physics correction** (RH, PBLH) lifts hourly Himawari–PM2.5 correlation from r = 0.110 to r = 0.162 — applied as Step A3.
8. **Fine-mode (Rf ≥ 0.5) + uncertainty (≤ 0.5) filters help Himawari**, no measurable improvement for MAIAC (preprocessing already favours fine-mode). Filter Himawari only.
9. **RANSAC robust regression** lifts daily Himawari–PM2.5 R² from 0.065 (OLS) to 0.293 — diagnostic, not a product filter.

---

## 6. Methodology Overview

```
       L2 Granules                         Reanalysis & ancillary
[Himawari, MAIAC, VIIRS-SNPP,                  [CAMS, MERRA-2, ERA5,
 VIIRS-NOAA20]                                  IMERG, NDVI, elev, land cover]
       │                                            │
       ▼                                            │
[Step A1: QA Filtering]                             │
       ▼                                            │
[Step A2: Regrid to 0.05°, 30-min slots]            │
       │   (persisted intermediate, shared)         │
       ├──────────────────────────────────┐         │
       ▼                                  ▼         │
  ┌─ CALIBRATION TRACK ─┐         ┌─ PRODUCTION TRACK ──────────┐
  │ (offline, once)     │         │ (per slot)                  │
  │ collocate sat ↔     │         │ read A2 grids               │
  │ MERRA-2 per         │         │      ▼                      │
  │ (sensor, region,    │         │ [Step A4: Soft calibration  │
  │  season) (§7.4.1)   │         │   linear α·sat + β]         │
  │      ▼              │         │      ▲                      │
  │ fit linear (α, β) ──┼────────►│      └── (α, β) from train  │
  │      ▼              │         │      ▼                      │
  │ triple-collocation  │         │ [Step A5: TC-weighted       │
  │ σ²_TC per stratum  ─┼────────►│   fusion (1/σ²_TC)]         │
  │ (§7.4.2)            │         │      ▲                      │
  │                     │         │      └── σ²_TC from train   │
  │                     │         │      ▼                      │
  │                     │         │ [Step A3: Physics           │
  │                     │         │   normalisation] ◄── ERA5   │
  │                     │         │   (applied after fusion;    │
  │                     │         │    separate output field —  │
  │                     │         │    not fed back to A4/A5)   │
  └─────────────────────┘         └─────────────────────────────┘
                                              ▼
                              ═══ Stage A complete: 30-min merged ═══
                                              ▼
                                ┌────────────┴───────────┐
                                ▼                        ▼
                  [Step B1: ST kriging,        [Step B2: Random Forest
                   Yang & Hu 2018 sum-metric,   per 30-min slot,
                   parallel product tree]       parallel product tree]
                                │                        │
                                └────────────┬───────────┘
                                             ▼
                       ═══ Stage B complete: two 30-min gap-filled products ═══
                                             ▼
                  [Step C: head-to-head validation against held-out AERONET +
                   coverage / SSO audit + Envisoft case studies]
                                             ▼
                  [Optional post-processing: daily roll-up of each product for
                   the §8 RQ4 PM2.5 R² comparison vs Nguyen 2025]
```

Stage A is split into a one-time **calibration / training track** and a per-slot **production track**. Both tracks consume the same persisted Stage A2 intermediate (one NetCDF per 30-min slot containing raw 0.05° gridded AOD per sensor) so gridding runs exactly once per slot per dataset.

Stage B is split into two **parallel product trees** (`output/st_kriging/` and `output/rf/`) at 30-min cadence within each day's data-driven observation window (§7.6.0). Daily aggregation is **not** part of Stage B; the §8.2.6 / RQ4 PM2.5 comparison performs a post-processing daily roll-up.

Calibration runs once in a linear chain (§7.4.4): grid → soft-calibrate vs MERRA-2 → triple-collocation σ² → production. No cyclic bootstrap is needed because neither anchor depends on a prior Stage A pass.

---

## 7. Methodology Detail

### 7.0 AERONET-cell extraction for validation

Under the v3.4/v3.5 architecture, satellite training pairs are constructed against MERRA-2 (§7.4.1) and inter-sensor triplets (§7.4.2), neither of which touches AERONET. AERONET is therefore reserved entirely for held-out validation (§8). The AERONET-cell extraction described in this section is a **validation** workflow, not a training workflow.

Three stages run independently from a shared intermediate: (1) **Stage A2 grid** writes one NetCDF per 30-min slot containing the raw per-sensor 0.05° gridded AOD before any bias correction (`MERGED_DIR` per-sensor variant); (2) **extract** reads that gridded slot and samples each sensor at the AERONET station's 0.05° cell using the tiered 1 / 3×3 / 5×5 cell-neighbourhood fallback; (3) **match** temporally aligns the station-cell extracts to AERONET observations within ±30 min of the slot centre (Himawari L2, VIIRS, MAIAC) or to the day (Himawari L3, kept as a daily comparator). Stages 2 and 3 feed §8.1.1's held-out AERONET metric panel.

These stages are exposed as CLI verbs in `run_collocate.py`: `grid`, `extract`, `match` (plus a `collocate` shortcut). The training-time calibration verbs are `soft_cal` (§7.4.1) and `tc_variance` (§7.4.2), neither of which consumes AERONET.

#### 7.0.1 Spatial matching

Every sensor is sampled from the Stage A2 0.05° gridded slot using the same tiered cell-neighbourhood fallback (Ichoku et al. 2002; Levy et al. 2010 scaled to the production grid):

1. **Exact cell (primary):** the 0.05° cell whose centre is closest to the AERONET station.
2. **3×3 cell neighbourhood (Fallback 1):** mean of all valid cells within one config-grid cell of the station cell.
3. **5×5 cell neighbourhood (Fallback 2):** half-width 2, side ≈ 25 km — used only when the inner tiers are empty.

Each matchup records which tier was used and the within-neighbourhood AOD standard deviation. The scene-homogeneity gate `box_std_max(mean) = max(BOX_STD_ABS_FLOOR=0.05, BOX_STD_SLOPE[sensor] × mean)` is computed at collocation time but **production fusion does not enforce it** — the §7.5 TC-weighted fusion absorbs heterogeneity via the cross-sensor std term and per-stratum σ²_TC. The per-cell `aod_std` and `cv` fields are written to the Stage A2 intermediate (§7.2.1) and exposed for downstream consumers to apply their own `cv ≤ CV_MAX` filter if a use case calls for it.

#### 7.0.2 Temporal matching

**Strategy: satellite-centric.** One record per Stage A2 30-min slot grid. For each snapshot, all AERONET measurements within the matching window are averaged.

| Sensor | Snapshot unit | Matched to | Time window |
|--------|---------------|------------|-------------|
| Himawari L2 | One row per 30-min slot (multiple 10-min files averaged at Stage A2) | AERONET near slot centre | ±30 min |
| Himawari L3 | One row per 30-min slot → aggregated to a daily satellite mean before matching | AERONET daily mean (UTC) | Whole day |
| VIIRS SNPP / NOAA-20 | One row per 30-min slot — Stage A2 pools all granules whose timestamp falls in ±30 min | AERONET near slot centre | ±30 min |
| MODIS MAIAC | One row per 30-min slot — per-orbit UTC timestamp from the HDF global attribute decides slot eligibility | AERONET near slot centre | ±30 min |

Days whose MAIAC HDF lacks a readable orbit-timestamp attribute are skipped entirely; the daily-mean broadcast fallback used by earlier drafts is not applied (see §10 #19).

### 7.1 Step A1 — Quality filtering

**Himawari AHI** — strict JAXA bit-mask gate on the L2 `QA_flag` (Band 4) / L3 `QA_flag_Merged` (Band 8), plus three product-level gates:

- **JAXA strict QA bit-mask** — a pixel passes only when every one of these bits is clean: `data_avail`, `cloud`, `retrieval_ok`, `AOT confidence = 00` ("very good", bits 4–5), `additional_cloud`, `Solz/Satz > 70°` (bit 10), `surface_refl_bad`, `snow/ice`, `turbid_water`. L3 zeroes the snow/ice and turbid-water bits by design.
- **Strict-zero AOT gate**: AOT > `HIMAWARI_AOT_MIN` (= 0.0).
- **Fine-mode fraction** Rf ≥ `HIMAWARI_RF_MIN` (= 0.5, Band 6, L2 only).
- **Retrieval uncertainty** |Uncertainty| ≤ `HIMAWARI_UNC_MAX` (= 0.5).

**MODIS MAIAC (MCD19A2):**

- Valid AOD range: 0 ≤ AOD ≤ 5.
- `AOD_QA` bitmask: bits 8–11 ≤ `MODIS_QA_BITS_MAX` (= 4 — "Best" through "Marginal").
- Terra and Aqua orbits separated by per-file UTC timestamp from the HDF metadata; each orbit's pixels are placed in the Stage A2 slots whose ±30 min window overlaps that orbit's overpass time.

**VIIRS Deep Blue (SNPP + NOAA-20):**

- Uniform QA gate: `QA_Flag_Land ≥ VIIRS_QA_MIN` (= 2) OR `QA_Flag_Ocean ≥ VIIRS_QA_MIN`.
- Valid AOD range: 0 ≤ AOD ≤ 5. SNPP and NOAA-20 treated as separate sensors throughout.

### 7.2 Step A2 — Regridding to 0.05° × 30-min

#### 7.2.1 Spatial aggregation

Box-averaging per Gupta et al. 2020: only pixels whose centre falls inside a 0.05° cell contribute. Output per sensor per slot: mean AOD, within-cell std, coefficient of variation `cv = std / max(mean, 0.02)`, valid-pixel count, theoretical maximum, mean VZA, mean SZA. Stage A2 itself does not apply a heterogeneity rejection.

**Persistence.** Steps A1+A2 are computed once per slot per sensor and written to a per-slot NetCDF in `GRIDDED_DIR`. AERONET validation extraction (§7.0), MERRA-2 soft-calibration (§7.4.1), TC σ² estimation (§7.4.2), and the production fusion driver (§7.5) all read from this intermediate.

#### 7.2.2 Temporal slot assignment per sensor

The 30-min slot cadence (48 slots/day, centred at 00:00, 00:30, …, 23:30 UTC) is a common reference frame the four sensors reach by different paths:

| Sensor | Native cadence | Slot strategy | Window |
|--------|----------------|---------------|--------|
| Himawari L2 | 10-min snapshots | All snapshots within ±15 min of slot centre averaged pixel-wise | ±15 min (~3 files/slot) |
| Himawari L3 | 1-hour composites | Nearest L3 composite within ±30 min used as-is | ±30 min |
| VIIRS SNPP / NOAA-20 | ~6-min granules | All granules within ±30 min pooled, then box-averaged | ±30 min |
| MODIS MAIAC | Multi-orbit daily HDF | Per-orbit UTC timestamps extracted; each orbit included only in slots whose ±30-min window overlaps the overpass | ±30 min per orbit |

Carry-over consequences (Himawari L2 no slot duplication; Himawari L3 consecutive slots share one composite; VIIRS may contribute to two adjacent slots near a granule boundary; MAIAC contributes to at most one or two slot pairs per day) are unchanged from v3.4 and recorded in §10 #18, #19.

### 7.3 Step A3 — Physics normalisation (stored as a separate output, applied after fusion)

```
AOD_phys = AOD_merged × (1 − RH/100)^γ / PBLH
```

with γ = 0.6 (Kotchenruther & Hobbs 1998), PBLH constrained ≥ 50 m, RH and PBLH from ERA5 bilinearly interpolated from 0.25° to 0.05°. ERA5 monthly files are nearest-hour matched to the 30-min slot centre, with a 1-hour absolute tolerance — slots beyond that fallback gracefully (the raw merged AOD is written but the physics field is NaN).

**Ordering note.** Step A3 runs **after** Steps A4 and A5. The physics correction is applied to the fused `AOD_merged` and stored as a separate output field; it is **not** fed back into bias correction or fusion. AERONET measures raw column AOD, so calibration and fusion must operate on raw AOD.

### 7.4 Step A4 — MERRA-2-anchored bias correction + triple-collocation error variance

**Strategy.** Bias correction and fusion weights are both anchored against domain-wide reference data — MERRA-2 reanalysis for bias correction, inter-sensor triple collocation for fusion weights. AERONET is reserved for held-out validation (§8) with one documented exception: §7.4.1.1 defines a per-stratum AERONET-anchored fallback that fires only when the MERRA-2 anchor fails its §8.1.2 gate A, and that fallback is fit and validated entirely inside the training window (the Jan 2025 – Apr 2026 held-out window is never touched). With only two AERONET stations and none in central Vietnam, this is the only design that can deliver a defensible central-region calibration without consuming the validation pool.

Two architectural changes do the work: (1) bias correction is anchored against spatially complete MERRA-2 (Randles 2017; Buchard 2017) using the Ding et al. 2025 linear form; (2) fusion weights are computed by triple collocation (Stoffelen 1998; McColl 2014; Gruber 2016) — no ground-truth reference needed. MERRA-2 is used **only as a training-time anchor**; it does not enter the production merge.

#### 7.4.1 Soft calibration against MERRA-2

For each (sensor, region, season) stratum:

1. At the satellite's 0.05° grid and slot (post-§7.2), collocate every valid satellite pixel with MERRA-2's hourly AOD550 (`TOTEXTTAU`) bilinearly resampled to 0.05° and nearest-hour matched.
2. Fit a linear regression `MERRA-2 = α · sat + β` over the training window (Sep 2022 – Dec 2024). The transfer function is `sat_corrected = α · sat + β`. Linear (rather than full PCHIP CDF) is chosen because MERRA-2 at 0.5° is structurally smoother than satellite AOD at 0.05°.
3. **CV-time guard rail.** 5-fold cross-validated α and β are computed. The stratum routes to `'apply'` when N ≥ `SOFT_CAL_MIN_PAIRS` (= 100), `α_CV ∈ [0.5, 2.0]`, and `|β_CV| ≤ 0.2`. Otherwise route to `'none'`: the satellite enters fusion uncorrected and receives `NONE_PENALTY_FACTOR × σ²_prior` (= 2.0, weight drops ~4×). Output is clipped to `[SOFT_CAL_OUTPUT_MIN=0.0, SOFT_CAL_OUTPUT_MAX=5.0]`.

Strata: **Region** — North (lat ≥ `NORTH_CENTRAL_LAT = 16.0`), Central (`CENTRAL_SOUTH_LAT = 11.5` ≤ lat < 16.0), South (lat < 11.5). **Season** — Dry (Oct–Apr) / Wet (May–Sep). **Himawari level** — L2 and L3 are soft-calibrated independently; the §7.5 stratum-aware per-pixel merge preserves the per-level treatment before fusion.

The trained `(α, β)` per stratum are persisted to `BIASC_DIR/soft_calibration.json` and reloaded at run time. The persisted schema also carries `alpha_cv`, `beta_cv`, `n_pairs`, `rmse_before`, `rmse_after`, `rmse_after_cv`, and the `route` flag for the §8.1.2 pre/post-correction comparison.

#### 7.4.1.1 AERONET-anchored fallback for 'none'-routed strata

The §7.4.1 validation in `validate_bias_correction.ipynb` (§8.1.2 gate A on the held-out summary) revealed that the MERRA-2 anchor clears the gate-A RMSE-drop threshold in only **5 of 14 apply-routed strata** at first pass, with the remaining 9 strata being **re-routed to `'none'`** (notably across `himawari_l3`, `modis_maiac` north/central/south dry, and `viirs_noaa20` north wet). The root cause is MERRA-2's coarser native grid (0.5° × 0.625°) and smoother aerosol field: the linear `MERRA-2 = α · sat + β` regression has no residual signal to lock onto in strata where MERRA-2 is structurally close to satellite already.

To recover usable corrections in those strata without consuming the §8 held-out window, the design is extended with **one documented fallback**:

For any (sensor, region, season) stratum whose MERRA-2 anchor routes to `'none'`, attempt a second fit `AERONET = α · sat + β` using a **temporal sub-split of the training window only**:

* **Fit partition.** Sep 2022 – Jun 2024 AERONET–satellite pairs at Nghia Do (north) and Bac Lieu (south).
* **In-train validation partition.** Jul – Dec 2024 (same training window; never touches the §8 hold-out which starts Jan 2025).
* **Guard rail.** Identical to §7.4.1 — `α ∈ [SOFT_CAL_ALPHA_MIN, SOFT_CAL_ALPHA_MAX]`, `|β| ≤ SOFT_CAL_BETA_ABSMAX`, with sample minimums `AERONET_FALLBACK_MIN_TRAIN_PAIRS = 30` (fit window) and `AERONET_FALLBACK_MIN_VAL_PAIRS = 10` (val window). The MERRA-2 minimum (`SOFT_CAL_MIN_PAIRS = 100`) is relaxed here because AERONET coverage is intrinsically two orders of magnitude sparser; the gate below replaces the lost sample size with an explicit temporal-holdout check.
* **Acceptance gate.** `rmse_aer_val_after ≤ rmse_aer_val_before − GATE_A_RMSE_DROP_MIN` — the same 0.02 drop threshold as gate A, applied to the AERONET-residual on the in-train holdout. If the fit fails this gate, the stratum stays `'none'`.

When the fallback passes, the persisted `SoftCal` record swaps `alpha`/`beta` to the AERONET-anchored fit and records `anchor: 'aeronet'`; the `route` flag flips to `'apply'`. When it fails (or AERONET coverage doesn't exist — i.e. **central Vietnam, which has no AERONET station**), the stratum stays `'none'` and inherits the `NONE_PENALTY_FACTOR × σ²_TC` fusion penalty unchanged. The implementation records both fits' diagnostics in the JSON: `alpha_aer`, `beta_aer`, `n_aeronet_train`, `n_aeronet_val`, `rmse_aer_train_before/after`, `rmse_aer_val_before/after`, plus the `anchor` field (`'merra2' | 'aeronet' | 'none'`).

**Limits of the fallback.** This is the only place AERONET enters a *training* step, and it is a deliberate, scoped exception to the §7.0 / §7.4 "AERONET = held-out only" principle. The bend is contained to a stratum-by-stratum guard-rail recovery — most strata that pass §7.4.1 never see the fallback at all. Central-Vietnam strata cannot benefit (no station), so the central calibration story is unchanged from §7.4.1: MERRA-2 anchor where it clears the gate, `'none'` otherwise. The thesis defense becomes: **MERRA-2 anchor works in a majority of strata; the design includes a documented AERONET-anchored fallback for the strata where it doesn't, validated on a temporal holdout still inside the training window.**

#### 7.4.2 Triple-collocation error variance

For each (sensor, region, season) stratum, the post-soft-calibration error variance `σ²_i` is estimated using triple collocation (Stoffelen 1998 Eq. 5; Gruber 2016 §2.1). McColl 2014 extended-TC additionally recovers the additive bias and a correlation-with-truth metric per sensor; both are computed and persisted alongside σ² for §8.1.3.

**Triplet construction.** Six "sensors" are eligible: {Himawari L2 (soft-calibrated), Himawari L3 (soft-calibrated), MAIAC, VIIRS-SNPP, VIIRS-NOAA20, MERRA-2}. All algorithm-distinct triplets are enumerated; pure-satellite triplets are enumerated when overlap exists. The σ² for each sensor is the **median across all valid triplets containing it**, per (region, season). Negative single-triplet σ² (Stoffelen 1998 §4 — independence violation) are dropped from the aggregate. Below `TC_MIN_TRIPLETS = 50` valid triplets, the stratum returns NaN and the caller falls back to the EE-envelope floor.

**Independence rule.** A triplet is rejected when two members share the **retrieval algorithm**. Specifically:

1. VIIRS-SNPP + VIIRS-NOAA20 share Deep Blue (Sayer 2020) → rejected.
2. MERRA-2 + MAIAC is allowed: MERRA-2's GOCART assimilates MODIS Dark Target NNR (Randles 2017 §3.1), not MAIAC.
3. Himawari L2 + L3 is allowed: JAXA L3 V3 `merged_aot` is a composite of land+ocean retrievals, not an hourly mean of L2 pixels.

**Sensitivity check.** σ²_TC is computed under both rules above (permissive) and a **strict** variant that additionally rejects all pairs sharing the underlying instrument (MERRA-2 + MAIAC, Himawari L2 + L3). The `is_independent_triplet(strict=True|False)` toggle in `triple_collocation.py` produces both tables; if rankings differ between variants, the strict ranking becomes the headline.

**Floor against the Sayer/Levy EE envelope.** σ²_i is floored at `(EE_OFFSET + EE_SLOPE · AOD_ref)² = (0.05 + 0.15 × 0.3)² ≈ 0.009` (Levy 2013; Sayer 2020).

**Sanity benchmark.** The TC-derived σ² is cross-checked once against AERONET–satellite RMSE on training-window pairs at the two AERONET cells (post soft-calibration). Agreement within a factor of 2 is expected; larger gaps trigger a triplet-set audit (usually reveals an undetected error-correlation pair). The benchmark uses training-window AERONET only as a one-time sanity check — AERONET is not used to adjust the weights and the §8 held-out window is not touched.

The TC σ² table is persisted to `BIASC_DIR/tc_error_variance.json`.

#### 7.4.3 Per-sensor correction summary

| Sensor | Soft calibration | TC error variance source |
| --- | --- | --- |
| Himawari L2 | Linear vs MERRA-2 per region+season | Triplets with MAIAC, VIIRS-*, and/or MERRA-2; merged-Himawari σ² is `min(σ²_L2, σ²_L3)` per the §7.5 stratum-aware merge |
| Himawari L3 | Linear vs MERRA-2 per region+season | Same; rolled up via min |
| MAIAC | Linear vs MERRA-2 per region+season | Triplets with Himawari-L2/L3, VIIRS-*, and/or MERRA-2 |
| VIIRS SNPP | Linear vs MERRA-2 per region+season | Triplets with Himawari, MAIAC; not paired with VIIRS-NOAA20 |
| VIIRS NOAA-20 | Linear vs MERRA-2 per region+season | Same; not paired with VIIRS-SNPP |
| MERRA-2 | (not corrected; serves as anchor) | σ² reported from MERRA-2-containing triplets for diagnostics; **not used** in §7.5 |

#### 7.4.4 Calibration ordering (single-pass)

Calibration runs once in a linear chain:

1. **Grid (A1+A2).** `python run_collocate.py grid …` over the full study period — persists per-slot NetCDFs every subsequent step reads.
2. **Soft-calibrate.** `python run_collocate.py soft_cal …` — regresses each (sensor, region, season) against MERRA-2 over the training window; persists `soft_calibration.json`.
3. **TC σ² table.** `python run_collocate.py tc_variance …` — applies soft calibrations, then runs triple collocation per stratum; persists `tc_error_variance.json`.
4. **Production Stage A.** `python run_stage_a.py …` over the full study period; loads both JSON tables and emits 30-min `MERGED_DIR` NetCDFs.

Steps 2 and 3 must be re-run together whenever gridding or QA filters change.

### 7.5 Step A5 — TC-weighted fusion

For each 0.05° cell and 30-min slot:

```
w_i = 1 / σ²_TC,i(region, season, sensor)
AOD_merged = Σ(w_i · AOD_i_corrected) / Σ(w_i)
```

floored at the Sayer/Levy EE envelope. This is identical in form to Ahn 2021's ICW; the variance source is AERONET-independent triplet statistics.

**MERRA-2 inclusion rule.** MERRA-2 does NOT enter the production merge sum. Slots with zero satellite retrievals stay as gaps and are forwarded to Stage B.

**Himawari L2/L3 stratum-aware per-pixel merge.** L2 and L3 are soft-calibrated independently and merged per pixel before fusion: the level having the lower σ²_TC for the (region, season) supplies the primary pixel value, the other fills its gaps. The fusion then sees a single Himawari channel.

**Sensor inclusion rule.** Every available sensor in the slot enters the merge weighted by 1/σ²_TC; if exactly one sensor is present the cell is flagged as low-confidence; if no sensor is present the cell is a gap (forwarded to Stage B). **No hand-set multipliers** — every weight decision flows through `tc_error_variance.json`.

**Stage A output (per 30-min NetCDF in `MERGED_DIR/YYYY/MM/DD/merged_YYYYMMDD_HHMM.nc`):**

- `AOD_merged` — TC-weighted mean of soft-calibrated sensors
- `AOD_std` — cross-sensor weighted spread
- `n_sensors`, `dominant_sensor`, `confidence_flag`
- Per-sensor soft-calibrated grids (diagnostic; written only when data are available)
- `AOD_phys_corrected` — §7.3 physics-normalised
- ERA5 RH and PBLH (when available)
- `weight_sum` — Σw_i (provenance / confidence proxy carried forward to Stage B)

`dominant_sensor` codes: **1 = Himawari (merged L2/L3), 3 = MODIS MAIAC, 4 = VIIRS SNPP, 5 = VIIRS NOAA-20**. `confidence_flag` codes: **0 = no data, 1 = Himawari only, 2 = LEO only, 3 = Himawari + LEO, 4 = multi-LEO + Himawari**.

---

## Stage B — Gap Filling (30-min cadence, two parallel products)

Stage B runs at 30-min cadence end-to-end and produces **two parallel product trees**, one per gap-fill method. Daily aggregation is **not** part of Stage B — the §8.2.6 / RQ4 comparison vs Nguyen 2025 performs an optional daily roll-up as a downstream post-processing step.

### 7.6 Step B0 — Day-window discovery and shared protocol

#### 7.6.0 Daytime window per UTC day (data-driven)

Stage A writes one NetCDF per slot only if that slot has at least one valid Stage A pixel anywhere in Vietnam (`merged_YYYYMMDD_HHMM.nc`). The HHMM in the filename is the source of truth for which slots are observable on a given day.

For UTC day `D`:

1. List `MERGED_DIR/YYYY/MM/DD/*.nc` and parse the HHMM substring of each filename to a slot index in `[0, 47]`.
2. If day `D` has ≥ `WINDOW_MIN_SLOTS_PRESENT` (= 10) observed slots, `slot_first = min(slot_idx)`, `slot_last = max(slot_idx)`, and the inference window is every 30-min slot in `[slot_first, slot_last]` inclusive.
3. Otherwise, fall back to a 7-day median window: the per-day-median of `(slot_first, slot_last)` over the seven days `D ± WINDOW_MEDIAN_HALF_DAYS` (= ±3), restricted to neighbour days that themselves clear the 10-slot bar. When the target day has *some* but < 10 observations, the median window is widened to include them.
4. At dataset edges (first/last few days), the one-sided median window is used.
5. If neither path yields a window, the day has no inference window and is skipped.

The implementation lives in `stage_b/slots.py::discover_day_window`. Typical Vietnam window size is ~20–22 slots/day (varies with latitude and season). Vietnam spans 9°N–23°N, so the actual window varies from ~23:30 UTC (previous day) in summer to ~10:30 UTC; a hard-coded 00:00–09:30 UTC range was provably too narrow. Roughly **~1,330 days × ~21 slots/day × ~13,500 cells ≈ 3.8 × 10⁸ (cell, slot) records per product**, or **~28,000 NetCDF files per method**.

**Inference target.** For each method (B1 ST kriging, B2 RF), the product is dense within each day's discovered window: every (cell, slot) where `slot ∈ [slot_first, slot_last](D)` carries exactly one AOD value. Observed cells pass through unchanged from Stage A; missing cells are filled by the model. Night-time slots and slots outside the window are not produced.

#### 7.6.1 Shared training / validation / test protocol (both methods)

Both B1 ST kriging and B2 RF use the same temporal partition and the same intra-train validation strategy, so their §8.2 metrics are directly comparable.

| Partition | Range               | Span                  | Use                                    |
| --------- | ------------------- | --------------------- | -------------------------------------- |
| Train     | Sep 2022 – Dec 2024 | ~28 months, ~840 days | Model fitting + intra-train CV         |
| Test      | Jan 2025 – Apr 2026 | ~16 months, ~490 days | §8.2 head-to-head evaluation only      |

**Intra-train validation: 5-fold contiguous temporal CV.**

- **Fold assignment by UTC date** — all slots of a given day fall in the same fold (prevents within-day leakage). Implemented as `RF_CV_FOLDS = 5` in `stage_b/config.py` and via `temporal_block_folds(meta_dates, k)` in `rf_gapfill.py`.
- **RF use of CV** — hyperparameter grid search over `RF_GRID` (`n_estimators ∈ {100, 200, 500}`, `max_depth ∈ {15, 20, 25, None}`, `min_samples_leaf ∈ {1, 5, 10}`, `max_features ∈ {sqrt, 0.5, 1.0}`). Selection criterion is **mean CV-R²** across the 5 folds. The final RF is retrained on the full train partition (after a temporal 80/20 internal-test split also taken by date).
- **ST kriging use of CV** — variogram parameters are fit on a stratified sub-sample of the train partition (§7.7). CV serves as a sanity check that fitted ranges/anisotropy don't drift across folds.

**⚠ Do not use OOB-R² for RF.** `RF_OOB_SCORE = False` is enforced in `config.py` and passed explicitly to every `RandomForestRegressor`. OOB sampling is IID bootstrap; AOD has strong temporal autocorrelation; OOB-R² would be systematically optimistic. This departs from Youn 2024's reporting convention but is principled given AOD's autocorrelation structure.

**Inference.** Both methods produce predictions for every (cell, slot, day) inside each day's observation window across Sep 2022 – Apr 2026. Train-partition predictions are produced too (for §8.2 consistency checks and uncertainty-field calibration). The §8 headline metrics are computed on the **test** partition only.

### 7.7 Step B1 — Spatiotemporal kriging (Yang & Hu 2018 sum-metric)

#### 7.7.1 Why ST kriging, not per-slot spatial kriging

At 30-min cadence many monsoon slots have zero in-domain valid observations. Yang & Hu (2018, *Sci. Total Environ.* 633, 677–683) show explicitly that "days with no valid AOD data cannot be interpolated by spatial kriging, while ST kriging may work because it can borrow data from adjacent days." That single observation forces the move from per-slot spatial kriging to spatiotemporal kriging at 30-min cadence in Vietnam.

#### 7.7.2 Sum-metric variogram

Yang & Hu compared four ST families (metric, product, separable, sum-metric) and selected sum-metric on minimum variogram-fit MSE. We adopt sum-metric directly:

```
γ_ST(h_s, h_t) = γ_S(h_s) + γ_T(h_t) + γ_J(√(h_s² + (k · h_t)²))
```

with spatial, temporal, and joint sub-components, plus an anisotropy parameter `k` (km/hour) that converts time to an equivalent spatial distance. Sub-component families are configurable in `B1_VARIOGRAM_SPEC`; the current production setting is:

| Component | Family | Initial guess |
|-----------|--------|---------------|
| Spatial   | `Exponential` | var = 0.05, len_scale = 60 km, nugget = 0 |
| Temporal  | `Gaussian`    | var = 0.05, len_scale = 6 h, nugget = 0   |
| Joint     | `Gaussian`    | var = 0.10, len_scale = 1 000 km, nugget = 0 |
| `k`       | —             | 0.6 km/hour                               |

The library is `gstools`; the families are chosen for tractable joint-norm computation under the gstools API. They are not the only sum-metric specification Yang & Hu validated, but they are families gstools handles cleanly and the empirical fit then sets the actual ranges. The fitted parameters are persisted to `MODELS_DIR/st_variogram.json`.

#### 7.7.3 Variogram fitting

Fit once on the training partition (Sep 2022 – Dec 2024). The implementation (`kriging.py::fit_variogram`):

1. Sub-samples `B1_VARIOGRAM_SUBSAMPLE = 100 000` pair-distances from observed (cell, slot) pairs in the train partition.
2. Builds an empirical 2-D ST variogram by binning on space (`B1_W_SPACE_KM × 1.5` km maximum lag, 12 bins) and time (`B1_W_TIME_H × 1.5` hours maximum lag, 12 bins) — only daytime-to-daytime pairs participate (no daytime-to-nighttime pairs exist because Stage A only writes daytime slots).
3. Fits the sum-metric model by `scipy.optimize.least_squares` against the populated bins, with positive-side bounds preventing parameter explosion.

Time is stored as a real UTC timestamp (not a slot ordinal), so kriging weights correctly discount cross-night neighbours relative to within-day neighbours.

#### 7.7.4 Inference window

A full ST kriging system over (~13 500 cells × ~1 330 days × ~21 slots) is computationally infeasible — the covariance matrix would have ~10¹⁴ entries. Inference uses a moving window per target (cell, slot):

| Knob | Live value | Rationale |
|------|------------|-----------|
| `B1_W_SPACE_KM`        | 100 km   | ~2× the expected spatial range; large enough that fitted variogram down-weighting (not window clipping) determines the influence radius |
| `B1_W_TIME_H`          | 12 h     | Covers same-day daytime neighbours; **does not bridge the full nighttime gap** between two adjacent days (~13–15 h). This is a deliberate trade-off — see §14 drift register |
| `B1_MAX_NEIGHBOURS`    | 40       | Per-target neighbour cap to bound per-prediction wall time |
| `B1_MIN_NEIGHBOURS`    | 8        | Below this, the cell falls back to climatology/local mean |

The fix-doc target was `W_t = 24 h` and `MAX_NEIGHBOURS ≈ 150`, intended to ensure every target slot sees both the previous day's afternoon and the next day's morning. The live implementation uses 12 h and 40 neighbours; the design rationale and the empirical sensitivity test that justifies the choice are documented in §14. If wet-season case-study coherence (§8.2.5) shows discontinuities at day boundaries, `B1_W_TIME_H` is the first knob to widen.

#### 7.7.5 Inference loop and edge handling

For each UTC day `D` in the run window:

1. Determine `[slot_first, slot_last]` from Stage A filenames (`discover_day_window`).
2. For each (cell, slot, D) with `slot ∈ [slot_first, slot_last]`:
   - If Stage A has an observed value at this cell, pass it through unchanged (`is_observed = True`).
   - Otherwise, query the spatiotemporal KD-tree (`scipy.spatial.cKDTree` built from the day's surrounding window of observations), assemble the local neighbour set under (`B1_W_SPACE_KM`, `B1_W_TIME_H`), solve the kriging system, write the predicted value plus its variance.
3. Cold-start cells (insufficient neighbours, dataset edges) fall back to the same-slot-of-day same-month mean over the training partition.

ST kriging variance is shipped as the `uncertainty` field (AOD² units; the `uncertainty_units` global attribute documents this).

#### 7.7.6 Coverage expectations

Yang & Hu reported 67.73% mean pixel-level completeness from ST kriging alone (Beijing, daily). At 30-min cadence in Vietnam, **40–60% slot-level completeness from B1 alone** is the working prior, with the remaining gaps closed by B2 (RF). Re-evaluated after the first end-to-end run via the §8.2.3 coverage audit.

### 7.8 Step B2 — Random Forest gap-fill per 30-min slot (Youn 2024 / Chen 2023 SIM, 30-min)

#### 7.8.1 Predictor vector (19 features)

The feature list is fixed in `stage_b/config.py::RF_FEATURES`. All dynamic predictors are resampled to **0.05° × 30-min** before training; ERA5 (hourly) and CAMS (3-hourly) are linearly interpolated in time to the slot centre; IMERG (native 30-min) is bilinearly resampled in space only.

| #   | Predictor     | Source                   | Native res.       | Pre-processed to                                  |
| --- | ------------- | ------------------------ | ----------------- | ------------------------------------------------- |
| 1   | `cams_aod`    | CAMS global forecasts    | 0.4° / 3-hourly   | 0.05° / 30-min (bilinear space + linear time)     |
| 2   | `t2m`         | ERA5 T2m                 | 0.25° / hourly    | 0.05° / 30-min                                    |
| 3   | `dpt`         | ERA5 Td2m                | 0.25° / hourly    | 0.05° / 30-min                                    |
| 4   | `rh`          | ERA5 RH                  | 0.25° / hourly    | 0.05° / 30-min                                    |
| 5   | `sp`          | ERA5 surface pressure    | 0.25° / hourly    | 0.05° / 30-min                                    |
| 6   | `u10`         | ERA5 U10                 | 0.25° / hourly    | 0.05° / 30-min                                    |
| 7   | `v10`         | ERA5 V10                 | 0.25° / hourly    | 0.05° / 30-min                                    |
| 8   | `blh`         | ERA5 PBLH                | 0.25° / hourly    | 0.05° / 30-min                                    |
| 9   | `tcc`         | ERA5 total cloud cover   | 0.25° / hourly    | 0.05° / 30-min                                    |
| 10  | `tcwv`        | ERA5 total column water  | 0.25° / hourly    | 0.05° / 30-min                                    |
| 11  | `ssrd`        | ERA5 surface downward SR | 0.25° / hourly    | 0.05° / 30-min                                    |
| 12  | `fal`         | ERA5 forecast albedo     | 0.25° / hourly    | 0.05° / 30-min                                    |
| 13  | `tp`          | GPM IMERG precip         | 0.1° / 30-min     | 0.05° / 30-min (bilinear space only; native time) |
| 14  | `elevation`   | Copernicus GLO-30 DEM    | 30 m → 0.05° mean | static                                            |
| 15  | `land_cover`  | MCD12Q1                  | annual            | static (refreshed by year)                        |
| 16  | `population`  | LandScan                 | annual            | static (refreshed by year)                        |
| 17  | `lat_rad`     | grid lat (radians)       | —                 | static                                             |
| 18  | `lon_rad`     | grid lon (radians)       | —                 | static                                             |
| 19  | `ndvi`        | MOD13Q1 16-day composite | 0.05°             | nearest-on-or-before in time (quasi-static)        |

**Total: 13 dynamic + 5 static + 1 quasi-static = 19 predictors.**

**Reconciliation with Youn 2024's 12-var set:**

- ✔ Kept: CAMS, T2m, Td2m, RH, U10, V10, PBLH, surface solar, surface pressure.
- ⚠ Substituted: Youn used `HCDC` (high cloud cover); we use ERA5 **`CloudCover` (total)**. Total is broader; Youn's HCDC was the lowest-importance variable at 2.55% relative importance.
- ✘ Missing: `LHFL` (surface latent heat flux). Not in the available ERA5 collection; the dominant signal (boundary-layer stability) is partially carried by PBLH and Td2m.
- ➕ Added: `tcwv` (TCWV, hygroscopic-growth proxy), `fal` (albedo, surface-reflectance bias — Chen 2023 used it).

**MERRA-2 deliberately excluded** as a Stage B predictor. MERRA-2 already underlies the Stage A bias correction (§7.4.1); using it again in Stage B would double-dip the same reanalysis signal and risk circular validation. CAMS is the sole reanalysis-AOD predictor (different model core, different aerosol species, separately assimilated chemistry).

#### 7.8.2 Training-data construction

Training pairs (X, y) come from the Stage A 30-min merged product on the train partition (Sep 2022 – Dec 2024), daytime slots only (per the per-day window discovery of §7.6.0). For each (cell, slot, day) where Stage A `AOD_merged` is finite and ≥ 0, `y = AOD_merged` and `X` is the 19-feature vector at that cell and slot.

**Stratified subsampling.** A full pass would produce ~2.4 × 10⁷ rows. To keep RF tuning tractable, `build_training_table` subsamples to `RF_TRAIN_TARGET_ROWS = 2 000 000` rows, stratified by `(month, slot_idx)`, with a per-stratum floor of `max(50, target / (4 × n_strata))`. The stratified design preserves coverage of every (month × slot) combination — important because monsoon-month coverage is sparse and would otherwise be under-represented under uniform random sampling.

Median imputation is applied at training time to features that occasionally lack values (e.g., NDVI on no-composite cells). Imputation medians are computed on the train fold and persisted in the saved bundle.

#### 7.8.3 Hyperparameter selection

Selection criterion is **mean 5-fold temporal-CV R²** (§7.6.1), as enforced in `rf_gapfill.py::tune_rf`. Initial defaults (used if no tuning is run):

```
RF_N_ESTIMATORS     = 100
RF_MAX_DEPTH        = 20
RF_MIN_SAMPLES_LEAF = 5
RF_N_JOBS           = -1
RF_RANDOM_STATE     = 42
RF_OOB_SCORE        = False  # never use OOB-R²
```

The tuning loop persists every grid point's `cv_r2_mean` and `cv_r2_std` to `MODELS_DIR/<name>_tune_results.csv`. The selected hyperparameters and the final bundle (model, feature column order, median-impute vector, training-window metadata, all metrics) are saved with `joblib` to `MODELS_DIR/<name>.joblib`.

#### 7.8.4 Internal-consistency check

After training, the bundle reports `rmse_train`, `rmse_cv_mean`, `rmse_cv_std`, `rmse_internal_test`, and per-fold metrics. The consistency check `consistency_check(metrics, tol=RMSE_CONSISTENCY_TOLERANCE = 0.15)` requires `rmse_train` and `rmse_internal_test` to lie within ±15% of `rmse_cv_mean`. Failure indicates either over-fitting (test ≫ CV) or distribution mismatch (train ≪ CV); either signals a re-look at the predictor pool or the subsample stratification before §8.

#### 7.8.5 Inference loop

`fill_range(start, end, bundle)`:

1. For each UTC day `D` in `[start, end]`, iterate slots in `iter_window_slots(D)` (i.e., within the discovered window).
2. For each slot: load Stage A merged grid (if present), build the feature grid via `build_feature_grid(slot_utc)`, predict with `predict_with_tree_sd` for the per-tree uncertainty, and write the NC file.
3. Observed cells pass through (`is_observed = True`, RF prediction overridden by the Stage A value, `uncertainty = NaN`). Missing cells get the RF prediction and the per-tree-SD uncertainty.

**Per-tree SD as uncertainty.** `RFBundle.predict_with_tree_sd` stacks `tree.predict(X)` over `estimators_` and returns mean + std. This is **not OOB SE** (which would be optimistic under temporal autocorrelation per §7.6.1). Documented in the output NetCDF as `uncertainty_units = "AOD (per-tree SD across the RF ensemble)"`.

#### 7.8.6 Expected performance

Youn 2024 reported cell-blind RMSE 0.064 and AERONET-blind RMSE 0.208 on Korea at hourly cadence. The thesis sets no a-priori bound on Vietnam performance; the first §8.2.1 CV pass establishes the empirical baseline. Comparison vs Youn is reported in §8.2.6.

### 7.9 Output schema (both Stage B products)

Two parallel product trees:

```
STAGE_B_DIR/output/st_kriging/YYYY/MM/DD/aod_YYYYMMDD_HHMM.nc
STAGE_B_DIR/output/rf/YYYY/MM/DD/aod_YYYYMMDD_HHMM.nc
```

~28,000 NC files per method (data-driven slot count per day). Each file is small (310 × 160 = 49 600 cells × 4 variables ≈ tens of KB compressed).

**Per-file contents (single 30-min slot, identical schema both methods):**

| Variable             | Dims       | Meaning                                                                                                  |
| -------------------- | ---------- | -------------------------------------------------------------------------------------------------------- |
| `aod_550nm`          | (lat, lon) | Filled AOD value (Stage A observation if available, else model prediction)                              |
| `is_observed`        | (lat, lon) | Boolean: true = passed through from Stage A; false = model-filled                                         |
| `uncertainty`        | (lat, lon) | ST kriging variance for `st_kriging/`; RF per-tree SD for `rf/`. NaN on observed cells                   |
| `stage_a_weight_sum` | (lat, lon) | Stage A ICW weight sum, only on observed cells (NaN elsewhere) — provenance carried forward from Stage A |

**Coordinates:** `lat`, `lon` (Vietnam 0.05° grid). Time is implicit in the filename and stored as a scalar global attribute.

**Global attributes per file:**

```
slot_utc          = "2022-09-01T00:00:00Z"
slot_index        = 0          # 0..47 within the day
method            = "st_kriging" | "rf"
method_version    = "v1.0"
training_window   = "2022-09-01 to 2024-12-31"
created_at        = ISO timestamp
uncertainty_units = "AOD^2 (kriging variance)" | "AOD (per-tree SD across the RF ensemble)"
```

**Daily / monthly aggregations for the §8.2.6 RQ4 PM2.5 comparison** are computed as a post-processing step from each method's per-slot NC files independently and written to `STAGE_B_DIR/output_aggregated/{st_kriging,rf}_daily/`. The roll-up is *not* a Stage B step.

---

## 8. Validation Strategy

§8.1 (Stage A) and §8.2 (Stage B) follow the structure of the predecessor literature: Ahn 2021 / Gupta 2024 for the merged product, Chen 2023 / Youn 2024 / Yang & Hu 2018 for the gap-fill. The shared protocol below pins what every block uses.

### 8.0 Shared validation protocol

**Temporal split.** Train Sep 2022 – Dec 2024, held-out Jan 2025 – Apr 2026. All §7.4 calibration and §7.7/§7.8 model fitting use only the train partition. The held-out window is never touched until §8.

**Metric panel.** Per Ahn 2021 Table 3 / Gupta 2024 Table 3 / Nguyen 2025 Tables 2–3:

- `N`, `R`, `R²`, `RMSE`, `MAE`, `Bias` (sat − AERONET), `slope`, `intercept`, `%EE` with `EE = ±(0.05 + 0.15 · AOD_AERONET)`.

Panels are computed per station, per season (Dry: Oct–Apr / Wet: May–Sep), and per confidence-flag tier (1–4 from §7.5) where applicable.

**AERONET co-location.** Spatial: the 0.05° cell containing the AERONET station, optionally extended by the §7.0.1 fallback. Temporal: AERONET observations averaged within **±30 min of the slot centre** — for both Stage A (`AOD_merged`) and Stage B (per-slot gap-filled product). AERONET is Ångström-interpolated to 550 nm before any comparison.

### 8.1 Stage A validation — is the merged 30-min product good?

Validation answers: does the merged product agree with AERONET better than any single sensor? Does it agree with itself across sensors? Does it beat the published predecessor merges?

#### 8.1.1 Held-out AERONET validation (headline test)

For each 30-min slot in the held-out window, match `AOD_merged` at the AERONET cell with AERONET observations within ±30 min. Report the full metric panel per station, per season, and per confidence flag. Pass criteria:

- Nghia Do: R ≥ 0.90, RMSE ≤ 0.30 averaged across confidence flags ≥ 2.
- Bac Lieu: R ≥ 0.85, RMSE ≤ 0.20 averaged across confidence flags ≥ 2.
- Dry-season bias inside [−0.05, +0.05] at both stations — confirms the §7.4 soft calibration removed the §5.1 baseline biases.

#### 8.1.2 Pre/post-correction comparison

For each (sensor, region, season) stratum: `RMSE_before`, `RMSE_after`, `RMSE_after_CV` from `soft_calibration.json`, plus held-out RMSE on training-window scatter. Per-stratum pass criterion: `RMSE_after ≤ RMSE_before − 0.02` or the stratum is routed to `'none'` (carrying the σ² penalty in §7.5). Held-out RMSE must lie within 1.5× CV RMSE — `> 2×` → overfit → routed to `'none'`.

#### 8.1.3 Inter-sensor consistency (internal uncertainty)

Where two or more sensors observe the same cell in the same slot, the across-sensor spread is an internal uncertainty estimate that doesn't require AERONET. Report inter-sensor R² per region for each sensor pair, before and after §7.4 soft calibration. Pass criterion: post-correction R² lifts by ≥ +0.10 in every region for at least one sensor pair. This is the only AERONET-comparable validation pathway for central Vietnam.

#### 8.1.4 Comparison against baselines

Four baselines, each validated on the held-out window with the same panel at both stations:

- **B1 — best-single-sensor.** Bias-corrected VIIRS-only.
- **B2 — Gupta 2024 equal-weight DT merge.** Same input grids, equal weights, no §7.4 calibration.
- **B3 — Ahn 2021 NE Asia composite** where its publication overlaps Vietnam.
- **B4 — Nguyen 2025 daily Himawari RANSAC.** The thesis daily product is fed into the same downstream regression structure.

Claim being tested: the thesis merged product beats all four on AERONET-validated R, RMSE, %EE at *at least one* station for *at least one* season.

#### 8.1.5 Precipitation-aware validation

Validate the merged product separately for dry intervals (> 24 h since IMERG ≥ 0.1 mm hr⁻¹), post-rain intervals (0–12 h), and recovery intervals (12–24 h). Pass criterion: R ≥ 0.85 in dry intervals at both stations. A wet-interval R that collapses below 0.50 signals that wet-season σ²_TC for Himawari is under-estimated (§10 #13).

#### 8.1.6 Case-study confirmation

Three documented Vietnam aerosol events from the held-out window:

1. Severe Hanoi dry-season haze (AOD > 1.5).
2. March–April biomass-burning transport from mainland SEA.
3. Precipitation washout (negative-control).

Qualitative comparison vs MERRA-2 + Worldview imagery; Spearman rank vs Envisoft (≥ 50% completeness in the held-out window, per §4.3 / §10 #16); HYSPLIT overlay for case 2.

---

### 8.2 Stage B validation — head-to-head between ST kriging and RF

Stage B ships two parallel products. §8.2 answers: *which method generalises better to held-out AERONET-blind slots?* and *do they agree spatially?*. The protocol comes from Chen 2023 §2.4 / §3.2, Youn 2024 §2.2.3 / §3.1, and Yang & Hu 2018 §3.

`stage_b/run_stage_b.py::run_validation` and `stage_b/validate.py` implement the workflow; the entry points named below match the function names there for traceability.

#### 8.2.1 Per-method internal cross-validation

Each method reports a CV panel on the train partition (held-out window untouched):

| Method | CV protocol | Source |
| --- | --- | --- |
| §7.7 ST kriging | Sum-metric variogram refit per fold on the other 4 folds' (cell, slot) pairs; CV-mean RMSE / R² reported; sanity on fitted variogram parameter drift across folds | Yang & Hu 2018; §7.6.1 |
| §7.8 RF | 80/20 temporal-block internal test + 5-fold contiguous temporal-block CV inside the 80%. Selection criterion is mean CV-R². OOB-R² explicitly disabled | Youn 2024 §2.2.3; §7.6.1 |

For RF, `rmse_train`, `rmse_cv_mean`, `rmse_cv_std`, `rmse_internal_test`, `r2_cv_mean`, `r2_cv_std`, `r2_internal_test` are recorded in the saved bundle's `metrics` field. `consistency_check` (±15%) must pass before a candidate moves to §8.2.2.

#### 8.2.2 AERONET-blind validation (the headline comparison)

Implemented in `validate.aeronet_pairs(test_start, test_end, candidate, blind_only=True)`.

The setup:

1. Identify slots in the held-out window where AERONET has a valid observation but the Stage A merged AOD at the AERONET cell was **missing** (cloud-occluded).
2. Run each method to fill that cell; read the filled `aod_550nm` from the candidate's product tree.
3. Compare the filled value against AERONET observations within ±30 min of the slot centre.

Report the full metric panel per method, per station, per season. The per-method RMSE on filled-only matchups should lie within ~1.3× the §8.1.1 fused-product RMSE on observed matchups — beyond that, the fill is meaningfully worse than a direct observation.

**Head-to-head:** `validate.compare_candidates({'B2_RF': pairs_rf, 'B1_ST_krig': pairs_krig})` produces a side-by-side panel; `validate.paired_skill(pairs_rf, pairs_krig)` computes paired RMSE difference and a paired t-test on errors over identical (cell, slot, day) keys. Fair because both methods predict the same set of points.

The method with the lower held-out RMSE on filled-only matchups, averaged across both stations and both seasons, becomes the **headline gap-fill recommendation** for downstream PM2.5 users. The other method is *still* shipped (parallel product trees) — Stage C downstream consumers can choose.

#### 8.2.3 Spatial coverage and provenance audit

`validate.coverage_audit(...)`: per-month coverage fraction over Vietnam, pre-fill (Stage A merged) vs post-fill (per method). Per the §9 target, the gap-filled product must reach ≥ 95% slot-level coverage **within the day's observation window**.

`validate.sso_stratified_rmse(...)`: stratify §8.2.2 RMSE by `slots_since_last_observed` (SSO) using the bins in `config.SSO_BINS = [-1, 0, 1, 4, 19, 49, ∞]` (labels: `observed`, `1 slot`, `≤ 2 h`, `≤ 10 h`, `≤ 25 h / one night`, `> 25 h`). The bin where the ML RMSE exceeds the §7.7 climatological-fallback RMSE is the empirical horizon beyond which RF is no better than climatology — reported as a hard cap on the ML recommendation.

#### 8.2.4 Model-diagnostic checks

Robustness, not pass/fail:

- **RF variable importance** (`validate.variable_importance(model_name)`): normalised Gini importance for the 19 predictors. CAMS AOD and ERA5 dew-point temperature are expected to dominate (Youn 2024 reported 27.4% / 8.66% respectively); a different ordering in Vietnam is a finding, not a failure.
- **Reanalysis-substitution sanity** (CAMS vs MERRA-2). Re-run the RF with MERRA-2 swapped for CAMS at training time. Held-out RMSE must agree within ±0.02 — agreement means the gap-fill is combining many predictors, not slaved to one reanalysis.
- **Residual envelope vs AERONET AOD.** Mean residual ± 1σ binned at 0.1 AOD intervals, per method.
- **Daytime variation against AERONET** (Chen 2023 Fig. 6 analogue). Slot-by-slot mean of gap-filled AOD vs AERONET slot-by-slot mean over the held-out window, per station — both methods on the same axes.

#### 8.2.5 Case-study stress test (cloud-occluded windows)

`validate.cloud_period_recovery(...)`: select a monsoon-season multi-day cloud window (48 consecutive missing daytime slots ≈ 2.3 days of complete daytime occlusion, drawn from July–August 2025). For each method:

- Visual coherence of consecutive maps (no day-to-day discontinuities at cloud edges; spatial structure not collapsed to flat climatology).
- Recovery check: when satellite retrievals resume, the merged value should align with the gap-fill of the prior slot within 1.5× the §8.2.2 filled-cell RMSE. A larger discontinuity means the gap-fill drifted during the occluded period.
- ST-kriging-specific: variance field inflates monotonically across the occlusion (a good sign that uncertainty propagation is honest).
- RF-specific: per-tree SD inflates where features depart far from training-distribution support.

#### 8.2.6 Success table and RQ4 PM2.5 comparison

Optional daily roll-up (`output_aggregated/{st_kriging,rf}_daily/YYYY/MM/DD.nc`) is computed as a post-processing step from each per-slot tree. The RQ4 comparison vs Nguyen 2025 R² = 0.293 is then a head-to-head: which daily-rolled product gives the better daily PM2.5 R²?

The §9 quantitative target table is reproduced per method, with achieved values filled in. Following Nguyen 2025, daily AOD–PM2.5 R² is reported with both OLS and RANSAC robust regression (residual threshold 1.5 × MAD, 50% minimum sample, 1 000 trials). The merged AOD product is *not* RANSAC-filtered itself — that would discard real high-AOD events. OLS-vs-RANSAC lift is a downstream diagnostic, not a product step.

---

## 9. Expected Outcomes and Deliverables

1. **Vietnam 30-min merged AOD dataset** at 0.05°, Sep 2022 – Apr 2026, NetCDF, with provenance flags. Archived publicly.
2. **Two Vietnam 30-min gap-filled AOD datasets** at 0.05° — `output/st_kriging/` (Yang & Hu 2018 sum-metric ST kriging) and `output/rf/` (Youn 2024 / Chen 2023 SIM-style RF at 30-min cadence), with `is_observed`, per-method `uncertainty`, and `stage_a_weight_sum` provenance.
3. **Bias-correction lookup tables** (linear soft-calibration `(α, β)` per sensor × region × season × product-level, plus the triple-collocation σ² table), distributed as supplementary data.
4. **Methodological comparison report** of the two gap-filling strategies on the same domain — the first such head-to-head comparison for tropical Southeast Asia.
5. **Validation against Gupta 2024, Ahn 2021, and best-single-sensor baselines.**
6. **Recommendation document** for the downstream PM2.5 mapping project.

**Quantitative targets:**

| Metric                                                    | Baseline (best individual sensor / Gupta 2024) | Thesis target                                                                |
| --------------------------------------------------------- | ---------------------------------------------- | ---------------------------------------------------------------------------- |
| AERONET R at Nghia Do                                     | 0.915 (VIIRS only)                             | ≥ 0.90 with full slot coverage on the AERONET cell                            |
| AERONET R at Bac Lieu                                     | 0.845 (VIIRS only)                             | ≥ 0.85 with full slot coverage on the AERONET cell                            |
| AERONET RMSE Nghia Do                                     | 0.271 (VIIRS only)                             | ≤ 0.30 averaged across confidence flags                                        |
| Slot-level coverage over Vietnam (gap-filled product)     | 10.3% (Himawari L2 raw, hourly proxy)          | ≥ 95% slot-level coverage within each day's observation window               |
| Daily AOD–PM2.5 R² (after post-processing daily roll-up)  | 0.293 (Himawari RANSAC, Nguyen 2025)           | ≥ 0.35 (either method)                                                        |
| Daily AOD–PM2.5 R² (MODIS-equivalent ceiling)             | 0.573 (MODIS RANSAC)                           | — (ceiling reference)                                                         |

---

## 10. Limitations and Caveats

1. **AOD availability is fundamentally ~10% before gap-filling.** Gap-filled values dominate ~90% of the 30-min product (and ~50–70% of any daily rollup, depending on the slot-count floor the downstream consumer applies) and must be flagged distinctly throughout the dataset.
2. **Two AERONET sites only.** Central Vietnam has no in-situ ground truth. Bias correction for central cells is provided by the MERRA-2 anchor (§7.4.1); only the inter-sensor consistency check of §8.1.3 and the case studies of §8.1.6 / §8.2.5 validate central performance.
3. **Himawari high VZA over northern Vietnam** elongates the atmospheric path; bias correction reduces but does not eliminate this geometric limitation.
4. **MAIAC fails at Bac Lieu** (R = 0.411). The south is essentially served by Himawari + VIIRS; MAIAC's σ²_TC there is large and its automatic weight small.
5. **Wet season in the north is unconstrained by LEO** (May–Sep). Compounds with caveat 13.
6. **Spatially uniform hygroscopic exponent γ = 0.6.** Per-region γ tuning is future work.
7. **PBLH unavailable at hourly resolution for ~25% of the study period**; replaced by monthly climatology over Jan–Jun 2024.
8. **Gap-filled values are model estimates.** Flagged via `is_observed` in every Stage B NC file; should not be used for trend analysis or extreme-value statistics without an explicit `is_observed == True` filter.
9. **ML gap-filling generalises only as far as training data.** Once-per-decade regimes absent from training are extrapolations.
10. **Spectral harmonisation dropped.** Bounded at ~ΔR² of 0.003 and ΔRMSE of 0.001 (Nguyen 2025).
11. **MAIAC bitmask filtering** rejects "Poor"-and-worse pixels (bits 8–11 ≤ 4), trading coverage for accuracy.
12. **MERRA-2 structural smoothness.** Its native 0.5° × 0.625° resolution means the soft calibration learns the seasonal-regional mean shift, not fine-scale spatial structure. The inter-sensor consistency check (§8.1.3) is the only diagnostic that catches this for central Vietnam.
13. **Wet-season Himawari is weighted automatically by σ²_TC; no hand-set down-weight applies.** The §8.1.1 wet/dry R ratio and the §8.1.5 precipitation-aware metrics are the only safety net.
14. **Physics correction uses ERA5 RH/PBLH only.** In-situ surface RH is not yet incorporated.
15. **Soft-calibration guard rails can route whole strata to `'none'`.** Those strata get a 2× σ² penalty (weight ↓ ~4×) at fusion.
16. **§8.1.6 case studies use a relaxed Envisoft completeness threshold (≥50%) over the held-out window.**
17. **Himawari L2/L3 merged per-pixel before fusion.** No per-pixel L2-vs-L3 provenance flag in the merged product.
18. **VIIRS overpasses near a slot boundary contribute to two consecutive slots.** Mild positive temporal autocorrelation between adjacent slots near overpass time.
19. **MAIAC no-orbit-timestamp days are dropped** (< 1% of MAIAC days).
20. **ST-kriging temporal window (`B1_W_TIME_H = 12 h`) does not bridge the full nighttime gap.** Slots near the day-window edges may see only same-day neighbours rather than borrowing across the night. See §14 for the design rationale and the test plan that decides whether to widen.
21. **RF training data is stratified-subsampled to 2 × 10⁶ rows** out of ~2 × 10⁷ available — the sub-sample is randomised within `(month, slot_idx)` strata, so total coverage is preserved but the effective row count per stratum is bounded. Re-tuning at full coverage is a stretch / future work; see §14.
22. **Per-slot RF has no inter-slot temporal constraint.** If §8.2.5 reveals visible slot-to-slot noise in filled time series, a 3-slot rolling-mean smoother on the RF output is a v2 refinement.
23. **ST-kriging warm-up.** Slots in the first ~24 h of Sep 2022 and the last ~24 h of Apr 2026 lack one side of the temporal window; flagged as warm-up / warm-down in any analysis.

---

## 11. Scope Management — In / Out / Stretch

**In scope (must complete):**

- Steps A1–A5 (full Stage A: filtering, gridding, physics correction, bias correction, fusion).
- Step B1 (ST kriging, parallel product, also serves as ML evaluation baseline).
- Step B2 (RF gap-fill per 30-min slot, primary ML candidate, parallel product).
- §8.0, §8.1, §8.2 validation including the head-to-head ST kriging vs RF comparison.
- §8.1.4 comparison vs Gupta 2024, best-single-sensor, and Nguyen 2025.

**Stretch (do if time permits):**

- §8.1.4 comparison vs Ahn 2021 (geographical overlap is thin).
- §8.1.6 PM2.5 case studies (3 of 4).
- RF re-tuning at full row coverage (drop the §10 #21 subsampling).
- ST kriging `B1_W_TIME_H` sensitivity sweep at 12 h / 18 h / 24 h.
- 3-slot rolling-mean smoother on RF output if §8.2.5 indicates need.

**Stretch / future work:**

- Per-region γ tuning.
- Multi-year extension as more AERONET data arrives.
- AERONET station near Đà Nẵng / Huế (external follow-up).

If the timeline shrinks, Step B2 (RF) is the first thing to drop — Step B1 (ST kriging) alone produces a usable 30-min product (Yang & Hu reported 67% completeness from ST kriging alone at daily cadence; expect 40–60% slot-level completeness at our 30-min cadence).

---

## 12. Proposed Timeline

| Phase                                    | Tasks                                                                | Duration |
| ---------------------------------------- | -------------------------------------------------------------------- | -------- |
| 1. Data preparation                      | Download & organise L2 products; AERONET re-processing               | 4 weeks  |
| 2. Baseline validation extension         | Reproduce/extend per-sensor validation; add SNPP VIIRS               | 3 weeks  |
| 3. Gridding pipeline (A1+A2)             | 0.05° box-averaging code for all sensors, persistent intermediate    | 3 weeks  |
| 4. Bias correction (A4)                  | Soft calibration vs MERRA-2 + TC σ² table                            | 5 weeks  |
| 5. Fusion (A5) + physics (A3)            | TC-weighted merge; produce 30-min files for full period               | 3 weeks  |
| 6a. ST kriging (B1)                      | Variogram fit, gstools moving-window inference, parallel product     | 3 weeks  |
| 6b. RF gap-fill (B2)                     | Feature pipeline (ancillary loaders), tuning, fill, parallel product | 4 weeks  |
| 7. Validation                            | Held-out AERONET, head-to-head, SSO bins, case studies               | 4 weeks  |
| 8. Writing                               | Thesis document                                                       | 6 weeks  |

Total: ~35 weeks. ~32 weeks without §8.1.4 Ahn 2021 comparison.

---

## 13. Connection to Downstream PM2.5 Project

This thesis produces the AOD input layer. The PM2.5 project will:

- Use 27 Envisoft PM2.5 monitoring stations (10/8/9 north/central/south) as PM2.5 ground truth.
- Apply physics-based correction (RH, PBLH) plus ML (LightGBM / RF / DL) on top of the thesis's AOD layer.
- Use ERA5 meteorology + GPM IMERG precipitation as covariates.
- Deliver near-real-time PM2.5 maps by feeding live Himawari through the same bias-correction + gap-filling pipeline.

**Performance ceiling** (from Nguyen 2025, daily RANSAC):

| Input                  | Daily R² | Notes                          |
| ---------------------- | -------- | ------------------------------ |
| Raw Himawari AOD       | 0.028    | Negligible                     |
| Corrected Himawari AOD | 0.293    | 10× improvement over raw       |
| Corrected MODIS AOD    | 0.573    | Benefits from midday BL mixing |

The merged product is expected to lie between the Himawari-only and MODIS-only daily numbers. The near-real-time use case specifically depends on Himawari's 10-min cycle, so the Himawari bias correction in §7.4 is the *critical enabling component* of the entire downstream operational system.

---

## 14. Implementation drift register (v3.5.0)

Places where the live code in `stage_b/` deliberately diverges from a literal reading of `stage_b_fixes.md`. Each entry quotes the live value and the design rationale. These are configuration choices, not architectural disagreements.

| # | Plan / fix-doc target | Live value (`stage_b/config.py`) | Rationale and re-tune plan |
|---|----------------------|----------------------------------|-----------------------------|
| 1 | ST kriging temporal window `W_t = 24 h` (to bridge the nighttime gap and let every target see both flanking days) | `B1_W_TIME_H = 12.0` h | A 24-h window combined with `B1_MAX_NEIGHBOURS` ~150 would multiply per-target kriging cost ~3.5×. 12 h with 40 neighbours runs each (cell, slot) in well under 100 ms, which is what made the full ~28 000-file run tractable for v1. The trade-off: targets near the start or end of a day's observation window can't borrow from the previous-or-next day. This is acceptable in v1 because the same-day spatial structure carries most of the predictive power inside a single observation window. **Re-tune plan:** §8.2.5's case-study cloud-recovery test is the trigger — if discontinuities at day boundaries appear, widen `B1_W_TIME_H` to 18 h first, then 24 h, with `B1_MAX_NEIGHBOURS` adjusted to hold wall time within budget. |
| 2 | `B1_MAX_NEIGHBOURS ≈ 150` (Yang & Hu 2018 used ~150 neighbours per target) | `B1_MAX_NEIGHBOURS = 40` | Wall-time-bound at the chosen `B1_W_TIME_H`. Beijing's 1 km × daily grid had a much denser neighbour cloud than Vietnam's 0.05° × 30-min grid; 40 neighbours is empirically adequate for the inversion to be well-conditioned with the live variogram fit. Same re-tune trigger as #1. |
| 3 | ST kriging sub-component families: Yang & Hu's exact sum-metric (their Eq. 5) | `B1_VARIOGRAM_SPEC = {spatial: Exponential, temporal: Gaussian, joint: Gaussian}` | The gstools API's tractable families are picked for clean joint-norm computation under `gs.CovModel`. The empirical fit then sets the actual ranges; the family choice is a structural prior, not a fitted parameter. If the §8.2.1 cross-fold parameter drift is large, the families are re-evaluated against gstools `Spherical` and `Matern`. |
| 4 | RF training rows: every valid (cell, slot, day) ≈ 2.4 × 10⁷ rows | `RF_TRAIN_TARGET_ROWS = 2 × 10⁶` rows, stratified by `(month, slot_idx)` with per-stratum floor `max(50, target / (4 × n_strata))` | A full pass triples the sklearn fit time per hyperparameter point, making the §7.8.3 grid search infeasible in the §12 timeline. The stratified sub-sample preserves coverage of every (month × slot) combination — important for monsoon-month under-representation under uniform random sampling. **Re-tune plan:** the §11 stretch list includes a full-coverage retraining pass once the head-to-head method choice is settled in §8.2.2. |
| 5 | Stage B AERONET temporal match window | ±30 min slot centre, identical to Stage A (see §8.0) | Match. No drift. |
| 6 | Per-method uncertainty source | ST kriging variance (AOD² units); RF per-tree SD across `estimators_` (AOD units, *not* OOB SE) | Match. Per-file `uncertainty_units` global attr documents the distinction. |
| 7 | `RF_OOB_SCORE` | `False` (enforced) | Match. Documented in §7.6.1 / §7.8.3. |

Stage A in-code constants reconcile cleanly with the v3.4.0 prose: `SOFT_CAL_MIN_PAIRS = 100`, `SOFT_CAL_ALPHA ∈ [0.5, 2.0]`, `|SOFT_CAL_BETA| ≤ 0.2`, `NONE_PENALTY_FACTOR = 2.0`, `TC_MIN_TRIPLETS = 50`, `BOX_STD_ABS_FLOOR = 0.05` — all match the §7.4.1 / §7.4.2 / §7.0.1 prose verbatim. The collocation `BOX_STD_SLOPE` table per sensor (`0.20` for AHI/VIIRS, `0.10` for MAIAC) is recorded at collocation time but production fusion does not enforce it, as §7.0.1 documents.

---

## References

- Ahn, S., Chung, S.-R., Oh, H.-J., Chung, C.-Y. (2021). Composite Aerosol Optical Depth Mapping over Northeast Asia from GEO-LEO Satellite Observations. *Remote Sensing*, 13, 1096. **[Primary methodological predecessor — CDF + IDW + ICW fusion]**
- Buchard, V., Randles, C. A., da Silva, A. M., Darmenov, A., Colarco, P. R., Govindaraju, R., et al. (2017). The MERRA-2 aerosol reanalysis, 1980 onward. Part II: Evaluation and case studies. *Journal of Climate*, 30, 6851–6872.
- Chen, A., Yang, J., He, Y., Yuan, Q., Li, Z., Zhu, L. (2023). High spatiotemporal resolution estimation of AOD from Himawari-8 using an ensemble machine learning gap-filling method. *Science of the Total Environment*, 857, 159673. **[RF SIM template — TIM upsampler is not applicable to this 30-min pipeline]**
- Ding, Y., Li, S., Xing, J., Yang, J., Dong, J., Hu, S., Teng, M., Ni, W., & Jiang, J. (2025). Global hourly seamless AOD through measurement-adjusted machine learning fusion of multi-satellite and reanalysis data. *GIScience & Remote Sensing*, 62(1), 2586203. **[Soft-calibration linear form against MERRA-2; §7.4.1]**
- Gruber, A., Su, C.-H., Zwieback, S., Crow, W., Dorigo, W., & Wagner, W. (2016). Recent advances in (soil moisture) triple collocation analysis. *International Journal of Applied Earth Observation and Geoinformation*, 45(B), 200–211.
- Gupta, P. et al. (2024). Increasing aerosol optical depth spatial and temporal availability by merging datasets from geostationary and sun-synchronous satellites. *Atmospheric Measurement Techniques*, 17, 5455–5476. **[Global LEO-GEO DT merged product; the Vietnam-coarse benchmark to beat]**
- Holben, B. N. et al. (1998). AERONET — A federated instrument network and data archive for aerosol characterization. *Remote Sensing of Environment*, 66, 1–16.
- Inness, A., Ades, M., Agustí-Panareda, A., Barré, J., Benedictow, A., Blechschmidt, A.-M., et al. (2019). The CAMS reanalysis of atmospheric composition. *Atmospheric Chemistry and Physics*, 19(6), 3515–3556.
- Kotchenruther, R. A. & Hobbs, P. V. (1998). Humidification factors of aerosols from biomass burning in Brazil. *Journal of Geophysical Research*, 103, 32081–32089.
- Levy, R. C., Mattoo, S., Munchak, L. A., Remer, L. A., Sayer, A. M., Patadia, F., & Hsu, N. C. (2013). The Collection 6 MODIS aerosol products over land and ocean. *Atmospheric Measurement Techniques*, 6, 2989–3034.
- Lyapustin, A. et al. (2018). MODIS Collection 6 MAIAC algorithm. *Atmospheric Measurement Techniques*, 11, 5741–5765.
- McColl, K. A., Vogelzang, J., Konings, A. G., Entekhabi, D., Piles, M., & Stoffelen, A. (2014). Extended triple collocation: Estimating errors and correlation coefficients with respect to an unknown target. *Geophysical Research Letters*, 41(17), 6229–6236.
- Nguyen, K. T., Trinh, A. H., Bui, C. K. (2025). Assessing the Feasibility of Estimating Air Quality in Vietnam Using Satellite Data. *Student Scientific Research Conference, Hanoi University of Science and Technology*. **[Team paper — empirical anchor for this thesis]**
- Randles, C. A., da Silva, A. M., Buchard, V., Colarco, P. R., Darmenov, A., Govindaraju, R., et al. (2017). The MERRA-2 aerosol reanalysis, 1980 onward. Part I: System description and data assimilation evaluation. *Journal of Climate*, 30, 6823–6850.
- Remer, L. A. et al. (2012). Retrieving aerosol in a cloudy environment. *Atmospheric Measurement Techniques*, 5, 1823–1840.
- Sayer, A. M., Hsu, N. C., Lee, J., Kim, W. V., & Dutcher, S. T. (2020). Validation, stability, and consistency of the VIIRS Deep Blue aerosol data record from 2012–2019. *Journal of Geophysical Research: Atmospheres*, 125, e2019JD031781. **[VIIRS Deep Blue reference]**
- Stoffelen, A. (1998). Toward the true near-surface wind speed: Error modeling and calibration using triple collocation. *Journal of Geophysical Research: Oceans*, 103(C4), 7755–7766. **[Triple-collocation foundation paper]**
- van Donkelaar, A. et al. (2010). Global estimates of ambient fine particulate matter concentrations from satellite-based aerosol optical depth. *Environmental Health Perspectives*, 118, 847–855.
- Yang, Q., & Hu, T. (2018). Spatiotemporal kriging for gap filling of MODIS aerosol optical depth: A case study over Beijing, China. *Science of the Total Environment*, 633, 677–683. **[Stage B Step B1 — sum-metric ST kriging template]**
- Yoshida, M. et al. (2018). Common retrieval of aerosol properties for imaging satellite sensors. *Journal of the Meteorological Society of Japan*, 96B, 193–209. **[Himawari/AHI retrieval algorithm]**
- Youn, Y., Kim, S., Kim, S. H., Lee, Y. (2024). Spatial Gap-Filling of Himawari-8 Hourly AOD Products Using Machine Learning with Model-Based AOD and Meteorological Data: A Focus on the Korean Peninsula. *Remote Sensing*, 16, 4400. **[Stage B Step B2 — pure-RF gap-fill reference]**

---

*__Draft v3.5.0 — Stage B implementation reconciliation: parallel ST kriging + RF products at 30-min cadence, drift register added.__ Stage A architecture is unchanged from v3.4.0 (MERRA-2-anchored bias correction + AERONET-independent TC fusion weights). Key changes vs v3.4.0:*

*__(P1) Stage B daily aggregation deleted.__ Old §7.6 (daily collapse) is gone. Stage B runs at 30-min cadence end-to-end inside each day's data-driven observation window (§7.6.0). Daily roll-ups, where needed for the §8.2.6 RQ4 PM2.5 comparison, are performed downstream as post-processing.*

*__(P2) Stage B is now two parallel product trees.__ `output/st_kriging/` (Yang & Hu 2018 sum-metric ST kriging, §7.7) and `output/rf/` (Youn 2024 / Chen 2023 SIM-style RF at 30-min, §7.8) ship side-by-side, both dense within each day's window, both with the same 4-variable per-file schema. The §8.2 head-to-head comparison decides which is the downstream PM2.5 recommendation; both are archived.*

*__(P3) Chen 2023 TIM and the v3.4 RF + DNN "Candidate 2" are dropped.__ TIM is a temporal upsampler (hourly → 10-min), not a temporal gap-filler; the per-slot RF already provides full spatiotemporal coverage as a side effect of running at every slot. The §11 scope list, §8.2 validation block, and §7.8.x sections are all simplified accordingly.*

*__(P4) Output schema rewritten.__ The v3.4 `gap_fill_method` enum and the co-shipped `aod_kriging_baseline` row are replaced by the two-tree layout described in §7.9. Each tree carries `aod_550nm`, `is_observed`, `uncertainty`, `stage_a_weight_sum` — uniform across methods.*

*__(P5) Predictor list rebuilt against the project's actual ERA5 collection.__ 19 predictors total (§7.8.1): CAMS + 11 ERA5 fields (T2m, Td2m, RH, SP, U10, V10, PBLH, total cloud cover, TCWV, surface solar, albedo) + IMERG precip + 5 static (elevation, land cover, population, lat_rad, lon_rad) + 1 quasi-static (NDVI). MERRA-2 deliberately excluded as a Stage B predictor (already consumed by Stage A; double-dipping risk). Youn 2024's HCDC replaced by ERA5 total cloud cover; LHFL skipped (not in collection); TCWV and albedo added.*

*__(P6) Validation cadence aligned across Stage A and Stage B.__ Both stages now match AERONET within ±30 min of the slot centre (§8.0). The v3.4 "across the local day for Stage B" is gone — Stage B's atomic unit is the slot, not the day.*

*__(P7) Implementation drift register added (§14).__ The live `stage_b/config.py` values for `B1_W_TIME_H = 12 h`, `B1_MAX_NEIGHBOURS = 40`, and `RF_TRAIN_TARGET_ROWS = 2 × 10⁶` are documented with the rationale for each gap vs the fix-doc targets, plus a §8.2.5-triggered re-tune plan. This makes the as-built configuration auditable without burying the deviation inside a config file.*

*__(P8) Caveats consolidated.__ §10 grows from 19 to 23 entries: caveats 20–23 cover the ST-kriging temporal window edge effect (#20), the RF training sub-sample (#21), the per-slot RF temporal-coherence note (#22), and the ST-kriging warm-up at dataset edges (#23). The v3.4 daily-aggregation rationale ("3-slot floor") is removed as no longer applicable.*

*__(P9) References add Yang & Hu (2018)__ as the Stage B Step B1 methodological template.*

*Pre-v3.5.0 history (condensed): v3.3 introduced per-pixel Himawari L2/L3 stratum-aware merge and ICW fusion; v3.4.0 pivoted bias correction to MERRA-2 and fusion weights to triple collocation (both AERONET-independent), preserving AERONET for held-out validation. The v3.5.0 entry above describes the changes vs v3.4.0 in detail.*
