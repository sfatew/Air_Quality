# Near Real-Time AOD Mapping of Vietnam: Multi-Source Satellite Fusion with Bias Correction and Spatiotemporal Gap-Filling

### Thesis Framework & Methodology — Draft 3.4.0

---

## 1. Context, Motivation, and Research Questions

### 1.1 The problem

Vietnam is among the most air-pollution-affected countries in Southeast Asia, with Hanoi consistently ranking among the most polluted cities globally. A reliable, high-resolution, near-real-time aerosol optical depth (AOD) map is the essential upstream input for any operational PM2.5 estimation system over Vietnam. No such product currently exists at operational quality specifically for Vietnam.

Two recent products come closest to what is needed and define the gap this thesis fills:

- **Gupta et al. (2024)** produced a global merged Dark Target AOD product at 0.25°/30-min from six LEO and GEO sensors. It applies one retrieval algorithm uniformly to every sensor, uses equal-weight averaging across sensors with no AERONET correction, and serves a global audience. Over Vietnam this resolution is too coarse (~28 km), and Dark Target is not the best algorithm for every sensor over Vietnam's surface and aerosol types.
- **Ahn et al. (2021)** produced an hourly composite AOD over Northeast Asia by CDF-matching each sensor to AERONET, extending the per-site corrections spatially via Inverse Distance Weighting (IDW), and merging sensors via Inverse Composite Weighting (1/RMSE², "ICW"). Their domain includes two Vietnamese AERONET sites (Nghia Do, Son La). It is the methodological template closest to this thesis. However, their domain centers on Korea/Japan/China, their product is hourly (not 30-min), and they did not address dense gap-filling for the cloud-prone tropics.

This thesis adapts and extends those approaches for Vietnam's specific conditions, addressing four characteristics of the Vietnam domain that distinguish it from the regions where those products were developed:

1. **Himawari operates at very high viewing zenith angle (VZA) over northern Vietnam** (Vietnam is on the edge of the AHI disk), elongating the atmospheric path and enlarging pixel footprints by 2–5× over Hanoi (Gupta et al., 2024).
2. **Monsoon cloud cover is severe.** Valid Himawari L2 retrievals exist for only **10.3% of all hourly observations** across Vietnam, dropping to 6.9% in July (Nguyen et al., 2025). This means gap-filling is not optional — it is the dominant component of any usable product.
3. **Only two AERONET stations** (Nghia Do in the north, Bac Lieu in the south) are available, with no station in Central Vietnam. Bias correction must therefore be region-aware but spatially extrapolated where ground truth is absent.
4. **Aerosol regimes differ sharply between regions.** The north is dominated by anthropogenic + transboundary biomass burning, with shallow boundary layers and dry-season inversions; the south is influenced by marine aerosols and convective mixing; the center has mixed orographic/maritime regimes. A single sensor or single bias correction cannot serve all three.

### 1.2 Research questions

This thesis answers four research questions, each tied directly to one phase of the methodology:

- **RQ1.** Which AOD retrieval product per sensor (Himawari L2 vs. L3; VIIRS SNPP vs. NOAA-20) performs best over Vietnam, and does the best choice differ by region or season?
- **RQ2.** Can region- and season-aware bias correction against AERONET, combined with inverse-RMSE sensor weighting, produce a fused AOD product that is more accurate over Vietnam than either an equal-weight merge (Gupta 2024 style) or a single best sensor?
- **RQ3.** How effectively can ML-aided spatiotemporal gap-filling, using reanalysis AOD and meteorology as covariates, recover usable AOD on days/regions where satellites see nothing?
- **RQ4.** Does the resulting merged + gap-filled product improve daily AOD–PM2.5 coupling over Vietnam compared to the Himawari-only baseline of R² = 0.293 established by Nguyen et al. (2025)?

### 1.3 Claimed contributions

1. The first published Vietnam-specific multi-sensor merged AOD product at 0.05°/30-min covering Sep 2022 – Apr 2026.
2. A regional/seasonal **soft-calibration table** (linear α, β per sensor against MERRA-2) and an **AERONET-independent triple-collocation error-variance table** per sensor — both reusable by future studies and both produced without consuming the AERONET record (preserving AERONET for held-out validation).
3. An empirical comparison, over Vietnam, of three gap-filling strategies (spatial kriging, ERA5-shape temporal, RF with reanalysis covariates) — the first such comparison for this domain.
4. An evidence-based set of recommendations for upstream PM2.5 mapping in Vietnam, including the central-Vietnam AERONET gap.

---

## 2. Related Work and Methodological Positioning

The thesis builds directly on three product/methodology families. Section 7 maps each methodological choice to its source.

| Predecessor                                      | Domain                         | Spatial / Temporal | Bias correction                                       | Sensor fusion      | Gap-filling                             | Why insufficient for Vietnam                                                     |
| ------------------------------------------------ | ------------------------------ | ------------------ | ----------------------------------------------------- | ------------------ | --------------------------------------- | -------------------------------------------------------------------------------- |
| **Gupta et al. 2024** (NASA LEO-GEO DT)          | Global                         | 0.25° / 30-min     | None                                                  | Equal-weight mean  | None                                    | Coarse; single algorithm not optimized per sensor for Vietnam; no AERONET tuning |
| **Ahn et al. 2021** (NE Asia composite)          | NE Asia incl. Nghia Do, Son La | ~5 km / hourly     | Per-site CDF cubic polynomial → IDW spatial extension | ICW (1/RMSE²)      | None                                    | Centered on Korea; no dense gap-filling; hourly not 30-min                       |
| **Chen et al. 2023** (Himawari ML gap-fill)      | BTH/YRD/PRD China              | 0.05° / 10-min     | None (uses raw AHI)                                   | Single sensor      | RF (space) + DNN (time, fwd+bwd)        | Single sensor; trained on temperate China not tropical monsoon                   |
| **Youn et al. 2024** (RF Himawari gap-fill)      | South Korea                    | 0.05° / hourly     | None                                                  | Single sensor      | RF + CAMS + ERA5 met (12 vars)          | Single sensor; temperate climate                                                 |
| **Nguyen et al. 2025** (this team)               | Vietnam                        | Point sites        | Validation only                                       | None               | None                                    | Validation study only — no gridded product produced                              |

**This thesis = Ahn 2021's inverse-variance fusion framework adapted to Vietnam with region/season strata, but with per-sensor bias correction anchored against spatially complete MERRA-2 reanalysis (rather than per-AERONET-site CDFs) and fusion weights derived from triple-collocation σ² (rather than AERONET-RMSE), combined with a gap-filling step inspired by Chen 2023 / Youn 2024. The empirical anchors come from Nguyen et al. 2025.**

What is genuinely new:

- **Vietnam-specific sensor algorithm selection per region**, not a single retrieval algorithm everywhere (departure from Gupta 2024).
- **Region- and season-stratified bias correction anchored against spatially complete MERRA-2 reanalysis** rather than discrete AERONET sites, eliminating the central-Vietnam ground-truth gap that constrained Ahn 2021 (and that earlier drafts of this plan tried to patch with a LEO-anchored offset map).
- **AERONET-independent fusion weights** derived from triple-collocation σ² (Stoffelen 1998; McColl 2014), so AERONET is preserved entirely for held-out validation.
- **Two-stage product design**: a bias-corrected fused product (analogous to Ahn 2021) followed by an ML-aided gap-filled daily product (analogous to Chen 2023 / Youn 2024). The literature does these in isolation. Doing both in sequence for the same domain is uncommon.
- **Validation tied to the downstream PM2.5 application** with documented RANSAC-based daily-aggregate metrics from Nguyen et al. 2025 as the baseline to beat.

---

## 3. Study Domain, Period, and Output Specification

| Parameter                  | Value                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------ |
| Spatial domain             | Vietnam + ~2° buffer (8°N–23.5°N, 102°E–110°E)                                         |
| Output grid                | 0.05° × 0.05° (~5.5 km)                                                              |
| Native temporal resolution | 30-minute slots (48 files/day)                                                       |
| Aggregated outputs         | Daily, monthly, seasonal                                                             |
| Study period               | Sep 2022 – Apr 2026 (~3.7 years, matching AERONET availability at Nghia Do/Bac Lieu) |
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

**Notes (justified empirically by Nguyen et al. 2025):**

- _Himawari 8 → 9 transition (Dec 2022):_ The retrieval algorithm and product format are identical; treated as one continuous record.
- _Spectral harmonization 500 → 550 nm:_ Ångström interpolation gives negligible improvement (median R² shifts from 0.614 → 0.610, RMSE 0.299 → 0.303). Raw 500 nm Himawari is used as a direct proxy for 550 nm — saves significant compute with no measurable accuracy loss.
- _Himawari L2 vs L3 — both soft-calibrated, stratum-aware per-pixel merge:_ In the high-AOD urban north L2 is better (R = 0.701, %EE = 34.5% vs L3 R = 0.869 but bias −0.316 and %EE only 24.7% — L3 systematically underestimates events). In the low-AOD maritime south L3 is better (R = 0.824, %EE = 67.8% vs L2 R = 0.733). Both levels are soft-calibrated against MERRA-2 independently (§7.4.1) and then merged per pixel — for each (region, season) the level with the lower triple-collocation error variance σ²_TC supplies the primary pixel value and the other level fills its gaps — into one Himawari grid that enters fusion. The stratum-aware per-pixel merge preserves the level-specific bias removal at the training stage without doubling Himawari's fusion weight, and it reuses the σ²_TC table that fusion already needs (rather than hard-coding an L3-first or L2-first rule that would ignore the south-L3 / north-L2 empirical pattern).
- _MODIS MAIAC vs Deep Blue:_ MAIAC's 1 km pixels find cloud-free gaps much more often (~250–400 matches/year at each AERONET site, vs ~50 for Deep Blue 10 km). MAIAC is kept; MODIS Deep Blue is rejected on coverage grounds for this domain.
- _VIIRS L3 daily 1°:_ Rejected. At 1° resolution Vietnam is ~8 grid cells nationwide — destroys spatial structure and removes the within-day timing information needed for Himawari co-location.

### 4.2 Ground truth — AERONET V3 L2.0

| Site             | Lat / Lon           | Period covered      | N observations | Mean AOD₅₅₀ |
| ---------------- | ------------------- | ------------------- | -------------- | ----------- |
| Nghia Do (Hanoi) | 21.048°N, 105.800°E | Feb 2022 – Apr 2026 | 13,172         | 0.699       |
| Bac Lieu         | 9.28°N, 105.73°E    | Feb 2022 – Apr 2026 | 15,690         | 0.212       |

AERONET is interpolated from 500/675 nm to 550 nm using the site-specific Ångström exponent, matching the LEO retrieval wavelength.

### 4.3 Ground PM2.5 — Envisoft (validation context only)

27 stations selected from 63 available on a ≥85% completeness threshold over the full Nguyen 2025 study window, distributed 10/8/9 across north/central/south (Nguyen et al. 2025, Table 1). Hourly PM2.5, 451,082 records, 91.4% overall completeness. Used here only to validate that the merged AOD product captures pollution episodes visible in the PM2.5 record (Section 8.4); the actual AOD → PM2.5 regression is a downstream project.

**Validation-window completeness relaxation.** No Envisoft station meets the ≥85% bar across the held-out validation window alone (Jan 2025 – Apr 2026 ≈ 510 days). The §8.1.6 case-study analysis therefore relaxes the per-station completeness threshold to ≥50% for that window only; the ≥85% bar still applies to the headline 27-station figure quoted above. This relaxation is flagged in §10.

### 4.4 Ancillary data (covariates for bias correction and gap-filling)

| Dataset                                           | Role                                                                                                                                                                                                    |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ERA5 reanalysis                                   | RH (i.e: Humidity) (preferentially overridden by Envisoft in-situ), PBLH (constrained ≥ 50 m), 10-m wind, total cloud cover, surface solar radiation, dew-point — bias correction & gap-fill predictors |
| CAMS global reanalysis AOD (0.4°, hourly, 500 nm) | Coarse-resolution "shape" predictor for gap-filling, downscaled to 0.05° (Chen et al. 2023; Youn et al. 2024)                                                                                           |
| MERRA-2 AOD (0.5° × 0.625°, hourly)               | Soft-calibration anchor (§7.4.1) and alternative reanalysis predictor for gap-filling — used as a sanity check against CAMS                                                                                                |
| GPM IMERG precipitation (0.1° / 30-min → hourly)  | Wet-scavenging flag for validation; gap-fill covariate                                                                                                                                                  |
| MODIS NDVI (MOD13C1, 0.05°, 16-day)               | Surface/vegetation predictor for gap-fill                                                                                                                                                               |
| Copernicus GLO-30 DEM (30 m → resampled 0.05°)        | Topographic predictor for gap-fill                                                                                                                                                                      |
| MODIS Land Cover (MCD12Q1)                        | Surface-type stratification for bias correction                                                                                                                                                         |
| FIRMS active fire                                 | Flag extreme biomass-burning days for separate treatment                                                                                                                                                |
| LandScan Global population (~1 km, annual)                         | Anthropogenic source proxy (gap-fill predictor)                                                                                                                                                         |

Choice of ancillary predictors mirrors Chen et al. 2023 and Youn et al. 2024, which both validated their utility for AHI gap-filling.

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

1. **VIIRS is the most accurate L2 product** at both stations. Its negligible/small positive bias means it functions as an anchor that constrains the others during fusion.
2. **Himawari has a strongly asymmetric regional bias** — large negative in the north (−0.117 L2, −0.316 L3) and near-zero in the south. Single-coefficient correction will not work.
3. **The optimal Himawari product level is region-dependent**: L2 in the high-AOD north preserves events; L3 in the low-AOD south reduces scan noise. Both levels are soft-calibrated against MERRA-2 independently (§7.4.1), then merged per pixel using a stratum-aware preference (the level with the lower triple-collocation error variance σ²_TC per (region, season) supplies the primary pixel value; the other level fills its gaps) into one Himawari fusion input rather than being weighted as separate sources. This finding shapes both the *per-level soft calibration* and the *per-pixel level selection*, but the merged Himawari channel still enters fusion as a single row carrying `min(σ²_TC_L2, σ²_TC_L3)` per stratum (see §7.5 and §10 #17).
4. **MAIAC fails at Bac Lieu against AERONET** (R = 0.411) over reflective Mekong agricultural surfaces. Earlier drafts addressed this with a hand-set 0.1× MAIAC-south down-weight; v3.4.0 removes that band-aid. The soft calibration anchors against spatially complete MERRA-2 rather than the single Bac Lieu station, and the triple-collocation σ²_TC for the (MAIAC, south, *) strata sets MAIAC's fusion weight automatically — if MAIAC-south is genuinely unreliable, σ²_TC will be large there and the inverse-variance weight will be small.
5. **Inter-sensor agreement degrades north → south for MODIS–VIIRS** (R² = 0.837 / 0.638 / 0.545) but is U-shaped for VIIRS–Himawari (highest in south R² = 0.756, lowest center R² = 0.450). Central Vietnam is the weakest spot for sensor consistency.
6. **AOD availability is only 10.3% of hourly slots** (6.9% in July, 16.7% in April). Gap-filling is dominant, not auxiliary.
7. **Physics correction** (RH, PBLH) lifts hourly Himawari–PM2.5 correlation from r = 0.110 to r = 0.162 — a 1.5× improvement. Cheap and worth applying.
8. **Fine-mode (Rf ≥ 0.5) + uncertainty (≤ 0.5) filters help Himawari** (r 0.167 → 0.242 → 0.239) but produce **no measurable improvement for MAIAC** because MAIAC preprocessing already favors fine-mode conditions. Filter Himawari, don't bother filtering MAIAC.
9. **RANSAC robust regression** lifts daily Himawari–PM2.5 R² from 0.065 (OLS) to 0.293 — used here diagnostically, not as a product filter.

---

## 6. Methodology Overview

Stage A is split into a one-time **calibration / training track** and a per-slot **production track**. Both tracks consume the same persisted Stage A2 intermediate (one NetCDF per 30-min slot containing raw 0.05° gridded AOD per sensor), so gridding runs exactly once per slot per dataset regardless of how many times calibration or production are re-run.

```
       L2 Granules                       Reanalysis & ancillary
[Himawari, MAIAC, VIIRS-SNPP,               [CAMS, MERRA-2, ERA5,
 VIIRS-NOAA20]                               IMERG, NDVI, elev, land cover]
       │                                         │
       ▼                                         │
[Step A1: QA Filtering]                          │
       ▼                                         │
[Step A2: Regrid to 0.05°, 30-min slots]         │
       │   (persisted intermediate, shared)      │
       ├───────────────────────────────┐         │
       ▼                               ▼         │
  ┌─ CALIBRATION TRACK ─┐      ┌─ PRODUCTION TRACK ──────────┐
  │ (offline, once)     │      │ (per slot)                  │
  │                     │      │                             │
  │ collocate satellite │      │ read A2 grids               │
  │ ↔ MERRA-2 per       │      │      ▼                      │
  │ (sensor, region,    │      │ [Step A4: Soft calibration  │
  │  season) (§7.4.1)   │      │   linear α·sat + β]         │
  │      ▼              │      │      ▲                      │
  │ fit linear (α, β) ──┼─────►│      └── (α, β) from train  │
  │ per stratum         │      │      ▼                      │
  │      ▼              │      │ [Step A5: TC-weighted       │
  │ triple-collocation  │      │   fusion (1/σ²_TC)]         │
  │ σ²_TC per stratum  ─┼─────►│      ▲                      │
  │ (§7.4.2)            │      │      └── σ²_TC from train   │
  │                     │      │      ▼                      │
  │                     │      │ [Step A3: Physics           │
  │                     │      │   normalization]  ◄── ERA5  │
  │                     │      │      │    (applied after    │
  │                     │      │      │     fusion; separate │
  └─────────────────────┘      │      ▼     output field —   │
                               │  one NetCDF/slot  not fed   │
                               │       back into A4/A5)      │
                               └─────────────────────────────┘
       ▼
═══════ Stage A complete: 30-min merged ═════
       ▼                                     │
[Step B1: Daily aggregation]                 │
       ▼                                     │
[Step B2: Spatial gap-fill] ◄────────────────┤
       ▼                                     │
[Step B3: Spatiotemporal gap-fill (ML)] ◄────┘
       ▼
═══════ Stage B complete: gap-filled daily ═══
       ▼
[Step C: Validation against held-out AERONET + Envisoft case studies]
```

Calibration runs once in a linear chain (§7.4.4): grid → soft-calibrate vs MERRA-2 → triple-collocation σ² → production. No cyclic bootstrap is needed because neither anchor (MERRA-2 for soft calibration, inter-sensor TC for fusion weights) depends on a prior Stage A pass. Stage A produces the 30-min merged product (analogue of Ahn 2021's hourly composite, refined for Vietnam). Stage B produces the daily gap-filled product (analogue of Chen 2023 / Youn 2024, adapted to Vietnam). Stage C validates both.

---

## 7. Methodology Detail

### 7.0 AERONET-cell extraction for validation

Under the v3.4.0 architecture, satellite training pairs are constructed against MERRA-2 (§7.4.1) and inter-sensor triplets (§7.4.2), neither of which touches AERONET. AERONET is therefore reserved entirely for held-out validation (§8). The AERONET-cell extraction described in this section is a *validation* workflow, not a training workflow.

Three stages run independently: (1) **Stage A2 grid** writes one NetCDF per 30-min slot containing the raw per-sensor 0.05° gridded AOD before any bias correction (shared with the production fusion driver of §7.2, §7.5); (2) **extract** reads that gridded slot and samples each sensor at the AERONET station's 0.05° cell; (3) **match** temporally aligns the station-cell extracts to AERONET observations. Stages 2 and 3 feed §8.1.1's held-out AERONET metric panel; they are *not* used to fit any training table.

These stages are exposed as CLI verbs in `run_collocate.py`: `grid`, `extract`, `match` (plus a `collocate` shortcut for `extract + match`). The training-time calibration verbs are documented in §7.4.4: `soft_cal` (§7.4.1) and `tc_variance` (§7.4.2), neither of which consumes AERONET.

**Why a shared intermediate.** The same Stage A2 0.05° gridded NetCDFs feed AERONET validation extraction, MERRA-2 soft-calibration, TC σ² estimation, and production fusion. Gridding therefore runs exactly once per slot per sensor across the entire workflow. QA still runs at native pixel level before binning — only the spatial *averaging* lives in the shared pipeline. The earlier-draft layout, in which training averaged native-pixel neighbourhoods around the AERONET station while production averaged into 0.05° grid cells, produced training pairs and production cells at incompatible spatial scales; routing both through the same gridded intermediate eliminates that mismatch.

#### 7.0.1 Spatial matching

Every sensor is sampled from the Stage A2 0.05° gridded slot using the same tiered cell-neighbourhood fallback (Ichoku et al. 2002; Levy et al. 2010 scaled to the production grid):

1. **Exact cell (primary):** The 0.05° cell whose centre is closest to the AERONET station.
2. **3×3 cell neighbourhood (Fallback 1):** Mean of all valid cells within one config-grid cell of the station cell — half-width 1, side ≈ 15 km.
3. **5×5 cell neighbourhood (Fallback 2):** Half-width 2, side ≈ 25 km — used only when the inner tiers are empty. Approaches the Petrenko–Ichoku scale.

Each matchup records which tier was used and the within-neighbourhood AOD standard deviation. The tier hierarchy is identical across sensors because they have all already been box-averaged onto the same grid by Stage A2.

**Scene homogeneity filter — applied at collocation, exposed but not enforced in production.** A matchup is rejected if the within-neighbourhood AOD spread exceeds a threshold that scales with the mean AOD at the box, `box_std_max(mean) = max(BOX_STD_ABS_FLOOR, BOX_STD_SLOPE[sensor] × mean)` (Levy et al. 2010 §3.2). With cell-scale neighbourhoods, the std is computed on 1, 9, or 25 cell-mean values — i.e. it captures inter-cell heterogeneity at ≈ 5.5 / 15 / 25 km scales, which is the heterogeneity that actually matters for whether a single fusion cell value is representative.

The same per-cell `aod_std` and `cv` fields are written to the Stage A2 intermediate (§7.2.1), but **production fusion does not apply the box-std gate** — the §7.5 TC-weighted fusion already absorbs heterogeneity via the cross-sensor std term and the per-stratum σ²_TC. The CV field is exposed for downstream consumers (validation notebooks, downstream PM₂.₅ users) to apply their own `cv ≤ CV_MAX` filter if a use case calls for it.

#### 7.0.2 Temporal matching

**Strategy: satellite-centric.** One record per satellite "snapshot", where a snapshot is now the Stage A2 30-min slot grid (not the underlying 10-min L2 file or overpass granule). For each snapshot, all AERONET measurements within the matching window are averaged.

| Sensor | Snapshot unit (matchup row cardinality) | Matched to | Time window |
|--------|------------------------------------------|-----------|-------------|
| Himawari L2 | One row per 30-min slot — multiple 10-min files in the slot were already averaged at Stage A2 | AERONET observations near slot centre | ±30 min |
| Himawari L3 | One row per 30-min slot (multiple per day) → aggregated to a daily satellite mean before matching | AERONET daily mean (UTC) | Whole day |
| VIIRS SNPP / NOAA-20 | One row per 30-min slot — Stage A2 pools all granules whose timestamp falls in the slot's ±30 min window | AERONET observations near slot centre | ±30 min |
| MODIS MAIAC | One row per 30-min slot — Stage A2 filters MAIAC pixels by per-orbit UTC timestamp from the HDF global attribute | AERONET observations near slot centre | ±30 min |

The per-orbit MODIS logic lives *inside* Stage A2: when a calendar day's MAIAC tiles carry orbit timestamps, each orbit's pixels are placed into the slot(s) whose ±30-min window overlaps that orbit's overpass time. Slots without overlap contain no MAIAC variables. The no-timestamp daily-mean fallback used by earlier drafts is not applied because the Stage A2 intermediate has no concept of a synthesised slot time (see §10 #19).

### 7.1 Step A1 — Quality filtering

**Himawari AHI** — strict JAXA bit-mask gate on the L2 `QA_flag` (Band 4) / L3 `QA_flag_Merged` (Band 8), plus three product-level gates:

- **JAXA strict QA bit-mask** — a pixel passes only when every one of these bits is clean: `data_avail = 0`, `cloud = 0`, `retrieval_ok = 0`, `AOT confidence = 00` ("very good", bits 4–5), `additional_cloud = 0`, `Solz/Satz > 70° = 0` (bit 10, which subsumes the prior explicit SZA<70 / VZA<60 gates), `surface_refl_bad = 0`, `snow/ice = 0`, `turbid_water = 0`. L3 zeroes the snow/ice and turbid-water bits by design. The prior "VZA > 55° flagged with lower confidence" weight bookkeeping is dropped — bit 10 makes it redundant.
- **Strict-zero AOT gate**: AOT > `HIMAWARI_AOT_MIN` (= 0.0). Drops pixels whose retrieval reports exactly zero — a known JAXA failure mode where the algorithm bottoms out rather than declaring a non-retrieval.
- **Fine-mode fraction** Rf ≥ `HIMAWARI_RF_MIN` (= 0.5, Band 6, L2 only — L3 has no Rf so this gate doesn't apply). Validated by Nguyen 2025: improves r from 0.167 → 0.242.
- **Retrieval uncertainty** |Uncertainty| ≤ `HIMAWARI_UNC_MAX` (= 0.5, Band 2 / Band 4). Cloud-edge rejection.

**MODIS MAIAC (MCD19A2):**

- Valid AOD range: 0 ≤ AOD ≤ 5.
- `AOD_QA` bitmask: bits 8–11 ≤ `MODIS_QA_BITS_MAX` (= 4 — "Best" through "Marginal"; "Poor" and worse rejected). Marginal-or-worse pixels passing through scene-heterogeneity alone otherwise inflate MAIAC-north RMSE into the §7.4.1 `'none'` regime; with the bitmask on, the stratum is correctable.
- No additional fine-mode or uncertainty filters (MAIAC's internal multi-angle retrieval already restricts to high-quality land pixels).
- Terra and Aqua orbits separated by per-file UTC timestamp from the HDF metadata; each orbit's pixels are placed in the Stage A2 slots whose ±30 min window overlaps that orbit's overpass time. Days whose HDF lacks a readable orbit-timestamp attribute are skipped entirely; the daily-mean broadcast fallback used by earlier drafts is not applied (see §10 #19).

**VIIRS Deep Blue (SNPP + NOAA-20):**

- Uniform QA gate: a pixel passes when `QA_Flag_Land ≥ VIIRS_QA_MIN` (= 2) OR `QA_Flag_Ocean ≥ VIIRS_QA_MIN`. Coastal pixels at QA = 1 inflate VIIRS RMSE in mixed-surface cells, so the relaxed dual-threshold rule used by earlier drafts ("Land ≥ 2, Ocean/coast ≥ 1") is not retained.
- Valid AOD range: 0 ≤ AOD ≤ 5.
- SNPP and NOAA-20 treated as separate sensors throughout.

### 7.2 Step A2 — Regridding to 0.05° × 30-min

#### 7.2.1 Spatial aggregation

Box-averaging per Gupta et al. 2020: only pixels whose centre falls inside a 0.05° cell contribute (no nearest-neighbour fill or ring search).

1. Per 30-min window, collect L2 pixels whose centre coordinates fall in each 0.05° cell.
2. MAIAC (1 km) and VIIRS DB (6 km): box-average to per-cell mean, std, count, mean VZA, mean SZA.
3. Himawari (0.05° native): already on the target grid — no spatial resampling needed.

**Output per sensor per 30-min slot:** mean AOD, within-cell std, **coefficient of variation `cv = std / max(mean, 0.02)`**, valid-pixel count, theoretical maximum pixel count at the cell's latitude, mean VZA, mean SZA. The CV field is the within-cell heterogeneity proxy that consumers (collocation extraction, validation notebooks) can gate on via `cv ≤ CV_MAX`; Stage A2 itself **does not** apply a heterogeneity rejection at the gridding stage. The floor of 0.02 in the denominator prevents clean-air cells with vanishing means from spiking the CV.

**Persistence.** Steps A1+A2 are computed once per slot per sensor and written to a per-slot NetCDF. AERONET validation extraction (§7.0), MERRA-2 soft-calibration (§7.4.1), TC σ² estimation (§7.4.2), and the production fusion driver (§7.5) all *read* from this intermediate rather than re-running the read+QA+bin pipeline. Each NetCDF holds one variable group per sensor present in the slot; sensors with no retrieval are omitted, not written as all-NaN.

#### 7.2.2 Temporal slot assignment per sensor

The 30-min slot cadence (48 slots/day, centred at 00:00, 00:30, …, 23:30 UTC) is a common reference frame that the four sensors reach by different paths, reflecting their different native temporal resolutions:

| Sensor | Native cadence | Slot strategy | Effective window |
|--------|---------------|---------------|-----------------|
| Himawari L2 | 10-min snapshots | All L2 snapshots within ±15 min of the slot centre are **averaged** pixel-by-pixel into one slot grid | ±15 min (~3 files/slot) |
| Himawari L3 | 1-hour composites | The nearest L3 composite within ±30 min is used as-is; no averaging across composites | ±30 min (1 file/slot) |
| VIIRS SNPP / NOAA-20 | ~6-min granules, ~1–2 overpasses/day | All granules within ±30 min of the slot centre are pooled, then box-averaged to the 0.05° grid | ±30 min |
| MODIS MAIAC | Multi-orbit HDF per day (~2 orbits) | Daily HDF loaded once; per-orbit UTC timestamps extracted from file metadata; each orbit's pixels are included only in slots whose ±30-min window overlaps that orbit's overpass time | ±30 min per orbit |

**Consequences of mismatched cadences:**

- **Himawari L2 — no slot duplication.** With a ±15 min window and 10-min file spacing, each L2 snapshot falls in exactly one slot. Up to three consecutive snapshots are averaged into the slot grid, then the 500→550 nm Ångström correction is applied once to the averaged result.

- **Himawari L3 — consecutive slots share one composite.** Because the L3 window is ±30 min but composites are hourly, both the X:00 and X:30 slots within the same clock-hour draw from the same hourly composite. This is intentional: the L3 product already integrates observations across the full hour, so both half-hour slots carry equivalent information content and no temporal interpolation between adjacent composites is attempted.

- **VIIRS — potential single-overpass double-slot contribution.** A VIIRS granule at time *t* is eligible for any slot whose ±30-min window contains *t*. A granule at 10:22 UTC, for example, satisfies the window condition for both the 10:00 and 10:30 slots and is gridded into both. In practice this affects at most one or two slot pairs per overpass per day and is a known limitation (§10 #18).

- **MODIS MAIAC — most slots receive no MODIS data.** Terra overpasses Vietnam around 03:30 UTC (~10:30 local), Aqua around 06:30 UTC (~13:30 local). Only the one or two slot pairs whose ±30-min window overlaps an actual overpass will contain MODIS pixels; all remaining slots leave the MODIS grid as NaN. This is fully consistent with VIIRS slot handling — both LEO sensors contribute exclusively near their overpass time — and avoids contaminating pre-dawn or evening slots with mid-morning retrieval data. When orbit timestamps are absent from the HDF file (attribute unreadable), MODIS is dropped for that day rather than being broadcast to all 48 slots (see §10 #19).

### 7.3 Step A3 — Physics normalization (stored as a separate output, applied after fusion)

```
AOD_phys = AOD_merged × (1 − RH/100)^γ / PBLH
```

with γ = 0.6 (mixed-type empirical; Kotchenruther & Hobbs 1998), PBLH constrained ≥ 50 m, RH and PBLH from ERA5 bilinearly interpolated from 0.25° to 0.05°.

**Ordering note:** In the production pipeline, Step A3 runs _after_ Steps A4 and A5. The physics correction is applied to the fused `AOD_merged` and stored as a separate output field alongside the ERA5 RH and PBLH fields. It is **not** fed back into the bias-correction or fusion steps. This is deliberate: AERONET measures raw column AOD, so bias-correction training and ICW fusion must operate on raw AOD; physics normalization is a downstream PM₂.₅-modelling convenience that does not belong in the calibration loop.

This step is validated by Nguyen 2025 to improve hourly Himawari–PM₂.₅ from r = 0.110 → 0.162. If ERA5 data are unavailable for a slot, the physics fields are omitted but the raw merged AOD is always written.

### 7.4 Step A4 — MERRA-2-anchored bias correction + triple-collocation error variance (v3.4.0)

**Strategy.** Bias correction and fusion weights are both anchored against domain-wide reference data — MERRA-2 reanalysis for bias correction, inter-sensor triple collocation for fusion weights. AERONET is reserved entirely for held-out validation (§8); neither the soft-calibration coefficients (§7.4.1) nor the TC error variances (§7.4.2) consume AERONET observations. This is a deliberate departure from the predecessor Ahn 2021 framework and from earlier drafts of this plan, both of which used per-AERONET-site CDFs for bias correction and AERONET-RMSE for weights; with only two AERONET stations (Nghia Do, Bac Lieu) in Vietnam and none in the central region, that earlier design could not produce a defensible bias correction for central Vietnam, and §8 could not test held-out performance without consuming the only ground truth that existed.

Two architectural changes do the work:

1. **Bias correction** is anchored against MERRA-2 reanalysis AOD (Randles et al. 2017; Buchard et al. 2017), spatially complete across Vietnam. The Ahn 2021 quantile-mapping idea is preserved in form, but the reference distribution is MERRA-2's regional/seasonal distribution rather than an AERONET station's. The linear soft-calibration form follows Ding et al. 2025 (their §2.1 / Figure S2 — used for VIIRS DT against MERRA-2).
2. **Fusion weights** are computed by triple collocation (Stoffelen 1998; Gruber et al. 2016; McColl et al. 2014 extended TC), which estimates per-sensor error variance from inter-sensor disagreement statistics *without* any ground-truth reference.

MERRA-2 is used **only as a training-time anchor**. It is *not* a sensor in the production merge equation; satellite-empty cells stay as gaps and are passed to Stage B, preserving the §7.6 merge → §7.7-§7.8 fill structure. CAMS (Inness et al. 2019) — a distinct reanalysis (ECMWF IFS, different model core, different aerosol species, separately assimilated chemistry) — is reserved as a Stage B fill covariate (§7.8) so the two reanalysis sources do not double-count.

#### 7.4.1 Soft calibration against MERRA-2

For each (sensor, region, season) stratum:

1. At the satellite's native 0.05° grid and slot (post-§7.2), collocate every valid satellite pixel with the MERRA-2 hourly AOD550 value at the same cell and slot. MERRA-2's 0.5° × 0.625° fields are bilinearly resampled to 0.05° before matching. MERRA-2's hourly timestamps are nearest-neighbour matched to the 30-min slot centre.
2. Fit a linear regression `MERRA2 = α · sat + β` over the training window (Sep 2022 – Dec 2024). The transfer function `sat_corrected = α · sat + β` is the soft calibration. Linear (rather than full PCHIP CDF) is chosen because MERRA-2 at 0.5° is structurally smoother than satellite AOD at 0.05° — fitting a full quantile map would over-flatten the satellite tails. This is the same form Ding et al. 2025 used to soft-calibrate the VIIRS DT products against MERRA-2.
3. CV-time guard rail: if the 5-fold cross-validated `α` lies outside [0.5, 2.0] or |β| > 0.2, the stratum is routed to `'none'` — the satellite enters fusion uncorrected and receives a penalty weight at §7.5 (`NONE_PENALTY_FACTOR × σ²_prior`, default factor = 2.0, so the weight drops by ~4×). This handles the rare case where a sensor's distribution is genuinely incompatible with MERRA-2 over a stratum.

Strata used:

- **Region:** North (lat ≥ 16°N), Central (11.5°N ≤ lat < 16°N), South (lat < 11.5°N). Central is a first-class stratum because the MERRA-2 anchor exists there.
- **Season:** Dry (Oct–Apr) / Wet (May–Sep).
- **Himawari level:** L2 and L3 are soft-calibrated independently; the §7.5 stratum-aware per-pixel merge before fusion preserves the per-level treatment.

The trained `(α, β)` per stratum are persisted to `soft_calibration.json` and reloaded at run time.

#### 7.4.2 Triple-collocation error variance

For each (sensor, region, season) stratum, the post-soft-calibration error variance `σ²_i` is estimated using triple collocation. Given three datasets X, Y, Z observing the same truth with mutually uncorrelated errors:

```
σ²_X = Var(X) − Cov(X, Y) · Cov(X, Z) / Cov(Y, Z)
σ²_Y = Var(Y) − Cov(X, Y) · Cov(Y, Z) / Cov(X, Z)
σ²_Z = Var(Z) − Cov(X, Z) · Cov(Y, Z) / Cov(X, Y)
```

(Stoffelen 1998 Eq. 5; Gruber et al. 2016 §2.1.) The extended TC of McColl et al. 2014 additionally recovers the unknown additive bias and a correlation-with-truth metric per sensor; both diagnostics are computed and persisted alongside σ² for §8.1.3.

**Triplet construction for Vietnam.** Six "sensors" are eligible: {Himawari L2 (soft-calibrated), Himawari L3 (soft-calibrated), MAIAC, VIIRS-SNPP, VIIRS-NOAA20, MERRA-2}. All triplets that include MERRA-2 plus two satellites are enumerated; pure-satellite triplets (three independent satellites at the same slot) are also enumerated when overlap exists. The σ² for each sensor is taken as the median across all triplets that contain it, per (region, season). The median rather than mean is used because individual triplets can produce noisy estimates in low-N strata (Gruber et al. 2016 §3.1). For the merged Himawari channel that enters §7.5 fusion, σ²_TC is rolled up as `min(σ²_TC_L2, σ²_TC_L3)` per stratum, consistent with the stratum-aware per-pixel L2/L3 merge.

**Independence assumption.** TC requires error-uncorrelated triplet members. The thesis rejects triplets only when the *retrieval algorithm* is shared, not merely the underlying input radiances:

1. **VIIRS-SNPP + VIIRS-NOAA20** share the Deep Blue algorithm (Sayer et al. 2020), so their algorithm-specific errors propagate together even though the two instruments are independent. Triplets containing both are rejected.
2. **MERRA-2 + MAIAC** is *allowed*: MERRA-2's GOCART assimilation ingests MODIS Dark Target NNR retrievals (Randles et al. 2017 §3.1), not MAIAC. The only shared dependency between MERRA-2 and MAIAC is the MODIS L1B radiances and instrument calibration — a residual correlation channel that most TC literature treats as much weaker than a shared-retrieval violation.
3. **Himawari L2 + L3** is *allowed*: the JAXA L3 V3 `merged_aot` field is a composite of land and ocean retrievals (with the QA flag explicitly named `QA_flag_Merged` to encode the distinction), not an hourly mean of L2 pixels. L2 and L3 share AHI radiances and calibration but differ algorithmically, parallel to the MERRA-2 + MAIAC case.

**Sensitivity check.** σ²_TC is computed under both the rule above (permissive) and a strict variant that additionally rejects all pairs sharing the underlying instrument (MERRA-2 + MAIAC, Himawari L2 + L3). If the per-sensor σ² rankings differ between the two variants, the strict ranking becomes the headline and the permissive variant is reported alongside as a robustness diagnostic. If they agree, the permissive variant is the headline (more triplets, tighter σ² estimates in low-N strata) and the strict variant is the robustness diagnostic.

The retained triplet set per stratum is logged with the σ² table so the construction is auditable.

**Floor against the Sayer/Levy EE envelope.** σ²_i is floored at `(0.05 + 0.15 · AOD_ref)²` with AOD_ref = 0.3 (Levy et al. 2013; Sayer et al. 2020) to prevent low-N strata from producing runaway weights. The floored value is what enters §7.5.

**Sanity benchmark.** The TC-derived σ² is cross-checked once against the AERONET–satellite RMSE on training-window pairs at the two AERONET cells, after soft calibration has been applied. Agreement within a factor of 2 is expected; larger gaps trigger a triplet-set audit (usually reveals an undetected error-correlation pair). The benchmark uses training-window AERONET only as a one-time sanity check — AERONET is not used to adjust the weights and the §8 held-out window is not touched.

The TC σ² table is persisted to `tc_error_variance.json`.

#### 7.4.3 Per-sensor correction summary

| Sensor | Soft calibration | TC error variance source |
| --- | --- | --- |
| Himawari L2 | Linear vs. MERRA-2 per region+season (N/C/S × Dry/Wet) | Triplets with MAIAC, VIIRS-*, and/or MERRA-2; merged-Himawari σ² is `min(σ²_L2, σ²_L3)` per the §7.5 stratum-aware merge |
| Himawari L3 | Linear vs. MERRA-2 per region+season | Same; rolled up via min |
| MAIAC | Linear vs. MERRA-2 per region+season | Triplets with Himawari-L2, Himawari-L3, VIIRS-*, and/or MERRA-2 (MERRA-2 assimilates MODIS Dark Target NNR, not MAIAC; see §7.4.2 independence rule 2) |
| VIIRS SNPP | Linear vs. MERRA-2 per region+season | Triplets with Himawari, MAIAC; not paired with VIIRS-NOAA20 in same triplet |
| VIIRS NOAA-20 | Linear vs. MERRA-2 per region+season | Same; not paired with VIIRS-SNPP |
| MERRA-2 | (not corrected; serves as anchor) | σ² reported from MERRA-2-containing triplets for diagnostics; **not used** in §7.5 |

#### 7.4.4 Bootstrap ordering (single-pass calibration)

Calibration runs once in a linear chain — no chicken-and-egg bootstrap, because no step depends on a prior Stage A pass:

1. **Grid (A1+A2).** `run_collocate.py grid` over the full study period — persists slot-NetCDFs that every subsequent step reads.
2. **Soft-calibrate.** `run_collocate.py soft_cal` — for each (sensor, region, season) regress against MERRA-2 over the training window. Persist `soft_calibration.json`.
3. **TC σ² table.** `run_collocate.py tc_variance` — apply soft calibrations, then run triple collocation per stratum. Persist `tc_error_variance.json`.
4. **Production Stage A.** `run_stage_a.py` over the full study period loads both tables and emits 30-min NetCDFs.

Steps 2 and 3 must be re-run together whenever the gridding or QA filters change, but neither depends on the other in a cyclical way. The chain is linear.

---

### 7.5 Step A5 — TC-weighted fusion (v3.4.0)

For each 0.05° cell and 30-min slot:

```
w_i = 1 / σ²_TC,i(region, season, sensor)
AOD_merged = Σ(w_i · AOD_i_corrected) / Σ(w_i)
```

where `σ²_TC,i` is the §7.4.2 triple-collocation error variance, floored at the Sayer/Levy EE envelope. This is identical in form to Ahn 2021's ICW (their Eq. 3), but the variance source is AERONET-independent triplet statistics rather than per-site AERONET-RMSE.

**MERRA-2 inclusion rule.** MERRA-2 does NOT enter the production merge sum. Its role ends at training time. Slots with zero satellite retrievals stay as gaps and are forwarded to Stage B (§7.6). This is the design decision that preserves the merge → fill architecture: MERRA-2 cannot stand in for a missing satellite cell without collapsing the empirical AOD product onto the reanalysis it was calibrated against.

**Himawari L2/L3 stratum-aware per-pixel merge.** L2 and L3 are soft-calibrated independently in §7.4.1 and merged per pixel before fusion, with the level having the lower σ²_TC for the (region, season) stratum supplying the primary pixel value and the other filling its gaps. The merge decision and the fusion weight are derived from the same `tc_error_variance.json` table.


**Sensor inclusion rule.** Every available sensor in the slot enters the merge weighted by 1/σ²_TC; if exactly one sensor is present the cell is flagged as low-confidence; if no sensor is present the cell is left as a gap and forwarded to Stage B.

Sensor inclusion is uniform across strata because soft calibration (§7.4.1) produces bias-corrected values for all six (3 region × 2 season) strata, including MAIAC-south. A bias correction anchored on a single AERONET station for the south (Bac Lieu, R = 0.41) would have had to route MAIAC-south to `'none'`; the MERRA-2 anchor's continuous south coverage and the §7.4.2 TC σ² together let MAIAC-south's actual error structure set its weight.

**No hand-set multipliers.** Every weight decision flows through `tc_error_variance.json`; there is no separate `MODIS_SOUTH_WEIGHT_FACTOR` or `HIMAWARI_WET_WEIGHT_FACTOR` (band-aids that earlier drafts of this plan used to compensate for weak decision diagnostics). A change to the σ²_TC table cannot be silently overridden by an out-of-sync multiplier in `config.py`.

**Stage A output.** Each 30-min NetCDF contains per cell:

- `AOD_merged` (TC-weighted mean of soft-calibrated sensors)
- `AOD_std` (cross-sensor spread)
- `n_sensors`, `dominant_sensor`, `confidence_flag`
- Per-sensor soft-calibrated grids (diagnostic; written only when data are available)
- `AOD_phys_corrected` (§7.3 physics-normalized)
- ERA5 RH and PBLH (when available)

`dominant_sensor` codes: **1 = Himawari (merged L2/L3), 3 = MODIS MAIAC, 4 = VIIRS SNPP, 5 = VIIRS NOAA-20**. `confidence_flag` codes: **0 = no data, 1 = Himawari only, 2 = LEO only, 3 = Himawari + LEO, 4 = multi-LEO + Himawari**.

---
## Stage B — Gap Filling


### 7.6 Step B1 — Daily aggregation

The §7.5 ICW fusion produces one 30-min merged AOD field per slot at 0.05°. Step B1 collapses each calendar day's slots into a single daily AOD value per cell, plus the auxiliary fields that downstream PM₂.₅ users and the §8 validation step need.

**Daily mean per cell.** For each cell, the daily value is the arithmetic mean of valid 30-min slot AOD values within the local day (UTC+7, 00:00–23:30). A cell is set to *missing* unless it has at least **3 valid slots** in that day. The 3-slot floor is set deliberately low because in monsoon months the median Himawari coverage per cell is only a handful of slots (per §5.2 finding 6: 6.9% of all slots in July, ~3 of 44 daylight slots); a higher floor would mask a high fraction of the dataset and push the entire daily product into Stage B3 territory. The trade-off is recorded in §10.

**Outlier handling within a day.** Before averaging, slots whose AOD differs from the within-day median by more than 3 × the within-day median absolute deviation (MAD) are dropped. This catches Himawari single-slot spikes near cloud edges (high-VZA pixel inflation at slot boundaries) without removing genuine within-day AOD evolution. If MAD = 0 (all slots identical), no slots are dropped.

**Per-slot confidence weighting.** The mean is weighted by the §7.5 per-slot weight sum (a proxy for how many sensors contributed at that slot, and how confident those contributions were per σ²_TC). A slot fused from a single sensor under a noisy stratum contributes less than a slot fused from three sensors under low-σ²_TC strata. This avoids "one bad slot one bad day" failures, particularly for wet-season cells where Himawari is the only contributor and its TC-derived weight already reflects the wet-season error structure (§7.4.2).

**Auxiliary daily fields written alongside the daily mean:**

| Field | Definition |
| --- | --- |
| `daily_mean` | Confidence-weighted daily mean as above |
| `n_slots` | Number of valid (post-outlier) slots used |
| `daily_std` | Standard deviation across the slots |
| `daily_max` | Maximum slot AOD within the day |
| `hour_of_max` | Local hour of the `daily_max` slot |
| `weight_sum` | Sum of ICW weights across slots (provenance / confidence proxy) |
| `sensor_set` | Bitmask of which sensors contributed in any slot that day |

`daily_max` and `hour_of_max` are kept for downstream PM₂.₅ workflows because the diurnal cycle's peak is the most informative single feature for surface PM₂.₅ inversion (Nguyen et al., 2025 §3.4).

---

### 7.7 Step B2 — Spatial gap-filling (baseline)

Two regimes, distinguished by how isolated a missing cell is from valid observations on the same day:

**Approach A — interior gaps (ordinary kriging).** For cells whose nearest valid same-day observation lies within 200 km, apply ordinary kriging fitted *that day*. The variogram model is **spherical** (chosen over exponential/Gaussian because it has a finite range, which matches the empirical AOD spatial-correlation length of ~100–200 km observed at sub-day scale in monsoon Asia). The variogram is fit on the same-day observed cells with explicit lag binning (15 km lag bins, 10 lags, ~150 km maximum lag), refit per day rather than using a climatological variogram so that the day's actual spatial pattern is respected. The nugget is left as a free parameter (not forced to zero) to absorb measurement noise. Anisotropy is left isotropic in v1 — Vietnam's N–S elongation suggests directional kriging may help, but the same-day sample sizes are typically too small for stable anisotropy estimation; this is recorded as future work. A minimum-neighbour rule requires at least 30 valid same-day cells within the kriging search radius before a cell is filled, otherwise the cell is escalated to Approach B.

**Approach B — persistent gaps (climatological pattern fill).** A cell is *persistent* if it has been gap on the kriging path for ≥ 3 consecutive days, or if its same-day nearest valid observation is > 200 km away. For these cells, the value is the **same-month-of-year mean** across all *prior* years in the dataset (e.g., a persistent cell on 14 July 2025 takes the mean of all observed values in that cell for July 2022, 2023, 2024). The climatological value is then down-weighted toward the regional climatological mean by `(1 − fraction_observed_for_that_cell_over_all_years)` — cells that have *never* been observed in that month inherit only the regional mean. The provenance flag `gap_fill_method = kriging_climatology` is set, with the lowest confidence in the §8.2.3 audit.

This baseline alone produces a usable daily product but does not exploit reanalysis information. It is the §11 fallback if Stage B3 is dropped on timeline grounds.

---

### 7.8 Step B3 — Spatiotemporal gap-filling with ML

The literature converges on the conclusion that ML with reanalysis covariates substantially outperforms pure spatial interpolation for AOD gap-filling, especially during persistent cloud cover (Youn et al., 2024: RMSE 0.064 on blind test, CC 0.711 against AERONET on filled-only cells; Chen et al., 2023: R = 0.80 against AERONET).

Two candidates are evaluated under the §8.2 protocol; the better-performing on the held-out AERONET window (Jan 2025 – Apr 2026) becomes the primary B3 product, with the §7.7 kriging baseline always co-shipped so the user can opt out of ML for trend analyses or extreme-value statistics.

**Methodological linkage between the two candidates.** Youn 2024 and Chen 2023's SIM (Space Interpolation Model) are the *same architectural pattern* — a random forest that maps reanalysis AOD + meteorology + geography at a single (cell, time) onto observed AOD, with no lagged AOD inputs. Youn's published method *is* a spatial gap-fill; Chen's full method *adds* a Time Interpolation Model (TIM, a forward/backward DNN over flanking observed AOD) on top of SIM. The thesis follows that lineage directly:

- **Candidate 1 (§7.8.1)** is a pure spatial RF gap-fill, treated equivalently to Youn 2024 *and* Chen 2023's SIM module. It is the primary because it is both well-validated in the literature and the cheaper of the two to train and audit. It contains no lagged-AOD predictors — every input is a contemporaneous covariate.
- **Candidate 2 (§7.8.2)** is Candidate 1 *plus* a temporal DNN that takes flanking-day AOD and meteorological deltas as inputs and outputs a temporally-refined AOD at the target day. This is the option-b adaptation of Chen 2023's SIM + TIM stack at daily (rather than 10-min) cadence.

Training Candidate 1 is therefore a prerequisite for Candidate 2, not a parallel effort: Candidate 2 consumes Candidate 1's SIM predictions as one of its temporal anchors when the true t±1 observation is missing (per §7.8.2). This linkage also clarifies the §11 scope-management order: shipping Candidate 1 alone gives a defensible "Youn 2024 method, replicated for Vietnam" result; Candidate 2 is the Chen 2023 extension on top.

#### 7.8.1 Candidate 1 — Spatial Random Forest (Youn 2024 ≡ Chen 2023 SIM, primary)

Predicts the daily AOD per 0.05° cell from a fixed predictor vector of reanalysis AOD, meteorology, and geography — *contemporaneous covariates only*, no lagged AOD. This makes Candidate 1 a pure spatial gap-fill (it cannot exploit temporal autocorrelation in observed AOD; that work is what Candidate 2's TIM/DNN adds). Random Forest is chosen as the primary because (a) it handles missing predictors without imputation, (b) it produces a built-in out-of-bag (OOB) error estimate that doubles as a cheap CV diagnostic, (c) it gives variable importance for free (used in §8.2.4), and (d) Youn 2024 reported RF performance competitive with deep methods on a similar Himawari-AOD task (RMSE 0.064 on blind test) at a fraction of the training cost.

**Predictor vector (per cell, per day).** The 12 predictors below mirror Youn 2024's set (which is, modulo two swaps justified for the Vietnam regime, the same family Chen 2023's SIM uses). All inputs are contemporaneous with the target day — there are no lagged-AOD predictors. Naming follows Youn 2024 Table 1 / Chen 2023 Table 1 for clarity:

| # | Predictor | Source | Native res. | Pre-processed to |
| --- | --- | --- | --- | --- |
| 1 | CAMS-AOD (550 nm, daily mean) | CAMS global atmospheric composition forecasts | 0.4° / 3-hourly | 0.05° / daily (bilinear spatial + 3h→daily mean) |

| 3 | T2M (2-m temperature) | ERA5 | 0.25° / hourly | 0.05° / daily mean |
| 4 | DPT (2-m dew-point temperature) | ERA5 | 0.25° / hourly | 0.05° / daily mean |
| 5 | RH (2-m relative humidity, derived from T2M & DPT) | ERA5 | 0.25° / hourly | 0.05° / daily mean |
| 6 | U10 (10-m u wind) | ERA5 | 0.25° / hourly | 0.05° / daily mean |
| 7 | V10 (10-m v wind) | ERA5 | 0.25° / hourly | 0.05° / daily mean |
| 8 | BLH (boundary-layer height) | ERA5 | 0.25° / hourly | 0.05° / daily mean |
| 9 | LHFL (surface latent heat flux) | ERA5 | 0.25° / hourly | 0.05° / daily mean |
| 10 | DSSF (downward surface solar flux) | ERA5 `ssrd` | 0.25° / hourly | 0.05° / daily mean |
| 11 | SP (surface pressure) | ERA5 | 0.25° / hourly | 0.05° / daily mean |
| 12 | TP (total precipitation) | IMERG / ERA5 | 0.1° / 0.25° | 0.05° / daily sum |

Static covariates that vary by cell but not by day are passed in once as part of the cell ID: elevation (Copernicus GLO-30, 30 m → 0.05° mean), NDVI (MOD13C1 16-day, nearest-in-time), land cover (MCD12Q1, annual), population density (LandScan, annual), and the cell's lat / lon (radians, so the RF can learn region-specific behavior even where the explicit regional mask is absent).

The Youn 2024 set excludes precipitation (TP) and uses HCDC (high cloud cover) and ALB (albedo); the plan keeps TP in (monsoon precipitation is a stronger AOD wet-deposition signal in Vietnam than high cloud per se) and drops HCDC and ALB (HCDC was Youn's lowest-importance variable at 2.55%; ALB is captured by NDVI + land cover in the Vietnam record). All temporal-autocorrelation modelling — Chen 2023's TIM contribution — is deferred to Candidate 2 (§7.8.2). Keeping Candidate 1 strictly spatial preserves the clean Youn 2024 ≡ Chen 2023 SIM comparison and isolates the question "does adding a temporal component buy more accuracy than it costs in compounded error?" to the §8.2.2 head-to-head.

**Training data construction.** Training pairs (X, y) are extracted from the Stage A merged daily product on the training partition (Sep 2022 – Dec 2024). For each cell × day where the merged daily product has a *valid* AOD value, y = the observed AOD and X = the 12-predictor vector. Cells where the daily aggregation in §7.6 fell below the 3-slot floor are excluded from training so the model is not learning to reproduce noisy partial-day means. The full training table over Sep 2022 – Dec 2024 yields on the order of 10⁷ rows (28 months × ~365 days × ~1,000 valid cells per day per Vietnam coverage).

**Hyperparameters.** Initial values follow Youn 2024 §2.2.2: N = 100 trees, max_depth = 20, min_samples_leaf = 5. These are then refined by minimising OOB MSE over a small grid: N ∈ {100, 200, 500}, max_depth ∈ {15, 20, 25, None}, min_samples_leaf ∈ {1, 5, 10}. Implementation: `scikit-learn` RandomForestRegressor with `n_jobs = −1` and a fixed `random_state` for reproducibility. The full grid takes ~6 hours on a 16-core machine for the Vietnam dataset.

**Validation protocol.** Per §8.2.1: 80/20 train / internal-test split inside the training partition + 5-fold CV inside the 80%, with OOB error as a free third diagnostic. **The 5-fold CV uses contiguous temporal blocks** (≈ 5.6 months per fold across the Sep 2022 – Dec 2024 training window) rather than random shuffling — AOD is strongly autocorrelated in both space and time, and random folds would place near-twin cells from the same day in train and val, overstating CV performance. Temporal blocking forces each fold to generalise to a held-out calendar period (often a held-out season), mirroring the §8.0 train/held-out split. The three numbers (`RMSE_train`, `RMSE_CV`, `RMSE_internal_test`, plus OOB) must agree within ±15% (a train-vs-validation consistency criterion) before the candidate moves to the §8.2.2 held-out AERONET test.

**Expected performance.** Based on Youn 2024 results adjusted for the Vietnam regime, the priors are: OOB R² in the 0.85–0.92 range (Korean Peninsula was 0.93 because their dry-cold half year is favourable), AERONET-blind RMSE in the 0.18–0.25 range at Nghia Do (Vietnam's high-AOD north is harder than Korea), and a noticeable wet-season degradation (Youn reported humid-condition RMSE 0.105 vs cold-dry 0.047, a 2.2× ratio; Vietnam's monsoon is wetter so a 2–3× ratio is expected).

#### 7.8.2 Candidate 2 — Spatial RF + temporal DNN forward/backward (Chen 2023 SIM + TIM, time permitting)

**Composition.** Candidate 2 is Candidate 1 (the §7.8.1 spatial RF, equivalent to Chen 2023's SIM) plus a Time Interpolation Model (TIM), implemented as a forward + backward Deep Neural Network. Chen 2023's original TIM is sub-daily — its forward DNN predicts AOD(t) from AOD(t − 10 min) plus meteorology and flanking hourly values; the backward DNN does the symmetric job from AOD(t + 10 min); the two are combined by a lead-time-weighted average (Chen 2023 Eq. 1). The thesis re-purposes this at daily cadence (the "option-b" form below); the architectural pattern — RF spatial backbone + forward/backward DNN temporal refinement — is unchanged.

**Placement at daily cadence.** Stage B aggregates to daily upstream of B3, so a literal port of Chen 2023 would have to either (a) sit *before* §7.6 daily aggregation, refining the 30-min Stage A grid to 10-min before B1 collapses it, or (b) be re-purposed at daily resolution by replacing "10-min flanking AOD" with "1-day flanking AOD." Option (a) preserves Chen's intent but multiplies the data volume by 3× and only buys benefit if a downstream consumer cares about 10-min AOD — most PM₂.₅ workflows do not. Option (b) is adopted here: the DNN's forward/backward formulation fills missing daily AOD using AOD(t − 1 day) and AOD(t + 1 day) plus daily meteorology and Candidate 1's SIM predictions, with the same lead-time-weighted average. Effectively, Candidate 2 takes Candidate 1's RF predictions as the SIM backbone and adds a "temporal smoother + bias-corrector" DNN on top — exactly the SIM → TIM chaining Chen 2023 describes, restated at daily cadence.

**DNN architecture (option-b form).** A small feed-forward network: input layer of ≈ 30 features comprising
- the 12 Candidate-1 (SIM) predictors at day t,
- AOD(t − 1) and AOD(t + 1) — observed if available, otherwise Candidate 1's SIM prediction — each carrying a NaN-flag indicator,
- per-variable meteorological deltas Δmet(t−1 → t) and Δmet(t → t+1) for the top-importance ERA5 variables from §7.8.1 (BLH, RH, U10, V10, TP).

Three hidden layers of {64, 32, 16} ReLU neurons with dropout = 0.2; output = single scalar (AOD(t)). Adam optimizer, MAE loss (Chen 2023 §2.4.3), Keras-tuner search over hidden-layer widths and dropout. 10-fold temporal-block CV per §8.2.1 (and Chen 2023 §2.4.3). Because the DNN inputs include lagged AOD that crosses fold boundaries, the cross-boundary leakage allowance in §8.2.1 ("Fold construction") applies specifically to this candidate.

**Output blend.** Forward and backward DNN predictions are combined by Chen 2023 Eq. 1: AOD_wt(t) = (AOD_FW(t) · t_back + AOD_BW(t) · t_fwd) / (t_back + t_fwd), where t_fwd is the lag to the next valid AOD day and t_back is the lag to the previous valid AOD day. When both flanks are present the blend gives a balanced estimate; when only one flank is observable within ±N days (N = 7 default), only that side contributes; when neither flank is within N days, Candidate 2 falls back to Candidate 1's SIM prediction without any TIM refinement.

**Why it might lose to Candidate 1.** In the cloud-prone monsoon, "AOD(t − 1 day)" is often *itself* a Candidate 1 SIM output, not a real observation. Chaining the TIM DNN on top of SIM outputs risks compounding errors rather than reducing them. The empirical question — whether the temporal smoothing buys more than the chained-error penalty costs — is exactly what §8.2.2 settles. The §8.2.4 residual-envelope check is the key diagnostic: if Candidate 2's residual envelope at AOD > 1.5 is wider than Candidate 1's, the TIM chaining hurt rather than helped, and Candidate 1's pure-SIM design wins.

#### 7.8.3 Common output schema for both candidates

Regardless of which candidate becomes primary, every Stage B daily cell carries:

| Field | Values |
| --- | --- |
| `aod_daily` | Final daily AOD (observed, kriged, or ML-filled) |
| `gap_fill_method` | `observed` / `kriging` / `kriging_climatology` / `ml_rf` / `ml_dnn` |
| `days_since_last_observed` | Integer; 0 for `observed`, ≥ 1 otherwise |
| `confidence` | RF: out-of-bag standard error; DNN: ensemble or MC-dropout standard error; kriging: kriging variance; observed: §7.5 per-day weight sum |
| `aod_kriging_baseline` | The §7.7 kriging value always co-shipped, so the user can opt out of ML |

The `confidence` field is what enables the §8.2.3 `days_since_last_observed`-stratified RMSE panel and the §8.2.5 case-study coherence check. The `aod_kriging_baseline` shipped alongside `aod_daily` is what makes the §11 scope-management line "the product is still publishable with Step B2 alone" operational rather than aspirational.

#### 7.8.4 Selection criterion (cross-reference to §8.2)

The candidate whose held-out AERONET RMSE on filled-only matchups, averaged across both stations and both seasons (§8.2.2), is the lower becomes the primary `ml_*` value in `aod_daily`. The losing candidate is still computed and shipped as an auxiliary field (`aod_ml_rf_alt` or `aod_ml_dnn_alt`, whichever was not selected) for downstream users who want to ensemble or compare. The kriging baseline is always present. The user always has the choice of whether to trust the ML or fall back to interpolation — provenance is sufficient information for that choice.

---

### Cross-reference to the rest of the plan

- §5.2 finding 6 — the 10.3% raw AOD availability — sets why Stage B is the dominant component of the daily product, not auxiliary.
- §7.5's TC-weighted fusion is what Stage B inherits as input — Candidate 1's contemporaneous predictors (it has no lagged-AOD channel) and Candidate 2's lagged AOD inputs both see fewer / lower-weighted wet-season Himawari pixels as a consequence of how σ²_TC varies by season. This is documented in §10 caveat 5.
- §8.2.1 candidate-CV protocols, §8.2.2 AERONET-blind validation, §8.2.3 coverage/provenance audit, §8.2.4 robustness diagnostics, and §8.2.5 case-study stress test all consume the §7.8.3 output schema unchanged.
- §10 caveat 8 (gap-filled values are model estimates, flag distinctly) and caveat 9 (ML generalizes only as far as training data) are unchanged.
- §11 scope-management: Step B2 must complete; Candidate 1 is the high-priority stretch; Candidate 2 is stretch / future work.

---

## 8. Validation Strategy

The pipeline produces two end-user products: the **Stage A 30-min merged AOD** (§7.5) and the **Stage B daily gap-filled AOD** (§7.6–§7.8). Validation is organised one block per product, mirroring the structure of the predecessor literature: Ahn 2021 and Gupta 2024 validate their merged products as single artefacts against AERONET; Chen 2023 and Youn 2024 each validate their gap-fill products and run an explicit method comparison to justify their primary candidate. §8.1 below answers *is the merged product good?* and §8.2 answers both *is the gap-filled product good?* and *which gap-fill candidate wins?*.

### 8.0 Shared validation protocol

**Temporal split (used by both stages).** A single immutable partition:

- **Training window:** Sep 2022 – Dec 2024 (~2.3 years). All soft calibration (§7.4.1), the TC σ² table (§7.4.2), and the gap-fill model fitting (§7.8) are built exclusively from this window.
- **Held-out validation window:** Jan 2025 – Apr 2026 (~16 months). Never touched by any training step. Used for the headline performance numbers reported below.

The split is temporal rather than station-based because the domain has only two AERONET stations — both must participate in validation, so the held-out window is the only mechanism that prevents leakage. Regional generalisation in central Vietnam (no AERONET) is validated indirectly via the inter-sensor consistency check (§8.1.3) and the case studies (§8.1.5, §8.1.6, §8.2.5).

**Metric panel (used by both stages).** Following Ahn 2021 Table 3, Gupta 2024 Table 3, and Nguyen 2025 Tables 2–3, every AERONET comparison reports:

- `N` — number of matched satellite–AERONET pairs.
- `R`, `R²` — Pearson correlation and coefficient of determination.
- `RMSE`, `MAE`, `Bias` (mean satellite − AERONET).
- `slope`, `intercept` of best-fit (AERONET on satellite).
- `%EE` — fraction of pairs inside the Dark-Target / Deep-Blue expected-error envelope `EE = ±(0.05 + 0.15 × AOD_AERONET)` (Gupta 2024; Sayer 2020 for VIIRS DB).

The panel is computed per station, per season (Dry: Oct–Apr / Wet: May–Sep), and per confidence-flag tier (1–4 from §7.5) where applicable.

**AERONET colocation (used by both stages).** Spatial: the 0.05° cell containing the AERONET station (consistent across sensors after §7.2 box-averaging). Temporal: AERONET observations averaged within ±30 min of the slot centre for Stage A, and across the local day for Stage B (matching Nguyen 2025 §2.3.1). AERONET is interpolated from 500 / 675 nm to 550 nm using the site-specific Ångström exponent before any comparison.

---

### 8.1 Stage A validation — is the merged 30-min product good?

The Stage A product is a 30-min merged AOD at 0.05° resolution, with per-cell `AOD_merged`, `AOD_std`, `n_sensors`, `dominant_sensor`, and `confidence_flag` fields. Validation answers: does the merged product agree with AERONET better than any single sensor? Does it agree with itself across sensors? And does it beat the published predecessor merges?

This is the same validation strategy Ahn 2021 used for their NE Asia composite (§3.2 of that paper) and Gupta 2024 used for their LEO–GEO merged product (§4.3 of that paper), adapted to Vietnam's two-station AERONET constraint.

#### 8.1.1 Held-out AERONET validation (the headline test)

For each 30-min slot in the Jan 2025 – Apr 2026 held-out window, match `AOD_merged` at the AERONET cell with AERONET observations within ±30 min of the slot centre. Report the full metric panel:

- **Per station.** Density scatter plot at Nghia Do and Bac Lieu separately (Gupta 2024 Fig. 10 analogue). The merged scatter should fall inside the envelope of the best individual LEO sensor at each station (Ahn 2021 §3.3.3: "the composite resembled the MODIS/VIIRS accuracy").
- **Per season.** Dry (Oct–Apr) vs Wet (May–Sep). The wet-season metrics are the critical test of whether σ²_TC (§7.4.2) captures Himawari's wet-season error structure: if wet-season R collapses below 0.50 at either station, the wet-season σ²_TC is under-estimated and Himawari is being weighted too heavily by §7.5 (§10 #13).
- **Per confidence flag.** Separate metric panel for flag = 1 (Himawari only), 2 (LEO only), 3 (Himawari + LEO), 4 (multi-LEO + Himawari). The expected pattern is monotonic improvement R(1) < R(2) ≤ R(3) ≤ R(4). A non-monotonic ordering means the confidence flag is mislabelling cells and the §7.5 inclusion rules need review.

**Pass criteria (linked to §9 targets):**

- Nghia Do (high-AOD urban): R ≥ 0.90, RMSE ≤ 0.30 averaged across confidence flags ≥ 2.
- Bac Lieu (low-AOD maritime): R ≥ 0.85, RMSE ≤ 0.20 averaged across confidence flags ≥ 2.
- Bias must lie inside [−0.05, +0.05] at both stations in dry season — confirming the §7.4 soft calibration removed the §5.1 baseline biases (−0.117 at Nghia Do, +0.085 VIIRS at Bac Lieu).

#### 8.1.2 Pre/post-correction comparison (was the bias correction worth it?)

For each (sensor, region, season) stratum used by §7.4.1, report `RMSE_before` and `RMSE_after` from k=5-fold CV on the training window, plus the held-out RMSE. This is the Ahn 2021 Table 3 analogue — they reported pre/post-CDF RMSE per AERONET site per sensor, showing systematic improvement.

The thesis version reports:

- `RMSE_before`, `RMSE_after_CV`, `RMSE_after_heldout` per stratum.
- The fitted `(α, β)` per §7.4.1 and the CV-time route (`'apply'` or `'none'` per the guard rail in §7.4.1 step 3), with the diagnostics that drove the route (CV α, CV |β|).
- Diagnostic scatter plots per stratum: satellite-vs-MERRA-2 (training scatter with fitted line, §7.4.1) and pre/post-correction satellite-vs-AERONET on the training window, to show whether the soft calibration improved the AERONET fit at the two AERONET cells.

**Pass criterion per stratum:** `RMSE_after ≤ RMSE_before − 0.02`, or the stratum is routed to `'none'` with the §7.5 penalty weight. The held-out RMSE must lie within 1.5 × the CV RMSE (a train-vs-validation consistency criterion); a held-out RMSE > 2 × CV RMSE indicates overfitting and routes the stratum to `'none'`.

#### 8.1.3 Inter-sensor consistency (internal uncertainty)

Where two or more sensors observe the same cell in the same 30-min slot, the across-sensor spread is an *internal* uncertainty estimate that does not require AERONET. Report inter-sensor R² per region (north / central / south) for each sensor pair (MODIS–Himawari, VIIRS–Himawari, MODIS–VIIRS), before and after the §7.4 soft calibration.

The baseline from Nguyen 2025 §3.1.4:

- MODIS–Himawari R² = 0.621 (N), 0.474 (C), 0.756 (S).
- VIIRS–Himawari R² = 0.450 (C), 0.756 (S) (north pair not separately reported).

**Pass criterion:** post-correction inter-sensor R² lifts by ≥ +0.10 in every region for at least one sensor pair. A lift means the soft calibration has shrunk the systematic disagreement between sensors. No lift means the corrections are not making the sensors agree more — i.e., fusion is averaging incompatible numbers and the §7.5 weighting cannot rescue it.

This is also the *only* AERONET-comparable validation pathway for the central Vietnam region, which has no AERONET. The central inter-sensor R² is the proxy ground truth there. If it remains below 0.55 after correction, the §7.4.1 MERRA-2 soft calibration is flagged as not delivering for central strata (§10 #12), and the §9 deliverable carries an explicit central-Vietnam uncertainty note.

#### 8.1.4 Comparison against baselines

Four published or rebuildable baselines, each validated on the same held-out window with the same metric panel at both AERONET stations:

- **B1 — best-single-sensor.** Bias-corrected VIIRS-only product (the §5.1 baseline reports VIIRS as the most accurate L2 product at both stations: R = 0.915 at Nghia Do, R = 0.845 at Bac Lieu). The simplest competitor — if the thesis product cannot beat single-sensor VIIRS, it is not adding value.
- **B2 — Gupta 2024 equal-weight DT merge.** The same input grids fused with equal weights and no §7.4 CDF (Gupta 2024's actual method, resampled from 0.25° to 0.05° for fair comparison). The Gupta 2024 global reference (N = 272 725, %EE = 65.45, Bias = 0.051, RMSE = 0.147, R = 0.83, slope = 1.10, intercept = 0.020) is quoted alongside.
- **B3 — Ahn 2021 NE Asia composite.** Where Ahn's published product overlaps Vietnam temporally and spatially, compare directly. The geographical overlap is thin — Nghia Do is at the southern edge of Ahn's domain; Bac Lieu falls outside — so this is a stretch baseline (§11).
- **B4 — Nguyen 2025 daily Himawari RANSAC.** R² = 0.293 vs PM₂.₅. The thesis daily product (aggregated from `AOD_merged`) is fed into the same downstream regression structure and compared.

**Claim being tested:** the thesis merged product beats all four baselines on AERONET-validated R, RMSE, %EE at *at least one* AERONET station for *at least one* season. The regimes where it does not (e.g. low-AOD wet-season Bac Lieu, where single-sensor VIIRS may already be near-optimal) are identified.

#### 8.1.5 Precipitation-aware validation

Per Nguyen 2025, AOD–PM₂.₅ coupling depends sharply on hours-since-last-rain (north r = +0.301, central r = +0.116, south r = +0.047). The merged product is validated separately for:

- **Dry intervals** (> 24 h since last GPM IMERG ≥ 0.1 mm hr⁻¹ event): expected best coupling.
- **Post-rain intervals** (0–12 h): wet scavenging active.
- **Recovery intervals** (12–24 h): partial accumulation.

The pass criterion is that the merged product retains R ≥ 0.85 in dry intervals at both stations against AERONET. A wet-interval R that collapses below 0.50 signals that the wet-season σ²_TC for Himawari (§7.4.2) is under-estimated and Himawari is being over-weighted by §7.5 (§10 #13). The wet-interval / dry-interval R ratio is reported per station and per season.

#### 8.1.6 Case-study confirmation

Three documented Vietnam aerosol events from the held-out window confirm the Stage A product's behaviour on real episodes:

1. **A severe Hanoi dry-season haze episode** (AOD > 1.5, PM₂.₅ > 100 µg m⁻³). Tests whether the §7.4 soft calibration retains high-AOD events in the high-VZA north (the §1 finding 1 concern).
2. **A March–April biomass-burning transport event from mainland SEA.** Tests spatial coherence across the regional-mask boundary at 16°N — the merged AOD field should show a smooth gradient consistent with HYSPLIT 72-hr back-trajectories (Ahn 2021 Fig. 6 protocol).
3. **A precipitation washout event.** Tests that the wet-flag correctly separates coupled vs decoupled AOD–PM₂.₅ periods — i.e. the merged AOD (column) should *not* respond to surface wet scavenging the way PM₂.₅ (surface) does. This is the negative-control case.

Per event: qualitative comparison of the merged 30-min maps against MERRA-2 reanalysis AOD and NASA Worldview true-colour imagery; Spearman rank correlation between the merged-AOD time series at each Envisoft station and the corresponding PM₂.₅ time series; HYSPLIT overlay for case 2.

The held-out-window Envisoft completeness threshold is relaxed to ≥ 50% (§4.3, §10 #16); each station's completeness is reported alongside.

---

### 8.2 Stage B validation — gap-fill model comparison and final product assessment

Stage B produces a daily gap-filled AOD at 0.05° using the §7.7 kriging baseline as default plus one of two ML candidates: §7.8 candidate 1 (Random Forest, primary) and §7.8 candidate 2 (RF + DNN, Chen 2023 style, if time allows). Validation answers two intertwined questions: *which candidate becomes the primary product?* and *how good is that primary product against AERONET?*.

The protocol is built directly from Chen 2023 §2.4 + §3.2 and Youn 2024 §2.2.3 + §3.1. Each of those papers ran an internal cross-validation on their training partition, then validated the result against AERONET-blind matchups (cells where AERONET had data but the satellite had none, now filled by the model), then compared their primary against published alternatives. The thesis does all three plus the per-candidate comparison.

#### 8.2.1 Per-candidate internal cross-validation

Each candidate is held to a candidate-appropriate CV protocol on the training partition (Sep 2022 – Dec 2024), with the held-out window untouched until the candidate is finalised:

| Candidate | CV protocol | Source |
| --- | --- | --- |
| §7.7 kriging baseline | Leave-one-cell-out artificial-gap test on days with > 50% valid coverage | Standard geostatistical practice |
| §7.8 candidate 1 (RF) | 80/20 train/internal-test + 5-fold **temporal-block** CV inside the 80% (folds are five contiguous time periods of the train window, ≈ 5.6 months each) + OOB R² as a free diagnostic | Youn 2024 §2.2.3, Chen 2023 §2.4.2 |
| §7.8 candidate 2 (RF + DNN) | RF as above; DNN trained with 10-fold **temporal-block** CV (ten contiguous time periods, ≈ 2.8 months each) | Chen 2023 §2.4.3 |

Each candidate reports `RMSE_train`, `RMSE_CV`, `RMSE_internal_test` plus the OOB number for the RF. The three RMSE values must agree within ±15% (a train-vs-validation consistency check; a blind-test RMSE > 1.3 × CV RMSE indicates overfitting). A candidate that fails this consistency check is rejected before it ever sees the held-out AERONET window.

**Fold construction (applies to both candidates' CV).** Folds are *contiguous calendar-time blocks* over the Sep 2022 – Dec 2024 training partition: for Candidate 1 (RF), five blocks of ≈ 5.6 months; for Candidate 2 (DNN), ten blocks of ≈ 2.8 months. Random k-fold is rejected because AOD has strong spatial *and* temporal autocorrelation — neighbouring cells on the same day are near-identical, so random folds would put near-twins in train and val and overstate generalisation. Temporal blocking forces each fold's validation set to be a held-out time period (often a held-out season), so CV RMSE is an honest predictor of held-out RMSE in §8.2.2 rather than a memorisation metric. **Cross-fold leakage in Candidate 2's TIM inputs.** Candidate 1 (the spatial RF) uses only contemporaneous predictors, so its folds are clean. Candidate 2's DNN, by design, ingests AOD(t − 1) and AOD(t + 1); the validation block's first and last days therefore draw lagged AOD from the adjacent training block. This is intentional: at production time t±1 is always observable history (or its SIM imputation), and zeroing the flanking AOD near fold edges would distort the training input distribution away from production. The 80/20 internal-test split inside the training partition is also taken temporally (last 20% of the train window, ≈ 5.6 months), so the internal-test set mirrors the §8.0 held-out window in construction and the temporal consistency criterion above is meaningful.

#### 8.2.2 AERONET-blind validation (the headline comparison)

The classic gap-fill test, used by Chen 2023 (Fig. 5: gap-filled AOD excluding the original AHI input) and Youn 2024 (Fig. 7: AERONET density scatter of filled-only values).

The setup:

1. Identify days in the held-out window where AERONET has valid data but the §7.6 daily product at the AERONET cell was *missing* in the raw merged input (i.e. all sensors saw nothing — cloud cover the dominant cause).
2. Run each candidate to fill that cell.
3. Compare the filled value against AERONET.

Report the full metric panel per candidate, per station, per season. Specifically:

- The per-candidate RMSE on filled-only matchups must lie within 1.3 × the §8.1.1 fused-product RMSE on observed matchups. Beyond 1.3× the fill is meaningfully worse than an actual observation; below 1.0× (rare but possible) the fill genuinely competes with direct retrieval.
- Per-season stratification matters because the wet-season filled-cell fraction is the dominant case (per §5.2 finding 6, ~90% of slots are missing in monsoon). A candidate that performs well in dry but collapses in wet is not the primary.

This block is what determines which candidate becomes primary. The candidate with the **lowest held-out AERONET RMSE on filled-only matchups, averaged across both stations and both seasons**, wins.

#### 8.2.3 Spatial coverage and provenance audit

The gap-fill's reason for existing is spatial coverage. Report:

- **Daily coverage fraction over Vietnam.** Pre-fill (the §7.6 baseline daily product) vs post-fill for each candidate, by month over the full study period. Per the §9 target, the gap-filled product must reach ≥ 95% daily coverage. The baseline is 10.3% (Himawari L2, raw, per §5.2 finding 6).
- **Provenance histogram.** Every gap-filled cell carries `gap_fill_method ∈ {observed, kriging, ml_rf, ml_dnn}` per §7.8. Report the fraction of cells in each category per month and per region. Months and regions dominated by `kriging` or `ml_*` rather than `observed` are flagged in §9 as having an essentially-modelled product.
- **`days_since_last_observed` distribution.** Per cell, the number of days since that cell had a real observation. Stratify the §8.2.2 AERONET-blind metric panel by this distance: bins {0, 1, 2, 3–5, 6–10, > 10 days}. The expected pattern is RMSE rises with `days_since_last_observed`. The bin where the ML RMSE exceeds the §7.7 climatology-fill RMSE is the empirical horizon beyond which the ML fill is no better than climatology — this horizon is reported in §9 as a hard cap on the ML product.

#### 8.2.4 Model-diagnostic checks (robustness, not headline)

These are not pass/fail tests; they tell users when to trust the chosen candidate.

- **Variable importance for the RF candidate (Youn 2024 Fig. 8 analogue).** Normalised Gini importance for the 12+ predictors in §7.8. CAMS AOD and ERA5 dew-point temperature are expected to dominate (Youn 2024 reported 27.4% and 8.66% respectively). A different ordering in Vietnam is a finding, not a failure; but if a clearly irrelevant variable (e.g. snow depth — meaningless on this domain) appears in the top three, the predictor selection is reviewed.
- **Reanalysis-substitution sanity (CAMS vs MERRA-2).** Chen 2023 and Youn 2024 both used CAMS as their primary reanalysis predictor; the thesis follows them. Re-run the primary candidate with MERRA-2 swapped in place of CAMS. The two runs must agree on held-out RMSE within ±0.02 — agreement means the gap-fill is combining many predictors, not slaved to one reanalysis. Disagreement means the predictor pool is too thin and the result is reanalysis-dependent.
- **Residual envelope vs AERONET AOD.** For the primary candidate, plot mean residual ± 1σ as a function of AERONET AOD, binned at 0.1 AOD intervals. The expected pattern is low residual for AOD ≤ 0.5 and growing bias + σ at AOD > 1.5 (a robust finding across published gap-fill literature for both reanalysis-anchored and satellite-anchored products). If the model shows pathological behaviour at low AOD at Bac Lieu (mean AOD = 0.212), the training distribution is rebalanced.
- **Daytime variation against AERONET (Chen 2023 Fig. 6 analogue).** Hourly mean of gap-filled AOD vs AERONET hourly mean over the held-out window, per station. The diurnal shape should track AERONET (Chen 2023 reported the gap-fill follows AERONET's daytime curve in YRD and PRD with R > 0.6 across most hours).

#### 8.2.5 Case-study stress test (cloud-occluded periods)

The §8.1.6 case studies test the merged product on observable events. Stage B requires a complementary test on *unobservable* days — multi-day cloud cover where the gap-fill is doing essentially all the work.

The selected case is a **monsoon-season multi-day cloud period** (5–10 consecutive days of near-total cloud cover, drawn from July–August 2025 in the held-out window). The validation:

- Visual coherence: the gap-filled daily maps must retain plausible spatial structure across consecutive days — not collapse to a flat climatology, not show day-to-day discontinuities at the boundary of cloud edges.
- Qualitative consistency with MERRA-2 reanalysis AOD over the same period — the gap-fill is allowed to disagree with MERRA-2 in magnitude (its CAMS/MERRA-2 covariates were not the only input) but the spatial pattern should be broadly consistent.
- Recovery check: when satellite retrievals resume after the cloud break, the merged value at the recovery cell should align with the gap-filled value of the day before within 1.5 × the §8.2.2 filled-cell RMSE. A large discontinuity means the gap-fill had drifted away from physical reality during the occluded period.

In this stress test the satellite sees nothing for days and the gap-fill carries the entire estimate; the test verifies whether the model produces a coherent spatial pattern in that regime, rather than collapsing to climatology or producing visible discontinuities at recovery.

#### 8.2.6 Comparison against §9 success criteria

The §9 quantitative-target table is reproduced for the gap-filled product, with achieved values filled in:

| Metric | Baseline | Target | Achieved |
| --- | --- | --- | --- |
| AERONET R at Nghia Do (full daily coverage) | 0.915 (VIIRS only, observed cells) | ≥ 0.90 | (TBC) |
| AERONET R at Bac Lieu (full daily coverage) | 0.845 (VIIRS only, observed cells) | ≥ 0.85 | (TBC) |
| AERONET RMSE Nghia Do | 0.271 (VIIRS only) | ≤ 0.30 | (TBC) |
| Daily AOD spatial coverage over Vietnam | 10.3% (Himawari L2, raw) | ≥ 95% | (TBC) |
| Daily AOD–PM₂.₅ R² (all stations) | 0.293 (Himawari RANSAC, Nguyen 2025) | ≥ 0.35 | (TBC) |
| Daily AOD–PM₂.₅ R² (MODIS-equivalent ceiling) | 0.573 (MODIS RANSAC) | — (ceiling reference) | (TBC) |

Following Nguyen 2025, the daily AOD–PM₂.₅ R² is reported with both OLS and RANSAC robust regression (residual threshold = 1.5 × MAD, 50% minimum sample, 1000 trials). The merged product is *not* RANSAC-filtered itself — that would discard real high-AOD events. The OLS-vs-RANSAC R² lift is a downstream diagnostic for PM₂.₅ users, not a product step.

Failure on any line is not automatic project failure — it is an entry in §10 with a documented cause traceable to one of the diagnostic outputs of §8.1 or §8.2.

---

## 9. Expected Outcomes and Deliverables

1. **Vietnam 30-min merged AOD dataset** at 0.05°, Sep 2022 – Apr 2026, NetCDF, with provenance flags. Archived publicly.
2. **Vietnam daily gap-filled AOD dataset** at 0.05°, NetCDF, with gap-fill method flags and days-since-observation.
3. **Bias-correction lookup tables** (linear soft-calibration coefficients α, β per sensor × region × season × product-level, plus the triple-collocation σ² table), distributed as supplementary data, reusable for future studies.
4. **Methodological comparison report** of up to three gap-filling strategies (kriging, RF, RF + DNN if completed) on the same domain — the first such comparison for tropical Southeast Asia.
5. **Validation against Gupta 2024, Ahn 2021, and best-single-sensor baselines.**
6. **Recommendation document** for the downstream PM2.5 mapping project: which AOD layer to feed in by region and by season, and where uncertainty is highest.

**Quantitative targets** (success criteria the thesis commits to):

| Metric                                                    | Baseline (best individual sensor / Gupta 2024) | Thesis target                               |
| --------------------------------------------------------- | ---------------------------------------------- | ------------------------------------------- |
| AERONET R at Nghia Do                                     | 0.915 (VIIRS only)                             | ≥ 0.90 _with full daily coverage_           |
| AERONET R at Bac Lieu                                     | 0.845 (VIIRS only)                             | ≥ 0.85 _with full daily coverage_           |
| AERONET RMSE Nghia Do                                     | 0.271 (VIIRS only)                             | ≤ 0.30 averaged across all confidence flags |
| Daily AOD spatial coverage over Vietnam                   | 10.3% (Himawari L2, raw)                       | ≥ 95% (gap-filled product)                  |
| Daily AOD–PM2.5 R² (validation, all stations)             | 0.293 (Himawari RANSAC, Nguyen 2025)           | ≥ 0.35                                      |
| Daily AOD–PM2.5 R² (validation, MODIS-equivalent ceiling) | 0.573 (MODIS RANSAC)                           | — (ceiling reference)                       |

---

## 10. Limitations and Caveats

1. **AOD availability is fundamentally ~10% before gap-filling.** Gap-filled values will dominate ~90% of the daily product for most of the year and must be flagged distinctly from observed values throughout the dataset.
2. **Two AERONET sites only.** Central Vietnam (11.5°N ≤ lat < 16°N) has no in-situ AERONET ground truth at all. Bias correction for central cells is provided by the MERRA-2 anchor (§7.4.1), which is spatially complete, but there is no AERONET pathway to validate central-Vietnam performance directly — only the inter-sensor consistency check of §8.1.3 and the case studies of §8.1.6 / §8.2.5. Establishing an AERONET station near Đà Nẵng or Huế remains the single highest-priority external follow-up.
3. **Himawari high VZA over northern Vietnam** elongates the atmospheric path and inflates pixel footprints by 2–5×. Bias correction reduces but does not eliminate this geometric limitation.
4. **MAIAC fails at Bac Lieu** (R = 0.411). The south is therefore essentially served by Himawari + VIIRS, with MAIAC contributing minimal weight.
5. **Wet season in the north is unconstrained by LEO** (May–Sep). The merged product during these months depends on bias-corrected Himawari plus gap-filling, neither of which is fully validatable when LEO data is scarce — and the wet season is precisely when AERONET coverage is also thinnest. Compounds with #13: a north-wet cell with no LEO present is filled by Himawari at whatever σ²_TC its (north, wet) stratum receives from §7.4.2; if that σ²_TC is under-estimated, no second mechanism catches it.
6. **Spatially uniform hygroscopic exponent γ = 0.6.** Marine aerosols in the south are more hygroscopic; industrial aerosols in the north are less so. Per-region γ tuning is future work.
7. **PBLH unavailable at hourly resolution for ~25% of the study period**; replaced by monthly climatology over Jan–Jun 2024. Affects physics-correction quality for that subset.
8. **Gap-filled values are model estimates**, not observations. They must be flagged in every output and should not be used for trend analysis or extreme-value statistics without an explicit "observed-only" filter.
9. **ML gap-filling generalizes only as far as the training data.** If a regime (e.g., a once-per-decade biomass burning event of unusual intensity) is absent from training, predictions there are extrapolations.
10. **Spectral harmonization is dropped.** Justified empirically (Nguyen 2025), but means the product is technically a mix of 500 nm Himawari and 550 nm LEO. The error this introduces is bounded at ~ΔR² of 0.003 and ΔRMSE of 0.001.
11. **MODIS MAIAC bitmask filtering rejects ~ "Poor"-and-worse pixels.** The §7.1 bitmask gate (bits 8–11 ≤ 4) costs a modest amount of coverage relative to a bitmask-off filter; the trade-off favours accuracy, justified by the MAIAC-north RMSE behaviour documented in §7.1.
12. **MERRA-2 as the bias-correction anchor brings its own structural smoothness.** MERRA-2's native 0.5° × 0.625° resolution is coarser than the 0.05° Vietnam grid, so the soft calibration (§7.4.1) learns the *seasonal-regional mean* shift rather than fine-scale spatial structure. Cells whose local AOD pattern departs from the MERRA-2 regional mean — particularly small-scale urban or burn plumes — will be under-corrected. The inter-sensor consistency check (§8.1.3) is the only diagnostic that catches this for central Vietnam, where there is no AERONET.
13. **Wet-season Himawari is weighted automatically by σ²_TC (§7.4.2); no hand-set down-weight applies.** The wet-season failure mode is a soft-calibration stratum whose CV-time (α, β) look acceptable but whose held-out σ²_TC is under-estimated — which would let §7.5 over-weight Himawari in fusion. The §8.1.1 wet/dry R ratio at each AERONET station and the §8.1.5 precipitation-aware metrics are the only safety net for this case.
14. **Physics correction uses ERA5 RH/PBLH only.** In-situ RH from surface stations (mentioned in §4.4) is not yet incorporated into the production pipeline.
15. **Soft-calibration guard rails can route whole strata to `'none'`.** Strata that fail the §7.4.1 CV-time guard rail (α outside [0.5, 2.0] or |β| > 0.2) enter fusion uncorrected with a 2× penalty on σ²_TC (so the inverse-variance weight drops by ~4×). For very low-coverage strata this means the satellite contributes with a deliberately demoted weight rather than a corrected estimate. Trade-off favours robustness over universal correction.
16. **§8.1.6 case-study Envisoft correlations use a relaxed station-completeness threshold (≥50%) over the held-out window.** No Envisoft station meets the ≥85% completeness bar across Jan 2025 – Apr 2026 alone. Case-study correlations therefore include stations whose held-out-window coverage is between 50% and 85% — flagged in the §8.1.6 tables so they are not mistaken for full-record statistics. The headline 27-station figure in §4.3 still refers to the ≥85% subset over the full Nguyen 2025 study window.
17. **Himawari L2 and L3 are merged per-pixel before fusion, not weighted as separate sources.** The per-pixel choice is stratum-aware: for each (region, season) the level with the lower σ²_TC (§7.4.2) supplies the primary pixel value and the other level fills its gaps. The §5.2 evidence motivating per-region level preference is therefore exploited in the *merge* step (the σ²_TC table favours L3 in the low-AOD south, L2 in the high-AOD north), but the merged grid still enters fusion with a single Himawari row carrying `min(σ²_TC_L2, σ²_TC_L3)`. The merged product does not currently carry a per-pixel L2-vs-L3 provenance flag, so downstream users cannot distinguish primary-from-fallback cells. Whether a separate-source approach (treating L2 and L3 as two independent fusion sources) would have outperformed the stratum-aware merge on the held-out window is left as future work.
18. **VIIRS overpass may contribute to two consecutive 30-min slots.** Because VIIRS granules are matched to any slot whose ±30-min window contains the granule's timestamp, a granule near a slot boundary (e.g., at 10:22 UTC) satisfies the window for both the 10:00 and 10:30 slots and its pixels are gridded into both. At most one or two slot pairs per day per sensor are affected, but the duplicated pixels introduce a mild positive temporal autocorrelation between consecutive slots near the overpass time. Eliminating this would require snapping each granule to its nearest slot, which would leave slot pairs near the boundary systematically thinner (fewer granules per slot); the current inclusive-window approach favours coverage over strict temporal independence.
19. **MODIS no-orbit-timestamp days are dropped.** When a MAIAC HDF lacks a readable orbit-timestamp attribute, the day's pooled retrieval is not used. The §7.2 Stage A2 intermediate has no concept of a synthesised slot time, so those days skip MAIAC entirely in both calibration and production. Empirically this affects < 1% of MAIAC days in the Vietnam record, so the impact is small; the daily-mean broadcast fallback used by earlier drafts is not retained.

---

## 11. Scope Management — What is in / out / stretch

**In scope (must complete):**

- Steps A1–A5 (full Stage A: filtering, gridding, physics correction, bias correction, fusion).
- Steps B1 + B2 (daily aggregation + kriging spatial gap-fill).
- §8.0, §8.1, §8.2 validation.
- §8.1.4 comparison vs Gupta 2024 (B2), best-single-sensor (B1), and Nguyen 2025 daily Himawari RANSAC (B4).

**Stretch (do if time permits):**

- Step B3 Candidate 1 (RF gap-filling) — high priority stretch.
- §8.1.4 comparison vs Ahn 2021.
- §8.1.6 PM2.5 case studies (3 of 4).

**Stretch / future work:**

- Step B3 Candidate 2 (RF + DNN, Chen 2023 style).
- Per-region γ tuning.
- Multi-year extension as more AERONET data arrives.

If the timeline shrinks, Step B3 is the first thing to drop. The product is still publishable with Step B2 alone, since spatial kriging is the standard fallback in the literature.

---

## 12. Proposed Timeline

| Phase                              | Tasks                                                         | Duration |
| ---------------------------------- | ------------------------------------------------------------- | -------- |
| 1. Data preparation                | Download & organize L2 products; AERONET re-processing        | 4 weeks  |
| 2. Baseline validation extension   | Reproduce/extend per-sensor validation; add SNPP VIIRS        | 3 weeks  |
| 3. Gridding pipeline               | 0.05° box-averaging code for all sensors                      | 3 weeks  |
| 4. Bias correction                 | Soft calibration vs MERRA-2 + TC σ² table                       | 5 weeks  |
| 5. Fusion (Stage A)                | Inverse-RMSE merge; produce 30-min files for full period      | 3 weeks  |
| 6a. Spatial gap-fill (Step B2)     | Kriging + climatological fill                                 | 2 weeks  |
| 6b. ML gap-fill (Step B3, stretch) | RF + reanalysis covariates                                    | 4 weeks  |
| 7. Validation                      | Held-out AERONET, internal, precipitation-aware, case studies | 4 weeks  |
| 8. Writing                         | Thesis document                                               | 6 weeks  |

Total: ~34 weeks (with stretch). ~30 weeks without ML gap-filling.

---

## 13. Connection to Downstream PM2.5 Project

This thesis produces the AOD input layer. The PM2.5 project will:

- Use 27 Envisoft PM2.5 monitoring stations (10/8/9 north/central/south) as PM2.5 ground truth.
- Apply physics-based correction (RH, PBLH) plus ML (LightGBM / RF / DL) on top of the thesis's AOD layer.
- Use ERA5 meteorology + GPM IMERG precipitation as covariates.
- Deliver near-real-time PM2.5 maps by feeding live Himawari through the same bias-correction + gap-filling pipeline.

**Performance ceiling** (from Nguyen et al. 2025, daily RANSAC):

| Input                  | Daily R² | Notes                          |
| ---------------------- | -------- | ------------------------------ |
| Raw Himawari AOD       | 0.028    | Negligible                     |
| Corrected Himawari AOD | 0.293    | 10× improvement over raw       |
| Corrected MODIS AOD    | 0.573    | Benefits from midday BL mixing |

The merged product is expected to lie between the Himawari-only (0.293) and MODIS-only (0.573) daily numbers — capturing LEO accuracy when LEO is available, and continuous Himawari coverage when it is not. The near-real-time use case specifically depends on Himawari's 10-min cycle, so the Himawari bias correction in §7.4 is the _critical enabling component_ of the entire downstream operational system.

---

## References

- Ahn, S., Chung, S.-R., Oh, H.-J., Chung, C.-Y. (2021). Composite Aerosol Optical Depth Mapping over Northeast Asia from GEO-LEO Satellite Observations. _Remote Sensing_, 13, 1096. **[Primary methodological predecessor — CDF + IDW + ICW fusion]**
- Buchard, V., Randles, C. A., da Silva, A. M., Darmenov, A., Colarco, P. R., Govindaraju, R., et al. (2017). The MERRA-2 aerosol reanalysis, 1980 onward. Part II: Evaluation and case studies. _Journal of Climate_, 30, 6851–6872.
- Chen, A., Yang, J., He, Y., Yuan, Q., Li, Z., Zhu, L. (2023). High spatiotemporal resolution estimation of AOD from Himawari-8 using an ensemble machine learning gap-filling method. _Science of the Total Environment_, 857, 159673. **[RF + DNN gap-filling reference]**
- Ding, Y., Li, S., Xing, J., Yang, J., Dong, J., Hu, S., Teng, M., Ni, W., & Jiang, J. (2025). Global hourly seamless AOD through measurement-adjusted machine learning fusion of multi-satellite and reanalysis data. _GIScience & Remote Sensing_, 62(1), 2586203. **[Soft-calibration linear form against MERRA-2; §7.4.1]**
- Gruber, A., Su, C.-H., Zwieback, S., Crow, W., Dorigo, W., & Wagner, W. (2016). Recent advances in (soil moisture) triple collocation analysis. _International Journal of Applied Earth Observation and Geoinformation_, 45(B), 200–211.
- Gupta, P. et al. (2024). Increasing aerosol optical depth spatial and temporal availability by merging datasets from geostationary and sun-synchronous satellites. _Atmospheric Measurement Techniques_, 17, 5455–5476. **[Global LEO-GEO DT merged product; the Vietnam-coarse benchmark to beat]**
- Holben, B. N. et al. (1998). AERONET — A federated instrument network and data archive for aerosol characterization. _Remote Sensing of Environment_, 66, 1–16.
- Inness, A., Ades, M., Agustí-Panareda, A., Barré, J., Benedictow, A., Blechschmidt, A.-M., et al. (2019). The CAMS reanalysis of atmospheric composition. _Atmospheric Chemistry and Physics_, 19(6), 3515–3556.
- Kotchenruther, R. A. & Hobbs, P. V. (1998). Humidification factors of aerosols from biomass burning in Brazil. _Journal of Geophysical Research_, 103, 32081–32089.
- Levy, R. C., Mattoo, S., Munchak, L. A., Remer, L. A., Sayer, A. M., Patadia, F., & Hsu, N. C. (2013). The Collection 6 MODIS aerosol products over land and ocean. _Atmospheric Measurement Techniques_, 6, 2989–3034.
- Lyapustin, A. et al. (2018). MODIS Collection 6 MAIAC algorithm. _Atmospheric Measurement Techniques_, 11, 5741–5765.
- McColl, K. A., Vogelzang, J., Konings, A. G., Entekhabi, D., Piles, M., & Stoffelen, A. (2014). Extended triple collocation: Estimating errors and correlation coefficients with respect to an unknown target. _Geophysical Research Letters_, 41(17), 6229–6236.
- Nguyen, K. T., Trinh, A. H., Bui, C. K. (2025). Assessing the Feasibility of Estimating Air Quality in Vietnam Using Satellite Data. _Student Scientific Research Conference, Hanoi University of Science and Technology_. **[Team paper — empirical anchor for this thesis]**
- Randles, C. A., da Silva, A. M., Buchard, V., Colarco, P. R., Darmenov, A., Govindaraju, R., et al. (2017). The MERRA-2 aerosol reanalysis, 1980 onward. Part I: System description and data assimilation evaluation. _Journal of Climate_, 30, 6823–6850.
- Remer, L. A. et al. (2012). Retrieving aerosol in a cloudy environment. _Atmospheric Measurement Techniques_, 5, 1823–1840.
- Sawyer, V. et al. (2020). Continuing the MODIS Dark Target aerosol time series with VIIRS. _Remote Sensing_, 12, 308.
- Sayer, A. M., Hsu, N. C., Lee, J., Kim, W. V., & Dutcher, S. T. (2020). Validation, stability, and consistency of the VIIRS Deep Blue aerosol data record from 2012–2019. _Journal of Geophysical Research: Atmospheres_, 125, e2019JD031781. **[VIIRS Deep Blue reference]**
- Stoffelen, A. (1998). Toward the true near-surface wind speed: Error modeling and calibration using triple collocation. _Journal of Geophysical Research: Oceans_, 103(C4), 7755–7766. **[Triple-collocation foundation paper]**
- van Donkelaar, A. et al. (2010). Global estimates of ambient fine particulate matter concentrations from satellite-based aerosol optical depth. _Environmental Health Perspectives_, 118, 847–855.
- Yoshida, M. et al. (2018). Common retrieval of aerosol properties for imaging satellite sensors. _Journal of the Meteorological Society of Japan_, 96B, 193–209. **[Himawari/AHI retrieval algorithm]**
- Youn, Y., Kim, S., Kim, S. H., Lee, Y. (2024). Spatial Gap-Filling of Himawari-8 Hourly AOD Products Using Machine Learning with Model-Based AOD and Meteorological Data: A Focus on the Korean Peninsula. _Remote Sensing_, 16, 4400. **[Pure-RF gap-fill reference]**
---

_**Draft v3.4.0 — architectural pivot: reanalysis-anchored bias correction + AERONET-independent fusion weights via triple collocation.** Both calibration anchors move off AERONET, freeing the AERONET record entirely for held-out validation. Key changes vs v3.3.3:_

_**(P1) Bias correction anchored against MERRA-2 reanalysis (§7.4.1).** Per-AERONET-site CDFs and the v3.3 decision tree (KS / range_ratio / decile_coverage) are dropped. Each (sensor, region, season) stratum is now soft-calibrated by a linear `MERRA-2 = α · sat + β` fit (Ding et al. 2025 form) over the spatially complete reanalysis. A single CV-time guard rail routes strata with α outside [0.5, 2.0] or |β| > 0.2 to `'none'` with a 2× σ²_TC penalty at fusion. Central Vietnam becomes a first-class stratum because the MERRA-2 anchor exists there._

_**(P2) Fusion weights from triple collocation (§7.4.2, §7.5).** The post-correction RMSE table is replaced by `tc_error_variance.json`, populated from inter-sensor TC (Stoffelen 1998; McColl 2014; Gruber 2016). Triplets reject only shared-algorithm pairs (SNPP-DB + NOAA20-DB); MERRA-2 + MAIAC and Himawari L2 + L3 are allowed because the algorithms differ even though the input radiances overlap. A strict-vs-permissive σ² sensitivity check is reported as a robustness diagnostic._

_**(P3) Three legacy structures dissolved:** (a) the §7.4.2 LEO–Himawari spatial offset map is gone (central-Vietnam compensation comes from MERRA-2 instead); (b) the §7.4.4 chicken-and-egg bootstrap is gone (calibration runs linearly once); (c) the §7.4.1 decision tree is gone (one regression form, one guard rail). The `train` and `leo_offset` CLI verbs in `run_collocate.py` are replaced by `soft_cal` and `tc_variance`._

_**(P4) Step B3 Candidate 3 (UNet 3+, Lee 2025 style) dropped.** The §7.8 gap-fill comparison shrinks from three candidates to two (RF primary; RF+DNN stretch). The Lee 2025 reference is removed across the plan; diagnostics borrowed from it that stand on their own (the ±15% train-vs-validation consistency criterion, the residual-envelope-vs-AERONET plot, the CAMS-vs-MERRA-2 reanalysis-substitution check) are kept but no longer attributed to Lee._

_**(P5) §10 caveats re-derived for v3.4.** Caveat 2 (central Vietnam) reframed around the MERRA-2 anchor instead of the dead LEO offset. Caveat 12 replaced — new caveat about MERRA-2's structural smoothness (its 0.5° × 0.625° resolution learns regional-mean shifts, not fine-scale spatial structure). Caveats 13, 15, 17 reworded to match the σ²_TC vocabulary. Caveat 16 fixed to reference §8.1.6 (the §8.4 ghost section is removed). Ghost references to §8.3 / §8.4 / §8.5 in §11 are corrected to §8.0 / §8.1 / §8.1.4 / §8.1.6._

_The §6 pipeline diagram is updated for the linear calibration chain. §7.0 is reframed as AERONET-cell extraction for validation, since training no longer touches AERONET. The plan version moves from "Draft 3.3.3" to "Draft 3.4.0" throughout._

---

_**Pre-v3.4 history (condensed, for record).** The plan went through Drafts v3.2 → v3.3 → v3.3.1 → v3.3.2 → v3.3.3 before the v3.4.0 pivot:_

_- **v3.3** (vs v3.2): three reversions after v3.2 design choices failed implementation. Himawari L2/L3 merged per-pixel rather than weighted as separate fusion sources; wet-season Himawari handled by a 0.5× fusion-stage down-weight rather than tightened read-stage QA; IDW spatial blend between AERONET anchors dropped in favour of a LEO-anchored offset map for central Vietnam._

_- **v3.3.1** (colocation flaw fix, not a methodology bump): train and apply paths routed through the same Stage A2 0.05° gridded intermediate to fix a long-standing scale mismatch. MODIS no-orbit-timestamp daily-mean fallback dropped._

_- **v3.3.2** (implementation refinements): per-pixel Himawari L2/L3 merge made stratum-aware via `post_correction_rmse.json`; CDF training decision tree expanded around KS / range_ratio / decile_coverage diagnostics; `'none'` strata given an explicit fusion penalty; LEO–Himawari offset rebuilt per-season for the merged Himawari grid._

_- **v3.3.3** (one source of truth): `MODIS_SOUTH_WEIGHT_FACTOR` (0.1) and `HIMAWARI_WET_WEIGHT_FACTOR` (0.5) decommissioned in favour of the post-correction RMSE table; §7.4.1 gained two CV-time guard rails (CV-inflation step-down and "don't fix what isn't broken"); §7.4.1 thresholds tightened; §7.1 Himawari QA simplified to the JAXA strict bit-mask; §7.1 VIIRS QA uniform ≥ 2 threshold; §7.2.1 Stage A2 added per-cell `aod_std` and `cv` fields._

_The v3.4.0 entry above describes the changes vs v3.3.3 in detail._
