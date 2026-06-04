# Near Real-Time AOD Mapping of Vietnam: Multi-Source Satellite Fusion with Bias Correction and Spatiotemporal Gap-Filling

### Thesis Framework & Methodology — Draft 3.0

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

- **RQ1.** Which AOD retrieval product per sensor (Himawari L2 vs. L3; MAIAC vs. Deep Blue; VIIRS SNPP vs. NOAA-20) performs best over Vietnam, and does the best choice differ by region or season?
- **RQ2.** Can region- and season-aware bias correction against AERONET, combined with inverse-RMSE sensor weighting, produce a fused AOD product that is more accurate over Vietnam than either an equal-weight merge (Gupta 2024 style) or a single best sensor?
- **RQ3.** How effectively can ML-aided spatiotemporal gap-filling, using reanalysis AOD and meteorology as covariates, recover usable AOD on days/regions where satellites see nothing?
- **RQ4.** Does the resulting merged + gap-filled product improve daily AOD–PM2.5 coupling over Vietnam compared to the Himawari-only baseline of R² = 0.293 established by Nguyen et al. (2025)?

### 1.3 Claimed contributions

1. The first published Vietnam-specific multi-sensor merged AOD product at 0.05°/30-min covering Sep 2022 – Apr 2026.
2. A regional/seasonal bias-correction lookup table per sensor for Vietnam — reusable by future studies.
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
| **Lee et al. 2025** (UNet 3+ Deep Blue gap-fill) | CONUS                          | 12 km / daily      | None                                                  | Single sensor (DB) | UNet 3+ + MERRA-2 + NAM met + HMS smoke | Single sensor; daily; trained on US wildfire-dominated regime                    |
| **Nguyen et al. 2025** (this team)               | Vietnam                        | Point sites        | Validation only                                       | None               | None                                    | Validation study only — no gridded product produced                              |

**This thesis = Ahn 2021's bias-correction + fusion framework, adapted to Vietnam with VZA-aware weighting, region/season strata, combined with a gap-filling step inspired by Chen 2023 / Youn 2024 / Lee 2025. The empirical anchors come from Nguyen et al. 2025.**

What is genuinely new:

- **Vietnam-specific sensor algorithm selection per region**, not a single retrieval algorithm everywhere (departure from Gupta 2024).
- **Region- and season-stratified bias correction** with explicit handling of the central Vietnam ground-truth gap (departure from Ahn 2021).
- **Two-stage product design**: a bias-corrected fused product (analogous to Ahn 2021) followed by an ML-aided gap-filled daily product (analogous to Chen/Youn/Lee). The literature does these in isolation. Doing both in sequence for the same domain is uncommon.
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
| Validation sites           | AERONET: Nghia Do (Hanoi, 21.048°N) and Bac Lieu (9.30°N)                            |
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
- _Himawari L2 vs L3 — site-type-dependent choice:_ In the high-AOD urban north L2 is better (R = 0.701, %EE = 34.5% vs L3 R = 0.869 but bias −0.316 and %EE only 24.7% — L3 systematically underestimates events). In the low-AOD maritime south L3 is better (R = 0.824, %EE = 67.8% vs L2 R = 0.733). The fusion uses **L2 in the north and L3 daily in the south**, with a smooth latitudinal blend between them.
- _MODIS MAIAC vs Deep Blue:_ MAIAC's 1 km pixels find cloud-free gaps much more often (~250–400 matches/year at each AERONET site, vs ~50 for Deep Blue 10 km). MAIAC is kept; MODIS Deep Blue is rejected on coverage grounds for this domain.
- _VIIRS L3 daily 1°:_ Rejected. At 1° resolution Vietnam is ~8 grid cells nationwide — destroys spatial structure and removes the within-day timing information needed for Himawari co-location.

### 4.2 Ground truth — AERONET V3 L2.0

| Site             | Lat / Lon           | Period covered      | N observations | Mean AOD₅₅₀ |
| ---------------- | ------------------- | ------------------- | -------------- | ----------- |
| Nghia Do (Hanoi) | 21.048°N, 105.800°E | Feb 2022 – Apr 2026 | 13,172         | 0.699       |
| Bac Lieu         | 9.30°N, 105.70°E    | Feb 2022 – Apr 2026 | 15,690         | 0.212       |

AERONET is interpolated from 500/675 nm to 550 nm using the site-specific Ångström exponent, matching the LEO retrieval wavelength.

### 4.3 Ground PM2.5 — Envisoft (validation context only)

27 stations selected from 63 available on a ≥85% completeness threshold, distributed 10/8/9 across north/central/south (Nguyen et al. 2025, Table 1). Hourly PM2.5, 451,082 records, 91.4% overall completeness. Used here only to validate that the merged AOD product captures pollution episodes visible in the PM2.5 record (Section 8.4); the actual AOD → PM2.5 regression is a downstream project.

### 4.4 Ancillary data (covariates for bias correction and gap-filling)

| Dataset                                           | Role                                                                                                                                                                                                    |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ERA5 reanalysis                                   | RH (i.e: Humidity) (preferentially overridden by Envisoft in-situ), PBLH (constrained ≥ 50 m), 10-m wind, total cloud cover, surface solar radiation, dew-point — bias correction & gap-fill predictors |
| CAMS global reanalysis AOD (0.4°, hourly, 500 nm) | Coarse-resolution "shape" predictor for gap-filling, downscaled to 0.05° (Chen et al. 2023; Youn et al. 2024)                                                                                           |
| MERRA-2 AOD (0.5° × 0.625°, hourly)               | Alternative reanalysis predictor for gap-filling (Lee et al. 2025) — used as a sanity check against CAMS                                                                                                |
| GPM IMERG precipitation (0.1° / 30-min → hourly)  | Wet-scavenging flag for validation; gap-fill covariate                                                                                                                                                  |
| MODIS NDVI (MOD13C1, 0.05°, 16-day)               | Surface/vegetation predictor for gap-fill                                                                                                                                                               |
| SRTM elevation (90 m → resampled 0.05°)           | Topographic predictor for gap-fill                                                                                                                                                                      |
| MODIS Land Cover (MCD12Q1)                        | Surface-type stratification for bias correction                                                                                                                                                         |
| FIRMS active fire                                 | Flag extreme biomass-burning days for separate treatment                                                                                                                                                |
| GPWv4 population (~5 km)                          | Anthropogenic source proxy (gap-fill predictor)                                                                                                                                                         |

Choice of ancillary predictors mirrors Chen et al. 2023 and Youn et al. 2024 (which both validated their utility for AHI gap-filling), plus Lee et al. 2025 (which added MERRA-2 + smoke).

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
3. **The optimal Himawari product level is region-dependent**: L2 in the high-AOD north preserves events; L3 in the low-AOD south reduces scan noise.
4. **MAIAC fails at Bac Lieu** (R = 0.411) over reflective Mekong agricultural surfaces. MAIAC should be heavily down-weighted in the south, not aggressively bias-corrected (an unreliable model cannot be corrected into reliability).
5. **Inter-sensor agreement degrades north → south for MODIS–VIIRS** (R² = 0.837 / 0.638 / 0.545) but is U-shaped for VIIRS–Himawari (highest in south R² = 0.756, lowest center R² = 0.450). Central Vietnam is the weakest spot for sensor consistency.
6. **AOD availability is only 10.3% of hourly slots** (6.9% in July, 16.7% in April). Gap-filling is dominant, not auxiliary.
7. **Physics correction** (RH, PBLH) lifts hourly Himawari–PM2.5 correlation from r = 0.110 to r = 0.162 — a 1.5× improvement. Cheap and worth applying.
8. **Fine-mode (Rf ≥ 0.5) + uncertainty (≤ 0.5) filters help Himawari** (r 0.167 → 0.242 → 0.239) but produce **no measurable improvement for MAIAC** because MAIAC preprocessing already favors fine-mode conditions. Filter Himawari, don't bother filtering MAIAC.
9. **RANSAC robust regression** lifts daily Himawari–PM2.5 R² from 0.065 (OLS) to 0.293 — used here diagnostically, not as a product filter.

---

## 6. Methodology Overview

```
       L2 Granules                Reanalysis & ancillary
[Himawari, MAIAC, VIIRS-SNPP,        [CAMS, MERRA-2,
 VIIRS-NOAA20]                        ERA5, IMERG, NDVI,
       │                              elev, land cover]
       ▼                                     │
[Step A1: QA Filtering]                      │
       ▼                                     │
[Step A2: Regrid to 0.05°, 30-min slots]     │
       ▼                                     │
[Step A4: Region/Season bias correction] ◄───┤  (AERONET + IDW spatial extension)
       ▼                                     │
[Step A5: Inverse-RMSE sensor fusion]        │
       ▼                                     │
[Step A3: Physics normalization] ◄───────────┘  (applied to AOD_merged; stored as
       │                                         separate output field only — not
       │                                         fed back into A4/A5)
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

Stage A produces the 30-min merged product (analogue of Ahn 2021's hourly composite, refined for Vietnam). Stage B produces the daily gap-filled product (analogue of Chen 2023 / Youn 2024 / Lee 2025, adapted to Vietnam). Stage C validates both.

---

## 7. Methodology Detail

### 7.0 Satellite–AERONET colocation protocol

The colocation workflow is separated into three stages that can be run independently: (1) extract satellite AOD time series at the two AERONET station locations, (2) temporally match the extracted satellite values to the corresponding AERONET observations, and (3) fit the bias-correction transfer functions from the matched pairs. This separation allows new satellite data or updated AERONET records to be incorporated by re-running only the relevant stage.

#### 7.0.1 Spatial matching

Each satellite product uses a tiered neighbourhood approach adapted to its native grid geometry (Ichoku et al. 2002; Levy et al. 2010):

1. **Exact pixel (primary):** The pixel or grid cell whose centre contains the AERONET station coordinates is extracted.
2. **3×3 neighbourhood (Fallback 1):** If the exact pixel has no valid retrieval, the mean of all valid pixels within one native-cell radius is used (a 3×3 box of native cells centred on the station). This is consistent with the 5×5 MODIS-pixel neighbourhood of Ichoku et al. (2002) scaled to each sensor's native resolution.
3. **5×5 neighbourhood (Fallback 2):** Applied only when a stratum would otherwise fall below the minimum sample size for CDF fitting (N < 100).

Each matchup records which spatial tier was used and the within-neighbourhood AOD standard deviation.

**Scene homogeneity filter:** A matchup is rejected if the within-neighbourhood AOD spread exceeds a threshold that scales with the mean AOD at the box (Levy et al. 2010, §3.2). This discards retrievals contaminated by cloud edges, mixed land/water, or sub-pixel aerosol gradients. No wider neighbourhood is tried after a heterogeneity rejection.

**Sensor-specific spatial details:**

| Sensor | Grid type | Exact pixel | 3×3 radius | 5×5 radius |
|--------|-----------|-------------|------------|------------|
| Himawari L2/L3 | Fixed 0.05° raster | Station's grid cell | ±1 cell (~5.5 km) | ±2 cells (~11 km) |
| MODIS MAIAC | Fixed 1 km sinusoidal | Station's 1 km cell | ±1 cell (~1.5 km) | ±2 cells (~2.5 km) |
| VIIRS SNPP/NOAA-20 | Raw swath ~6 km pixels | < 3 km from station | < 9 km | < 15 km |

#### 7.0.2 Temporal matching

**Strategy: satellite-centric.** One record is emitted per satellite snapshot (not per AERONET observation). For each satellite observation, all AERONET measurements within the time window are averaged. This avoids over-representing a single satellite snapshot against multiple AERONET points.

| Sensor | Matched to | Time window | Rationale |
|--------|-----------|------------|-----------|
| Himawari L2 | Each 10-min L2 snapshot | ±30 min | Multiple 10-min files within the slot are averaged into one record |
| VIIRS SNPP / NOAA-20 | Each overpass | ±30 min | Consecutive granules from the same overpass are pooled before extraction |
| MODIS MAIAC | Each orbit layer separately | ±30 min **per orbit** | Terra (~10:30 LT) and Aqua (~13:30 LT) matched independently using per-orbit timestamps; daily-mean fallback when timestamps are unavailable |
| Himawari L3 | Daily mean AERONET AOD | Whole day | L3 is itself a daily composite; per-observation matching would introduce noise inconsistent with the L3 aggregation |

The per-orbit MODIS approach avoids confounding diurnal aerosol variation with retrieval bias: Terra and Aqua overpasses are matched independently rather than against a daily mean that mixes morning and afternoon aerosol loading.

### 7.1 Step A1 — Quality filtering

**Himawari AHI** (most aggressive filtering, since this is where filtering pays off):

- QA ≥ 1; SZA < 70°; VZA < 60° (cutting the worst pixel-distortion zones in the north).
- Fine-mode fraction Rf ≥ 0.5 (validated by Nguyen 2025: improves r from 0.167 → 0.242).
- Retrieval uncertainty ≤ 0.5 (cloud-edge rejection).
- VZA > 55° flagged with lower confidence (carried into the fusion weight, not discarded).

**MODIS MAIAC (MCD19A2):**

- Valid AOD range: 0 ≤ AOD ≤ 5. The MAIAC algorithm's internal multi-angle retrieval implicitly restricts to high-quality land retrievals; no additional fine-mode or uncertainty filters are applied (they give no measurable improvement over MAIAC's own pre-filtering).
- Terra and Aqua orbits separated by per-file timestamp, with a daily-mean fallback when timestamps are unavailable.

**VIIRS Deep Blue (SNPP + NOAA-20):**

- Land pixels: QA ≥ 2 (moderate–good). Ocean and coastal pixels: QA ≥ 1, allowing mixed-surface pixels to contribute when the ocean retrieval meets a lower threshold.
- Valid AOD range: 0 ≤ AOD ≤ 5.
- SNPP and NOAA-20 treated as separate sensors throughout.

### 7.2 Step A2 — Regridding to 0.05° × 30-min

Box-averaging per Gupta et al. 2020: only pixels whose centre falls inside a 0.05° cell contribute (no nearest-neighbour fill or ring search).

1. Per 30-min window, collect L2 pixels whose centre coordinates fall in each 0.05° cell.
2. MAIAC (1 km) and VIIRS DB (6 km): box-average to per-cell mean, std, count, mean VZA, mean SZA.
3. Himawari (0.05° native): already on the target grid. Multiple 10-min L2 files within the slot window are averaged into one slot value.

**Output per sensor per 30-min slot:** mean AOD, std AOD, pixel count, mean VZA, mean SZA.

### 7.3 Step A3 — Physics normalization (stored as a separate output, applied after fusion)

```
AOD_phys = AOD_merged × (1 − RH/100)^γ / PBLH
```

with γ = 0.6 (mixed-type empirical; Kotchenruther & Hobbs 1998), PBLH constrained ≥ 50 m, RH and PBLH from ERA5 bilinearly interpolated from 0.25° to 0.05°.

**Ordering note:** In the production pipeline, Step A3 runs _after_ Steps A4 and A5. The physics correction is applied to the fused `AOD_merged` and stored as a separate output field alongside the ERA5 RH and PBLH fields. It is **not** fed back into the bias-correction or fusion steps. This is deliberate: AERONET measures raw column AOD, so bias-correction training and ICW fusion must operate on raw AOD; physics normalization is a downstream PM₂.₅-modelling convenience that does not belong in the calibration loop.

This step is validated by Nguyen 2025 to improve hourly Himawari–PM₂.₅ from r = 0.110 → 0.162. If ERA5 data are unavailable for a slot, the physics fields are omitted but the raw merged AOD is always written.

### 7.4 Step A4 — Region/Season-aware bias correction (the Ahn-2021 step, refined)

**Strategy.** Use AERONET to train a transfer function per _(sensor, region, season)_ stratum, then extend spatially via IDW from the two anchor sites.

#### 7.4.1 Per-site quantile mapping (CDF correction)

For each stratum:

1. Build empirical CDF of the sensor's AOD at AERONET-matched times.
2. Build the corresponding AERONET CDF.
3. The transfer function maps each sensor AOD value to its AERONET-equivalent value by matching quantile ranks. This corrects both mean bias _and_ distribution shape, which simple linear regression cannot do at high AOD.

**Implementation:** Empirical CDFs are built from 200 quantile points across the matched pair distribution. A monotone piecewise cubic interpolant is fitted from satellite quantiles to AERONET quantiles — equivalent in spirit to Ahn 2021's piecewise cubic, with stable extrapolation beyond the training range. Where N_pairs < 100, linear regression is used as a fallback. The fitted transfer functions are saved after training and reloaded at run time, so training and production runs are fully decoupled.

Strata used:

- **Region:** North (Nghia Do anchor) / South (Bac Lieu anchor) — with smooth interpolation across central Vietnam.
- **Season:** Dry (Oct–Apr) / Wet (May–Sep).
- **Himawari level:** L2 for the north / L3 daily for the south (per finding 3 in §5.2).

#### 7.4.2 Spatial extension: IDW from AERONET anchors

AERONET anchors only the two endpoints (north and south). To extend bias correction across Vietnam (especially Central Vietnam where no AERONET exists):

- Apply Ahn 2021's **IDW from the two AERONET anchors**. Distance weight ∝ d⁻². For any grid cell, the corrected AOD is the IDW-blend of the north-stratum (Nghia Do) and south-stratum (Bac Lieu) corrections applied at the cell's AOD value:

  ```
  w_N = 1 / d(cell, NghiaDo)²
  w_S = 1 / d(cell, BacLieu)²
  AOD_corr(cell) = (w_N · f_N(AOD) + w_S · f_S(AOD)) / (w_N + w_S)
  ```

- The central Vietnam coefficients are therefore _interpolated_ but _not validated_ (no central AERONET). This is flagged as a limitation in §10.

**Future work — LEO–Himawari co-location correction (not yet implemented):** A planned second pass would use (AOD_LEO, AOD_Himawari) co-located pairs across all Vietnam grid cells over 4 years to build a spatially varying offset map for Himawari, anchoring the central region independently of the two AERONET sites. This will provide a denser spatial constraint, particularly across the high-VZA gradient in the north-central zone.

#### 7.4.3 Per-sensor correction summary

| Sensor               | Correction                                               | Rationale                                      |
| -------------------- | -------------------------------------------------------- | ---------------------------------------------- |
| Himawari (blended)   | Quantile mapping (north) + IDW blend to south correction | Both L2 and L3 keys receive the same spatially blended grid; region-appropriate correction applied via IDW |
| MAIAC (north/central)| Quantile mapping or linear regression                    | Large negative bias (−0.146 at Nghia Do); reasonable skill in north |
| MAIAC (south)        | Down-weight factor 0.1 + minimal correction              | R = 0.41 at Bac Lieu; cannot be corrected reliably |
| VIIRS SNPP & NOAA-20 | Quantile mapping or linear regression (per region)       | High skill; small positive bias corrected      |

### 7.5 Step A5 — Inverse-RMSE fusion (Ahn 2021 ICW, region/season-stratified)

For each 0.05° cell and 30-min slot:

```
w_i = 1 / RMSE_i(region, season, sensor, product-level)²
AOD_merged = Σ(w_i · AOD_i_corrected) / Σ(w_i)
```

RMSE_i is taken from post-correction validation statistics stratified per (sensor, region, season). Initial prior RMSE values are self-derived from the training collocations (Sep 2022–Dec 2024); once bias-correction training is complete, the post-correction RMSE values replace these priors for all subsequent fusion runs. This is identical in form to Ahn 2021's ICW (their Eq. 3), but with strata they did not use.

**RMSE floor:** A minimum RMSE of 0.05 is enforced when computing ICW weights (1/RMSE²). Wet-season corrections trained on very few pairs can overfit to near-zero RMSE, which would produce near-infinite weights and unstable fusion.

**Himawari wet-season down-weighting:** In May–Sep, Himawari is down-weighted by a factor of 0.1, analogous to the MAIAC south down-weighting. Validation shows near-zero R for both AERONET stations during wet months, caused by cloud-edge contamination that passes the QA filters.

**Himawari L2/L3 blending:** Both L2 and L3 are read for every slot, then blended per grid cell before entering the fusion: north cells (lat ≥ 16°N) use pure L2, south cells (lat < 11.5°N) use pure L3, and the central zone receives a linear latitudinal blend. Both the blended-L2 and blended-L3 representations enter the ICW fusion with their respective RMSE weights, so the combined Himawari contribution is naturally region-weighted (L2-dominant in the north, L3-dominant in the south).

**Sensor inclusion rules** (ICW weights enforce this; explicit logic handles edge cases):

```
if VIIRS available in slot:              include (highest accuracy anchor)
if MAIAC available and not south region: include with regional weight
if MAIAC in south region:               strongly down-weighted (factor 0.1; R = 0.41)
if Himawari available and SZA < 70°:    include
if Himawari in wet season (May–Sep):    strongly down-weighted (factor 0.1; R ≈ 0)
if only one sensor:                     use that sensor, flag low confidence
if no sensor:                           leave as gap → Stage B
```

**Stage A output** — 48 × 30-min NetCDF files per day, each containing per cell:

- `AOD_merged` (550 nm, ICW-weighted mean of bias-corrected sensors)
- `AOD_std` (cross-sensor spread)
- `n_sensors`, `dominant_sensor` (sensor with highest ICW weight)
- `confidence_flag` (0 = no data, 1 = Himawari only, 2 = LEO only, 3 = Himawari + LEO, 4 = multi-LEO + Himawari)
- Per-sensor bias-corrected grids (diagnostic; written only when data are available)
- `AOD_phys_corrected` (Step A3 output; physics-normalized surface-concentration proxy)
- ERA5 RH and PBLH fields (written when ERA5 data are available for the slot)

### 7.6 Step B1 — Daily aggregation

Daily mean from all valid 30-min slots, plus standard daily-product variables: `n_slots`, `daily_std`, `daily_max`, `hour_of_max`.

### 7.7 Step B2 — Spatial gap-filling (baseline)

For each daily AOD map:

- **Approach A (interior gaps).** Thin-plate spline / ordinary kriging using a same-day-fitted variogram. Cap at 200 km from nearest valid observation.
- **Approach B (persistent gaps, e.g., cloudy wet-season cells).** Climatological pattern fill: same-month mean across all years available, downweighted by `1 − fraction_observed`. Lowest confidence flag.

This baseline alone produces a usable daily product but does not exploit reanalysis information.

### 7.8 Step B3 — Spatiotemporal gap-filling with ML

The literature is clear that ML with reanalysis covariates substantially outperforms pure spatial interpolation for AOD gap-filling. Three strategies are evaluated; the best is kept as the primary product. (If time is short, this entire step can degrade gracefully to Step B2's kriging — see §11.)

**Candidate 1 — Random Forest (Youn 2024 style, primary).**

Predictors per 0.05° cell per day:

- CAMS reanalysis AOD (downscaled from 0.4° to 0.05° via cubic spline)
- ERA5: 10-m u/v wind, RH, T2m, dew-point T, surface pressure, PBLH, total cloud cover, surface solar radiation, total precipitation, albedo
- Static: elevation (SRTM), NDVI (MOD13C1, 16-day), land cover, population (GPWv4)
- Coordinates: lat, lon (so the model can learn region-specific behavior)
- AOD context: same-cell AOD on `t−1`, `t+1`, and ±3 days where available

Trained on grid-cell-time pairs where AOD is _observed_, predicting the observed value. Applied to fill cells where AOD is missing. Hyperparameters tuned via out-of-bag error.

**Candidate 2 — RF + DNN forward/backward (Chen 2023 style, if time allows).**

Adds a temporal-interpolation stage on top of Candidate 1: a small DNN trained to estimate `AOD(t)` from `AOD(t±10 min)` and meteorological deltas, combined as a time-weighted forward/backward average. This refines the 30-min cycle rather than daily.

**Candidate 3 — UNet 3+ (Lee 2025 style, stretch goal).**

Reframes gap-filling as image inpainting: stack daily AOD + reanalysis layers as channels, train UNet 3+ to predict the AOD layer at masked locations. Stronger spatial context than RF; needs more data/compute and is a stretch given thesis scope.

**Selection criterion.** Out-of-bag R² and AERONET-blind validation. The model whose AERONET RMSE/R/%EE is best on the held-out validation period (§8) is the primary gap-filling product. The simpler kriging fallback (§7.7) is always also computed and shipped, so the user can choose.

**Provenance flags.** Every gap-filled cell carries `gap_fill_method ∈ {observed, kriging, ml_rf, ml_dnn, ml_unet}` and `days_since_last_observed`.

---

## 8. Validation Strategy

### 8.1 Held-out AERONET validation (Stage A and Stage B)

Temporal split (not station split — only two stations):

- **Train:** Sep 2022 – Dec 2024 (~2.3 years).
- **Held-out test:** Jan 2025 – Apr 2026.

Report R, R², RMSE, MAE, Bias, %EE separately for:

- Each station (Nghia Do, Bac Lieu)
- Each season (Dry/Wet)
- Each `confidence_flag` (1 to 4)
- Each `gap_fill_method` for the daily product

### 8.2 Internal-consistency check (no external data required)

Where multiple sensors overlap in a 30-min slot, the across-sensor spread is an _internal_ uncertainty estimate. Report inter-sensor R² by region as a sanity check that the Stage A correction has improved internal consistency. Baseline numbers from Nguyen 2025 (MODIS–Himawari R² = 0.621 north, 0.474 central, 0.756 south) provide the bar to beat.

### 8.3 Precipitation-aware validation

Per Nguyen 2025, AOD–PM2.5 coupling depends sharply on hours-since-last-rain (north r = +0.301, central r = +0.116, south r = +0.047). The merged product is validated separately for:

- **Dry intervals** (>24 h since rain): expected best coupling.
- **Post-rain intervals** (0–12 h): wet scavenging active.

GPM IMERG ≥ 0.1 mm/hr defines a rain event.

### 8.4 Indirect validation via PM2.5 case studies

Three to five documented events:

1. A severe Hanoi haze episode (dry-season, AOD > 1.5, PM2.5 > 100 µg/m³).
2. A March–April biomass-burning transport event from mainland SEA.
3. A monsoon-season multi-day period — primarily a gap-fill stress test.
4. A precipitation washout event — to confirm the wet-flag correctly separates coupled vs decoupled periods.

Pass criterion: the merged product captures the temporal pattern of the corresponding Envisoft PM2.5 record (qualitative + Spearman rank correlation), and the gap-filled product retains the pattern through cloud cover.

### 8.5 Comparison against baselines

The merged product is compared against:

- **B1.** Best-single-sensor: bias-corrected VIIRS-only daily product.
- **B2.** Gupta et al. 2024 equal-weight DT merged product (re-sampled to 0.05°).
- **B3.** Ahn et al. 2021 NE Asia composite product (where it overlaps Vietnam temporally).
- **B4.** Nguyen et al. 2025's daily Himawari-only RANSAC result (R² = 0.293 vs PM2.5).

Hypothesis: this thesis's product beats all four on AERONET-validated R, RMSE, %EE.

### 8.6 RANSAC diagnostic (not a product filter)

Per Nguyen 2025, RANSAC lifts daily Himawari–PM2.5 R² from 0.065 to 0.293. Used here only to _report_ outlier influence on AERONET–merged validation. The merged AOD product itself is _not_ RANSAC-filtered (that would discard real high-AOD events).

---

## 9. Expected Outcomes and Deliverables

1. **Vietnam 30-min merged AOD dataset** at 0.05°, Sep 2022 – Apr 2026, NetCDF, with provenance flags. Archived publicly.
2. **Vietnam daily gap-filled AOD dataset** at 0.05°, NetCDF, with gap-fill method flags and days-since-observation.
3. **Bias-correction lookup tables** (CDF coefficients per sensor × region × season × product-level), distributed as supplementary data, reusable for future studies.
4. **Methodological comparison report** of three gap-filling strategies (kriging, RF, DNN/UNet if completed) on the same domain — the first such comparison for tropical Southeast Asia.
5. **Validation against Gupta 2024, Ahn 2021, and best-single-sensor baselines.**
6. **Recommendation document** for the downstream PM2.5 mapping project: which AOD layer to feed in by region and by season, and where uncertainty is highest.

**Quantitative targets** (success criteria the thesis commits to):

| Metric                                                    | Baseline (best individual sensor / Gupta 2024) | Thesis target                               |
| --------------------------------------------------------- | ---------------------------------------------- | ------------------------------------------- |
| AERONET R at Nghia Do                                     | 0.915 (VIIRS only)                             | ≥ 0.90 _with full daily coverage_           |
| AERONET R at Bac Lieu                                     | 0.845 (VIIRS only)                             | ≥ 0.85 _with full daily coverage_           |
| AERONET RMSE Nghia Do                                     | 0.271 (VIIRS only)                             | ≤ 0.30 averaged across all confidence flags |
| Daily AOD spatial coverage over Vietnam                   | 10.3% (Himawari L2, raw)                       | ≥ 95% (gap-filled product)                  |
| Daily AOD–PM2.5 R² (validation, north)                    | 0.293 (Himawari RANSAC)                        | ≥ 0.35                                      |
| Daily AOD–PM2.5 R² (validation, MODIS-equivalent ceiling) | 0.573 (MODIS RANSAC)                           | — (ceiling reference)                       |

---

## 10. Limitations and Caveats

1. **AOD availability is fundamentally ~10% before gap-filling.** Gap-filled values will dominate ~90% of the daily product for most of the year and must be flagged distinctly from observed values throughout the dataset.
2. **Two AERONET sites only.** Central Vietnam bias correction is _interpolated_ and _unvalidated by direct AERONET ground truth_. Establishing an AERONET station near Đà Nẵng or Huế is the single highest-priority external follow-up.
3. **Himawari high VZA over northern Vietnam** elongates the atmospheric path and inflates pixel footprints by 2–5×. Bias correction reduces but does not eliminate this geometric limitation.
4. **MAIAC fails at Bac Lieu** (R = 0.411). The south is therefore essentially served by Himawari + VIIRS, with MAIAC contributing minimal weight.
5. **Wet season in the north is unconstrained by LEO** (May–Sep). The merged product during these months depends on bias-corrected Himawari plus gap-filling, neither of which is fully validatable when LEO data is scarce — and the wet season is precisely when AERONET coverage is also thinnest.
6. **Spatially uniform hygroscopic exponent γ = 0.6.** Marine aerosols in the south are more hygroscopic; industrial aerosols in the north are less so. Per-region γ tuning is future work.
7. **PBLH unavailable at hourly resolution for ~25% of the study period**; replaced by monthly climatology over Jan–Jun 2024. Affects physics-correction quality for that subset.
8. **Gap-filled values are model estimates**, not observations. They must be flagged in every output and should not be used for trend analysis or extreme-value statistics without an explicit "observed-only" filter.
9. **ML gap-filling generalizes only as far as the training data.** If a regime (e.g., a once-per-decade biomass burning event of unusual intensity) is absent from training, predictions there are extrapolations.
10. **Spectral harmonization is dropped.** Justified empirically (Nguyen 2025), but means the product is technically a mix of 500 nm Himawari and 550 nm LEO. The error this introduces is bounded at ~ΔR² of 0.003 and ΔRMSE of 0.001.
11. **MODIS MAIAC quality filtering relies on the algorithm's internal encoding.** Explicit bitmask application (bits 3–4) is not active in the current pipeline; filtering relies on MAIAC's own fill-value and valid-range metadata, which implicitly restricts to the algorithm's high-quality retrievals. Explicit bitmask filtering is a planned hardening step.
12. **LEO–Himawari co-location correction not yet implemented.** Central Vietnam bias correction remains a pure IDW interpolation between the two AERONET anchors. The calibration gap is largest in the 11.5°N–16°N zone where neither anchor is nearby.
13. **Himawari wet-season down-weight is empirically set.** The factor of 0.1 was chosen based on observed near-zero correlation during wet months but has not been tuned against held-out data; a different optimal value may emerge from validation.
14. **Physics correction uses ERA5 RH/PBLH only.** In-situ RH from surface stations (mentioned in §4.4) is not yet incorporated into the production pipeline.

---

## 11. Scope Management — What is in / out / stretch

**In scope (must complete):**

- Steps A1–A5 (full Stage A: filtering, gridding, physics correction, bias correction, fusion).
- Steps B1 + B2 (daily aggregation + kriging spatial gap-fill).
- §8.1, 8.2, 8.3 validation.
- §8.5 comparison vs Gupta 2024 and best-single-sensor.

**Stretch (do if time permits):**

- Step B3 Candidate 1 (RF gap-filling) — high priority stretch.
- §8.5 comparison vs Ahn 2021.
- §8.4 PM2.5 case studies (3 of 4).

**Stretch / future work:**

- Step B3 Candidates 2 and 3 (DNN, UNet 3+).
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
| 4. Bias correction                 | QM (CDF) + IDW spatial extension from AERONET anchors         | 5 weeks  |
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
- Chen, A., Yang, J., He, Y., Yuan, Q., Li, Z., Zhu, L. (2023). High spatiotemporal resolution estimation of AOD from Himawari-8 using an ensemble machine learning gap-filling method. _Science of the Total Environment_, 857, 159673. **[RF + DNN gap-filling reference]**
- Gupta, P. et al. (2024). Increasing aerosol optical depth spatial and temporal availability by merging datasets from geostationary and sun-synchronous satellites. _Atmospheric Measurement Techniques_, 17, 5455–5476. **[Global LEO-GEO DT merged product; the Vietnam-coarse benchmark to beat]**
- Holben, B. N. et al. (1998). AERONET — A federated instrument network and data archive for aerosol characterization. _Remote Sensing of Environment_, 66, 1–16.
- Kotchenruther, R. A. & Hobbs, P. V. (1998). Humidification factors of aerosols from biomass burning in Brazil. _Journal of Geophysical Research_, 103, 32081–32089.
- Lee, J. S. M., Loría-Salazar, S. M., Holmes, H. A., Sayer, A. M. (2025). Spatiotemporal Gap-Filling of NASA Deep Blue Satellite Aerosol Optical Depth Over the Contiguous United States Using the UNet 3+ Architecture. _Earth and Space Science_, 12, e2025EA004338. **[UNet deep-learning gap-fill reference]**
- Levy, R. C. et al. (2013). The Collection 6 MODIS aerosol products over land and ocean. _Atmospheric Measurement Techniques_, 6, 2989–3034.
- Lyapustin, A. et al. (2018). MODIS Collection 6 MAIAC algorithm. _Atmospheric Measurement Techniques_, 11, 5741–5765.
- Nguyen, K. T., Trinh, A. H., Bui, C. K. (2025). Assessing the Feasibility of Estimating Air Quality in Vietnam Using Satellite Data. _Student Scientific Research Conference, Hanoi University of Science and Technology_. **[Team paper — empirical anchor for this thesis]**
- Remer, L. A. et al. (2012). Retrieving aerosol in a cloudy environment. _Atmospheric Measurement Techniques_, 5, 1823–1840.
- Sawyer, V. et al. (2020). Continuing the MODIS Dark Target aerosol time series with VIIRS. _Remote Sensing_, 12, 308.
- van Donkelaar, A. et al. (2010). Global estimates of ambient fine particulate matter concentrations from satellite-based aerosol optical depth. _Environmental Health Perspectives_, 118, 847–855.
- Yoshida, M. et al. (2018). Common retrieval of aerosol properties for imaging satellite sensors. _Journal of the Meteorological Society of Japan_, 96B, 193–209. **[Himawari/AHI retrieval algorithm]**
- Youn, Y., Kim, S., Kim, S. H., Lee, Y. (2024). Spatial Gap-Filling of Himawari-8 Hourly AOD Products Using Machine Learning with Model-Based AOD and Meteorological Data: A Focus on the Korean Peninsula. _Remote Sensing_, 16, 4400. **[Pure-RF gap-fill reference]**

---

_Draft v3.1 — Stage A sections updated to reflect the current implementation. Key changes vs v3.0: (1) Step ordering corrected — physics normalization (A3) runs after fusion (A5) and is stored as a separate output, not in the calibration pipeline; (2) LEO–Himawari co-location correction reclassified as future work; (3) Himawari L2/L3 blending clarified — both are blended spatially before fusion and enter the ICW with separate region-stratified RMSE weights; (4) RMSE floor and wet-season Himawari down-weighting added to §7.5; (5) VIIRS QA corrected to a two-tier land/ocean policy; (6) MODIS MAIAC QA limitation noted; (7) four new limitations added (§10 #11–14)._
