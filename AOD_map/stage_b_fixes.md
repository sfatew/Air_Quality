# Stage B revision — fix list

Companion to `thesis_aod_vietnam_Plan_v3_4_0.md`. Scope: bring Stage B in line with the Stage A output cadence (30-min) and the methodological precedents (Youn 2024 for the RF gap-fill, **Yang & Hu 2018 for the spatiotemporal kriging baseline**, Chen 2023's SIM for the slot-independent training architecture). All fixes follow the user's directive: **everything in Stage B runs at 30-min cadence. No daily aggregation inside Stage B.**

Chen 2023's TIM / Candidate 2 ("option-b" temporal DNN) is **deleted, not deferred** — TIM is a temporal upsampler (hourly → 10-min), not a temporal gap-filler, and the per-slot RF already covers the gap-fill problem completely. See §7.8.2 edit below and §7 (resolved items) for rationale.

The kriging baseline is **upgraded from per-slot spatial kriging to spatiotemporal kriging** following Yang & Hu (2018, sum-metric variogram). Spatial-only kriging cannot fill slots that have zero in-domain observations; ST kriging can, by borrowing from neighbouring 30-min slots. See §7.7 edit below.

---

## 0. The daytime convention and dense-inference target

**Daytime window is data-driven per UTC day from Stage A filenames.** Stage A writes one NC file per slot only if that slot has at least one valid Stage A pixel anywhere in Vietnam. Files are named `merged_YYYYMMDD_HHMM.nc` (e.g., `merged_20221016_0000.nc` for the 00:00 UTC slot on 2022-10-16). The HHMM in the filename is the source of truth for which slots are observable on a given day.

**Window definition per UTC day `D`:**

1. List `stage_a_output/YYYY/MM/DD/*.nc`.
2. Parse the HHMM substring from each filename. Each match is an "observed slot" for day `D`.
3. **If day `D` has ≥ 10 observed slots:** `slot_first = min(HHMM)`, `slot_last = max(HHMM)`. The inference window is every 30-min slot in `[slot_first, slot_last]` inclusive.
4. **If day `D` has < 10 observed slots:** fall back to the 7-day median window (the per-slot median of `slot_first` and `slot_last` over the 7 calendar days centred on `D`, restricted to neighbour days that themselves clear the 10-slot bar). This handles fully-clouded days and days where a handful of isolated observations would give an unrepresentatively narrow window.
5. **At dataset edges** (first/last 3 days of Sep 2022 / Apr 2026 where the 7-day median is one-sided): use whichever side of the window is available.

**Why this is right.** Vietnam spans 9°N–23°N, so the actual daytime window varies with latitude and season — early observations as early as ~23:30 UTC (previous day) in summer, late observations through ~10:30 UTC in southern Vietnam. The hard-coded 00:00–09:30 UTC of the prior draft was provably wrong (you found observations through 10:30 UTC in the data). The filename-based approach matches Stage A by construction: we never try to fill a slot that wasn't observable, and we always fill the slots that were.

**Typical window size.** In practice the daytime window per UTC day will be ~20–22 slots (00:00 UTC ± a few slots at each end, varying by latitude/season). Order-of-magnitude estimates downstream use **~21 slots/day on average**.

**Inference target (the key point).** Stage B's job is to produce a **dense gap-filled product within each day's observation window**: every (cell, slot, day) where `slot ∈ [slot_first, slot_last](day)` has exactly one AOD value in the output, with no gaps inside the window. Concretely, on any given day, Stage A delivers an irregular set of observed slots within the day's window — maybe 10 out of 21, maybe 3 out of 21, maybe 19 out of 21 — and the rest are cloud gaps. Stage B fills **all** the missing slots in the window. Observed slots are passed through unchanged; missing slots are filled by ST kriging (B1) or RF (B2).

**Output volume.** ~1,330 days × ~21 slots/day (data-driven, varies per day) × ~13,500 cells ≈ **~3.8 × 10⁸ (cell, slot) records** in the gap-filled product. ~28,000 NC files per method (slightly more than the prior 26,600 estimate because Vietnam has a slightly wider effective daytime window than the original 20-slot cap). Plan disk and chunking accordingly.

**Implications baked into the rest of the fixes:**

- **Slot count per day:** **data-driven, ~21 on average**. All "slots/day" arithmetic uses 21 as the working estimate, but the actual count per day comes from Stage A filenames.
- **ST kriging window:** the temporal window `W_t` must bridge the nighttime gap between the last slot of day `D` and the first slot of day `D+1`. The gap is at least ~13 h (10:30 UTC → 23:30 UTC next day in summer south) and at most ~14.5 h (09:30 UTC → 00:00 UTC next day in winter north). Set `W_t = 24 h` to cover all cases (§7.7).
- **Inference loop, both B1 and B2:** for each day, determine the day's observation window from Stage A filenames (or the 7-day median fallback), iterate over every (cell, slot) in that window where Stage A is missing, predict, write. Stage A observed cells/slots pass through (no overwrite).
- **Output schema:** the gap-filled product contains exactly the day's-window slots per cell per day, no placeholders for night or for slots outside the window.

---

## 0.5. Shared training / validation / test protocol (both models)

Both ST kriging and RF use the same temporal partition **and the same intra-train validation strategy** so their §8.2 metrics are directly comparable.

**Partitions:**

| Partition | Range               | Span                  | Use                                    |
| --------- | ------------------- | --------------------- | -------------------------------------- |
| Train     | Sep 2022 – Dec 2024 | ~28 months, ~840 days | Model fitting + intra-train validation |
| Test      | Jan 2025 – Apr 2026 | ~16 months, ~490 days | §8.2 head-to-head evaluation only      |

Both partitions span monsoon and dry seasons, so neither is biased toward a particular regime.

**Intra-train validation: 5-fold contiguous temporal CV (both models).**

- **5 folds**, each ~5.6 months of the train partition (contiguous in time, not random).
- **Fold assignment by UTC date** — all slots of a given day fall in the same fold. This prevents within-day leakage (e.g., the 03:00 UTC slot in the training fold and the 03:30 UTC slot of the same day in the validation fold), which would be a trivially easy generalization case and inflate scores.
- **Per fold:** train on 4 folds, predict the held-out fold, accumulate RMSE / MAE / R².
- **RF use of CV:** hyperparameter grid search over `n_estimators`, `max_depth`, `min_samples_leaf`, `max_features`. Selection criterion is **mean CV-R² across the 5 folds**. After selection, the final RF is retrained on the full train partition.
- **ST kriging use of CV:** the sum-metric variogram is refit per fold on the other 4 folds' (cell, slot) pairs. CV serves two purposes for kriging: (a) report expected interpolation skill as the CV-mean RMSE, and (b) sanity-check that fitted variogram parameters (spatial range, temporal range, anisotropy `k`) don't drift wildly across folds (drift would indicate non-stationarity). Final variogram is fit on the full train partition for test inference.

**⚠ Do not use OOB-R² for RF.** sklearn's `RandomForestRegressor` offers `oob_score=True` and Youn 2024 reports OOB-R² as their RF skill metric, but OOB sampling is IID bootstrap and AOD has strong temporal autocorrelation (diurnal cycle, synoptic systems, monsoon persistence). OOB-R² would therefore be **systematically optimistic** vs true held-out temporal generalization — it tells you "how well RF predicts points held out from this exact day's training data" rather than "how well RF predicts a different time period." 5-fold temporal CV is the honest answer. Set `oob_score=False` explicitly in the sklearn config to avoid any temptation to report the misleading number.

**Inference (on full timespan):**

Both models produce predictions for every (cell, slot, day) in the day's observation window across Sep 2022 – Apr 2026. Train-partition predictions are produced too — useful for §8.2.4 consistency checks, for uncertainty-field calibration, and so that the output product is uniform across the full timespan. They are **not** used in the headline §8.2 evaluation.

**Test evaluation (§8.2):**

Metrics computed **on test partition only** (Jan 2025 – Apr 2026). This is the apples-to-apples comparison surface for both models.

- **AERONET-blind validation:** AERONET stations held out throughout (never seen by either model in training). Predictions at the held-out stations compared against AERONET ground truth in the test window.
- **Cell-blind validation:** random Stage A pixels held out from the train partition during fitting; corresponding predictions reported as test-period equivalents.
- **Head-to-head:** RF vs ST kriging compared on identical test-partition (cell, slot) pairs. Paired skill metrics (paired RMSE difference, paired t-test on errors) — fair because both models predict the same set of points.

---

---

## 1. Architectural change

**Old (v3.4.0):** Stage A 30-min merged → B1 collapse to daily → B2 daily spatial kriging → B3 daily ML gap-fill → daily validation.

**New:** Stage A 30-min merged → B1 per-slot spatiotemporal kriging (Yang & Hu 2018) → B2 per-slot RF gap-fill → per-slot validation. Daily aggregation, if needed at all, is a **downstream post-processing step** that lives in §8 (RQ4 PM₂.₅ comparison) — not in the Stage B pipeline.

This makes the §1.3 headline contribution ("0.05° / 30-min daytime Vietnam product") finally true of the _gap-filled_ product, not just the sparse Stage A merge.

---

## 2. Section-by-section edits

### §1.3 #1 — claimed contribution

No change needed; the claim is now actually delivered by the gap-filled product, not just Stage A.

### §3 (output specification box) — line 73-74

`Aggregated outputs: Daily, monthly, seasonal` → reword to:
`Native output: 30-min daytime slots (~20–22/day, data-driven from Stage A's per-day observation window; see §0). Optional downstream aggregations: daily, monthly, seasonal (post-processing, §8).`

### §6 pipeline diagram — lines 168-215

Replace the Stage B block. From:

```
═══════ Stage A complete: 30-min merged ═════
       ▼
[Step B1: Daily aggregation]
       ▼
[Step B2: Spatial gap-fill]
       ▼
[Step B3: Spatiotemporal gap-fill (ML)]
       ▼
═══════ Stage B complete: gap-filled daily ═══
```

to:

```
═══════ Stage A complete: 30-min merged ═════
       ▼
[Step B1: Spatiotemporal kriging (Yang & Hu 2018)]   (was B2 spatial kriging)
       ▼
[Step B2: Random Forest gap-fill per 30-min slot]    (was B3 Candidate 1)
       ▼
═══════ Stage B complete: gap-filled 30-min ══
       ▼
[Step C: Validation against held-out AERONET + Envisoft]
       │  (RQ4 comparison vs Nguyen 2025 R²=0.293 aggregates
       │   the 30-min product to daily here, not in Stage B)
```

Line 217: replace `Stage B produces the daily gap-filled product` with `Stage B produces the 30-min gap-filled product at the same cadence as Stage A`.

### §7.6 — Step B1 "Daily aggregation": DELETE entirely

The entire section (lines 447-471) is removed. Stage A's 30-min slot product flows straight into the gap-filling steps. The auxiliary fields written in old §7.6 (`daily_mean`, `n_slots`, `daily_std`, `daily_max`, `hour_of_max`, `weight_sum`, `sensor_set`) are either:

- already available per-slot from Stage A (`weight_sum`, `sensor_set`),
- recoverable downstream as a post-processing rollup if needed (`daily_max`, `hour_of_max` for the PM₂.₅ diurnal-peak feature),
- or no longer applicable (`n_slots` and the 3-slot floor — these were artefacts of daily aggregation, which no longer exists in Stage B; see §10 caveat update).

Renumber: **old §7.7 → new §7.6 (Step B1)**, **old §7.8 → new §7.7 (Step B2)**. Or keep section numbers and just drop the old §7.6 body — choose whichever creates fewer reference fixes elsewhere.

### §7.7 → new B1 — Spatiotemporal kriging baseline (Yang & Hu 2018)

**Method upgrade.** The per-slot spatial kriging design (one variogram per slot, fall back to climatology when too few same-slot observations) is replaced with **spatiotemporal kriging** following Yang & Hu (2018, _Sci. Total Environ._ 633, 677-683). The two-line argument: at 30-min cadence many monsoon slots will have zero in-domain valid observations, and Yang & Hu show explicitly that "days with no valid AOD data cannot be interpolated by spatial kriging, while ST kriging may work because it can borrow data from adjacent days." This is the only kriging design that survives the move to 30-min without collapsing into "climatology fallback" for most slots.

**Model: sum-metric variogram.** Yang & Hu compared four ST covariance families (metric, product, separable, sum-metric) and selected sum-metric on minimum MSE of variogram fit:

```
C_ST(h_s, h_t) = C_S(h_s) + C_T(h_t) + C_ST(sqrt(||h_s||² + k·||h_t||²))
```

with separate spatial, temporal, and space-time-joint components, plus an anisotropy parameter `k` that converts time to an equivalent spatial distance. We adopt sum-metric directly without re-comparing the four families (Yang & Hu's experiment already settled this; replicating it for Vietnam is a thesis-extension question, not a baseline question).

**Variogram fitting on the training partition.** Fit once on Sep 2022 - Dec 2024 (the same training partition as §7.8). All training-period (cell, slot) pairs with valid Stage A AOD are candidate input points; subsample to a tractable size (~10⁵ pairs) before fitting, weighted to ensure coverage across all months and slot positions. **Fitted parameters will differ from Yang & Hu's Beijing numbers** — Beijing is daily cadence over one year; we are 30-min cadence over 28 months in tropical Vietnam. Expected differences:

- **Temporal range much shorter than 3 days.** Yang & Hu's temporal range was 3 days because daily-mean AOD at mid-latitudes is dominated by synoptic systems. At 30-min cadence in Vietnam the dominant variability is the diurnal cycle plus convective scavenging; expect a temporal range on the order of a few hours, possibly with two characteristic scales (fast convective, slow synoptic) — worth examining the empirical variogram before fitting.
- **Spatial range probably similar.** Beijing's 60 km spatial range is set by mesoscale aerosol transport, which should be comparable in Vietnam (possibly longer over the deltas).
- **Anisotropy parameter `k`.** Yang & Hu reported `k ≈ 14.13 km/day`. At 30-min cadence ours will be `k` in `km / 30-min`; the numeric value falls out of the empirical fit, no need to seed it.

**Critical implementation detail — overnight gaps in the time axis.** Time distance in the variogram must respect actual elapsed time, not slot index. Consecutive 30-min daylight slots have `h_t = 30 min`. The last daylight slot of day `D` and the first daylight slot of day `D+1` are separated by the nighttime gap, which varies with the data-driven window (§0): at least ~13 h (summer south, e.g., 10:30 UTC → 23:30 UTC next day) and typically ~14–15 h. Store time as a real timestamp (UTC), not a slot ordinal, so the kriging weights correctly discount across-night neighbours relative to within-day neighbours. The variogram is fit only on (daytime, daytime) pairs — there are no daytime-to-nighttime pairs to feed it.

**Prediction window — must straddle one full day.** A full ST kriging system over the timespan × ~13,500 cells is computationally infeasible (the covariance matrix would be ~(13,500 × 1,330 × 21)² ≈ 1.4 × 10¹⁴ entries). Use a **moving window** approach: for each target (cell, slot), use only observations within a window of ±W_s km spatially and ±W_t hours temporally.

The choice of `W_t` is constrained by the nighttime gap. A 12-hour window does not work: for a slot at the start of day `D`'s observation window, looking back 12 h does not reach day `D-1`'s last slot (which is ~13–15 h earlier in wall-clock time). **Set `W_t = 24 h`** to cover all nighttime-gap cases and let every target slot see both the previous day's afternoon and the next day's morning. The variogram itself down-weights far-away time neighbours — the window only needs to be wide enough to avoid _clipping_ potentially-correlated data.

Recommended starting values: **`W_s = 100 km`**, **`W_t = 24 hours`**. Tune both based on a small sensitivity sweep (RMSE vs window size) at the start of Stage B execution — Vietnam's variogram fit may yield different ranges from Beijing's, in which case scale `W_s` to ~2× whatever joint range falls out. Per-target neighbour cap: ~150 observations (similar to Yang & Hu, balances accuracy against per-prediction wall time), which bounds compute cost regardless of how generous `W_s` is set.

**Inference loop.** For each UTC day `D` in the timespan:

1. Determine day `D`'s observation window `[slot_first, slot_last]` from Stage A filenames (or 7-day median fallback per §0).
2. Iterate over every (cell, slot, D) with `slot ∈ [slot_first, slot_last]`. If Stage A has an observed value at this point, pass it through unchanged (no kriging). Otherwise, build the local window, fit/lookup the variogram, solve the kriging system, write the predicted value plus its variance.

Result: a dense product within each day's observation window, ~21 slots/day on average, totalling ~28,000 NC files per method.

**Computational tool.** Yang & Hu used R `gstat` (`krigeST` function). Two implementation paths, with a note that the project's pipeline is otherwise Python:

- **(a) R `gstat` via `rpy2` or subprocess.** Mature, exactly matches Yang & Hu, well-documented for sum-metric models. The bridge is a standard pattern but adds an R environment-management dependency on top of the Python stack. Recommended for v1 if the dependency is acceptable — uses the reference implementation.
- **(b) Python `gstools` (Python-native).** Newer pure-Python ST kriging library. Reasonable for v2 or if the R dependency is a hard constraint. Caveat: sum-metric in `gstools` is less battle-tested than in `gstat`; budget extra validation time to confirm the variogram fit matches an R reference fit on the same data.

Pick based on what the Stage A pipeline already requires. If Stage A is pure Python with no R dependency, going via `gstools` keeps the stack uniform; if some other step already needs R, lean toward (a).

**Climatological fallback — drastically reduced scope.** With ST kriging, the explicit "Approach B: climatology fallback" disappears for almost all cases. The ST kriging variance naturally inflates in cells/slots with no nearby observations in space _or_ time, and the prediction reverts toward the local mean — which is what the climatology fallback was approximating manually. Keep a _narrow_ climatological-fallback path only for: **(i)** any (cell, slot) with no valid observations within the `W_s × W_t` window, **(ii)** the cold-start edges of the timespan: the first ~24 hours of Sep 2022 (no temporal back-window) and the last ~24 hours of Apr 2026 (no temporal forward-window). The climatology, when used, comes from same-slot-of-day same-month means computed from the training partition (e.g., a 14 July 2025 06:30 UTC fallback uses the mean of 06:30 UTC observations in that cell for Julys 2022-2024).

**Variance/confidence output.** ST kriging produces a per-prediction variance natively; ship it as `kriging_variance` per slot in the output schema (§7.8.3). This replaces the per-slot "approach used" string and is more informative for downstream users.

**Coverage expectations.** Yang & Hu reported 67.73% mean pixel-level completeness from ST kriging alone (Beijing, daily). At 30-min cadence in Vietnam we expect lower per-slot completeness from kriging alone — the per-slot observation count is smaller, even though temporal borrowing partially compensates. Order-of-magnitude target: **40-60% slot-level completeness from B1 (ST kriging) alone**, with the remaining gaps closed by B2 (RF). Re-evaluate after the first end-to-end run.

### §7.8 → new B2 — Random Forest gap-filling at 30-min cadence

#### 7.8.1 Predictor vector rebuild

Drop the table at lines 504-517 entirely. Rebuilt below from the actual ERA5 variables available in the project (user-provided list). Resampling rules use **linear interpolation in time** for ERA5 (hourly → 30-min slot centre); see §3 of this doc for the rationale.

| #   | Predictor  | Source                  | Native res.       | Pre-processed to                                  |
| --- | ---------- | ----------------------- | ----------------- | ------------------------------------------------- |
| 1   | CAMS-AOD   | CAMS global forecasts   | 0.4° / 3-hourly   | 0.05° / 30-min (bilinear space + linear time)     |
| 2   | T2m        | ERA5                    | 0.25° / hourly    | 0.05° / 30-min (bilinear space + linear time)     |
| 3   | Td2m       | ERA5                    | 0.25° / hourly    | 0.05° / 30-min                                    |
| 4   | RH         | ERA5                    | 0.25° / hourly    | 0.05° / 30-min                                    |
| 5   | Psfc       | ERA5                    | 0.25° / hourly    | 0.05° / 30-min                                    |
| 6   | U10        | ERA5                    | 0.25° / hourly    | 0.05° / 30-min                                    |
| 7   | V10        | ERA5                    | 0.25° / hourly    | 0.05° / 30-min                                    |
| 8   | PBLH       | ERA5                    | 0.25° / hourly    | 0.05° / 30-min                                    |
| 9   | CloudCover | ERA5 total cloud cover  | 0.25° / hourly    | 0.05° / 30-min                                    |
| 10  | TCWV       | ERA5 total column water | 0.25° / hourly    | 0.05° / 30-min                                    |
| 11  | SolarRad   | ERA5 surface downward   | 0.25° / hourly    | 0.05° / 30-min                                    |
| 12  | Albedo     | ERA5 forecast albedo    | 0.25° / hourly    | 0.05° / 30-min                                    |
| 13  | Precip     | **GPM IMERG**           | 0.1° / **30-min** | 0.05° / 30-min (bilinear space only; native time) |

Static covariates (cell-level, time-invariant within a year): elevation (Copernicus GLO-30), land cover (MCD12Q1 annual), population (LandScan annual), lat (rad), lon (rad).

Time-varying but slow (treat as static per slot, refresh every 16 days): **NDVI (MOD13C1, 16-day, nearest-in-time)** — per user direction, NDVI is varied by time, not pinned static.

**Total: 13 dynamic + 5 static + 1 quasi-static (NDVI) = 19 predictors.** Drop the "12 predictors" framing from the old §7.8.1 since it referenced Youn's set, which the new vector deliberately diverges from (see §4 below).

#### 7.8.1 Training data construction

Replace lines 523-524 with:

> Training pairs (X, y) come from the Stage A 30-min merged product on Sep 2022 – Dec 2024, daytime slots only (per §0's data-driven window, ~21 slots/day on average). For each (cell, slot) where Stage A AOD is valid, y = the merged AOD value and X = the 19-predictor vector at that point. No daily aggregation. Total training rows ≈ 2.4 × 10⁷.
>
> **Inference loop.** After training, for each UTC day `D` in the full timespan (Sep 2022 – Apr 2026), determine `[slot_first, slot_last]` from Stage A filenames (§0) and run the RF at every (cell, slot, D) with `slot ∈ [slot_first, slot_last]` where Stage A is missing. Observed cells/slots pass through unchanged. Result: a dense per-day-window product matching the B1 ST kriging output schema.

#### 7.8.1 Hyperparameter selection

See **§0.5** for the shared train/test protocol. RF hyperparameter selection (`n_estimators`, `max_depth`, `min_samples_leaf`, `max_features`) uses **5-fold contiguous temporal CV** on the train partition, identical to ST kriging's CV structure. Selection criterion is mean CV-R² across the 5 folds.

**Do not use OOB-R²** (sklearn's `oob_score=True`). OOB sampling is IID bootstrap; AOD is strongly temporally autocorrelated; OOB-R² would be systematically optimistic and is not comparable to the temporal-block-CV numbers reported for ST kriging. Set `oob_score=False` explicitly in the RF config. Note that this departs from Youn 2024's reporting convention (they reported OOB-R²); the departure is principled given AOD's autocorrelation structure.

#### 7.8.1 Expected performance

The "RMSE 0.18-0.25 at Nghia Do" prior was anchored on daily-collapsed predictors. At slot cadence with time-matched ERA5, expect performance closer to (though probably still worse than) Youn 2024's hourly numbers (RMSE 0.064 cell-blind, 0.208 AERONET-blind). New prior: hold expected RMSE bounds open until the first §8.2.1 internal CV run lands; record the comparison vs Youn explicitly in §8.2.6.

### §7.8.2 — Candidate 2 (Chen 2023 SIM + TIM): DELETE entirely

Chen 2023's TIM solves a problem this thesis doesn't have. TIM is a **temporal upsampler** that recovers 10-min cadence after the SIM step coarsened things to hourly. It does not fill temporal gaps in a cell's time series — SIM does that, by running independently at every hour. By the time TIM runs, every cell already has complete coverage at every hourly anchor; TIM just interpolates between anchors to finer time steps.

Our pipeline does not need this:

- Stage A already outputs 30-min slots (not hourly), so there is no "coarsening" step to undo.
- The Candidate 1 RF, run independently at every 30-min slot, already produces full spatiotemporal coverage at 30-min as a side effect (per the SIM pattern).
- A TIM analogue would upsample 30-min → 10-min, which would (a) implicitly become a Himawari-only product at sub-30-min cadence since VIIRS/MAIAC don't observe that frequently, (b) fabricate sub-30-min variability that the multi-sensor merge can't justify, and (c) deliver no downstream value because PM₂.₅ users want hourly/daily.

**Action: delete §7.8.2 from the plan entirely.** Also remove:

- §7.8.4 (selection criterion between two candidates — no longer needed; §7.8.3 rewrite handles this by shipping methods as parallel products instead).
- The "two candidates" framing in §7.8 intro (lines 489-496).
- The Chen 2023 reference in §2 stays (SIM is still the RF's template), but the SIM/TIM split is reduced to a one-line footnote: _"Chen 2023's full pipeline includes a TIM upsampling step (hourly anchors → 10-min); not applicable here because Stage A already operates at 30-min cadence and downstream PM₂.₅ workflows do not require sub-30-min AOD."_
- (The `aod_ml_dnn_alt`, `ml_dnn` enum option, and any other Candidate-2-era output fields are subsumed by the §7.8.3 wholesale rewrite below — they don't need separate deletion instructions.)

**Bonus note for the new §7.8 (RF-only Stage B):** the per-slot RF has no temporal constraint linking adjacent slots in the same cell, which can produce noisier-than-physical filled time series. If §8.2.5's case-study coherence check shows this is a real problem, the fix is a simple **3-slot rolling-mean smoother on the RF output** as a post-process — much simpler than TIM and addressing an actual issue rather than a phantom one. Flag this in §10 as a potential v2 refinement, not a v1 requirement.

### §7.8.3 — Output schema, rewritten

**Old design (v3.4.0):** one merged product with a `gap_fill_method` enum picking the best method per cell, method-specific confidence columns side-by-side, and an `aod_kriging_baseline` co-shipped on every row. This was an _operational_ product design — appropriate if Stage B's deliverable is "the single best AOD value for each cell." It is **not** appropriate for a thesis whose deliverable is evaluating two gap-fill methods against each other.

**New design.** Two **parallel product trees**, one per method, both dense and same-shape. Each method trains, infers, and writes its own complete gap-filled product. Stage C evaluation then loads both side-by-side and compares them against held-out AERONET and against each other.

**Directory layout:**

```
output/
├── st_kriging/                       # Yang & Hu 2018 baseline
│   ├── 2022/09/01/
│   │   ├── aod_20220901_0000.nc      # slot 00:00 UTC
│   │   ├── aod_20220901_0030.nc      # slot 00:30 UTC
│   │   ├── …                          # ~20–22 slots/day (data-driven, §0)
│   │   └── aod_20220901_1030.nc      # slot 10:30 UTC (window end varies by day)
│   ├── 2022/09/02/…
│   └── 2026/04/30/…
│
└── rf/                                # Random Forest, Youn-style
    ├── 2022/09/01/aod_20220901_0000.nc
    ├── …
    └── 2026/04/30/aod_20260430_1030.nc
```

~28,000 files per method (1,330 days × ~21 slots/day average), ~56,000 files total. Each file is small (Vietnam 0.05° grid is ~150 × 90 cells ≈ 13,500 points × a few variables ≈ tens of KB compressed).

**Per-file contents (single 30-min slot, both methods identical structure):**

| Variable             | Dims       | Meaning                                                                                                                                                                                                                    |
| -------------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `aod_550nm`          | (lat, lon) | Filled AOD value (Stage A observation if available, else model prediction)                                                                                                                                                 |
| `is_observed`        | (lat, lon) | Boolean: true = passed through from Stage A; false = model-filled                                                                                                                                                          |
| `uncertainty`        | (lat, lon) | Per-method: ST kriging variance for `st_kriging/`, RF per-tree prediction SD for `rf/` (standard deviation across the ensemble's individual tree predictions — does not use OOB samples; see §0.5). NaN on observed cells. |
| `stage_a_weight_sum` | (lat, lon) | Stage A ICW weight sum, only on observed cells (NaN elsewhere). Same in both products — provenance carried forward from Stage A.                                                                                           |

**Coordinates:** `lat`, `lon` (Vietnam 0.05° grid). Time is implicit in the filename (single-slot file) but also stored as a scalar global attribute for safety.

**Global attributes per file:**

```
slot_utc        = "2022-09-01T00:00:00Z"
slot_index      = 0           # 0..19 within the day
method          = "st_kriging" | "rf"
method_version  = "v1.0"      # model checkpoint / variogram fit hash
training_window = "2022-09 to 2024-12"
created_at      = ISO timestamp
```

**Why this is better than the merged-schema design I had earlier:**

1. **Direct head-to-head comparison.** Stage C can `xarray.open_mfdataset()` both trees, align by coordinate, and compute per-cell `rf - st_kriging` differences, paired error vs AERONET, paired skill scores — all trivial because the products are isomorphic.
2. **No method-selection logic in the product.** Picking the "best" method per cell is an analysis question for Stage C, not a Stage B data structure.
3. **Each method is a clean, self-contained deliverable.** If a downstream PM₂.₅ user wants the RF product only, they grab `output/rf/` and that's it. If a sensitivity analysis wants the kriging-only path, same deal.
4. **Single uncertainty column per file.** No more "this column is meaningful on some rows but not others" hazard.
5. **Storage format matches analysis tools.** Per-slot NC files are the de facto standard for satellite gridded products; xarray, CDO, NCO, GDAL all consume this layout natively. Glob-by-day, glob-by-month, mosaic into bigger zarr stores — all painless.

**One nuance — the `uncertainty` variable.** Its units and interpretation differ between methods (variance for kriging in AOD² units; standard error for RF in AOD units). The global attribute `uncertainty_units` should document this per-file. Don't try to unify them numerically; they measure different things and forcing a common scale would be misleading.

**Daily / monthly aggregations for RQ4 PM₂.₅ comparison.** Compute as a Stage C post-processing step from each method's NC files independently:

- `output_aggregated/st_kriging_daily/2022/09/01.nc` — daily mean over the day's observation window (~21 daytime slots, per §0)
- `output_aggregated/rf_daily/2022/09/01.nc` — same for RF
  Both daily products then go into RQ4's R² comparison vs Nguyen 2025. This way the RQ4 comparison is itself a head-to-head: "does RF or ST kriging give better daily PM₂.₅ R²?"

### §8.0 — AERONET colocation, line 601

Replace `and across the local day for Stage B` with `and within ±30 min of the slot centre for Stage B (identical protocol to Stage A)`.

This unifies Stage A and Stage B validation under a single AERONET match rule, which is the cleanest outcome of the cadence fix.

### §8.2 — Stage B validation

- §8.2.2 AERONET-blind validation: now operates on filled 30-min slots, not filled days. The §8.0 metric panel runs at slot cadence.
- §8.2.5 case-study stress test: choose a cloud-occluded _window_ (e.g., 48 consecutive missing daytime slots ≈ **2.3 days** of complete daytime occlusion at ~21 slots/day) rather than a cloud-occluded _day_. The granularity is finer; the test is correspondingly more discriminating.

### RQ4 (§1.2 line 32) — comparison vs Nguyen 2025

The R² = 0.293 baseline is daily. To honour the comparison without contaminating Stage B:

- Run Stage B fully at 30-min.
- For the RQ4 comparison only, aggregate the 30-min gap-filled product to daily mean as a **post-processing step** (described in §8 wherever the Nguyen comparison lives).
- This gives you both: the 30-min product as the headline contribution **and** a directly comparable daily roll-up for the PM₂.₅ R² comparison.

### §10 caveats

- **Caveat 1** (~10% AOD availability): unchanged but reword "~90% of the daily product" → "~90% of the 30-min product (and ~50-70% of any daily rollup depending on the slot-count floor applied downstream)."
- **Caveat 8** (gap-filled values are model estimates): unchanged.
- **Drop the implicit "3-slot floor" rationale** (was in old §7.6) — no longer applicable.
- **Add: ST kriging temporal-window caveat.** ST kriging weights neighbours within a `W_t = 24-hour` window. Slots near the start of the dataset (first 24 hours of Sep 2022) lack a temporal back-window and reduce to forward-only ST kriging or climatology fallback; flag the first day as warm-up in any analysis. Symmetric warm-down at the very end of the dataset.
- **Add: per-slot RF temporal-coherence caveat.** Per-slot RF has no inter-slot temporal constraint; a 3-slot rolling-mean smoother on output is a v2 refinement if §8.2.5 shows excessive slot-to-slot noise.

### §11 scope management — line 818-823

- "Steps B1 + B2 (daily aggregation + kriging spatial gap-fill)" → "Step B1 (**spatiotemporal kriging per 30-min slot, Yang & Hu 2018 sum-metric model**, also serves as ML evaluation baseline)."
- "Step B3 Candidate 1 (RF gap-filling)" → "Step B2 (RF gap-filling per 30-min slot)" — promote from "high priority stretch" to **in-scope (must complete)** since it's now the only ML step and the headline B-stage deliverable.
- "Step B3 Candidate 2 (RF + DNN, Chen 2023 style)" under stretch/future work → **delete the bullet entirely** (Candidate 2 is gone, not deferred).
- "If the timeline shrinks, Step B3 is the first thing to drop" → "If the timeline shrinks, Step B2 ML is the first thing to drop and Step B1 ST kriging alone produces a usable 30-min product (Yang & Hu reported 67% completeness from ST kriging alone at daily cadence; expect 40-60% at our 30-min cadence)."

---

## 3. ERA5 hourly → 30-min slot: what to do

The user's question. Three options, in order of preference:

**(a) Linear interpolation between flanking hourly fields (recommended).**
For the slot centred at `HH:30`, use `0.5 × ERA5(HH:00) + 0.5 × ERA5(HH+1:00)`. For slots at `HH:00`, use `ERA5(HH:00)` directly. This is the simplest defensible choice and is exactly the philosophy Youn 2024 used going from 3-hourly LDAPS → hourly via cubic spline (their Fig. 1). Linear is sufficient at the hourly → 30-min step because the gap is small and ERA5's smooth meteorology has very little curvature over one hour.

**Per-variable notes** (covers all ERA5 variables in the project's collection, including those not in the v1 predictor list — handling rules in case they're added in v2):

- T2m, Td2m, RH, Psfc, MSLP, TCWV, U10, V10, PBLH, SolarRad, Albedo, CAPE: linear interp is fine. All vary smoothly hour-to-hour.
- CloudCover, CBH: linear interp is acceptable. Cloud fields are noisier in time, but ERA5's analysis already smooths over the noise; a 30-min linear interp doesn't introduce more error than the underlying field carries.
- Precip: **do not interpolate ERA5 precip.** Use **IMERG natively at 30-min** instead. IMERG is already at the target cadence, with much better spatial resolution (0.1° vs 0.25°) and better physical meaning for instantaneous precip than ERA5's hourly accumulations. The plan already has IMERG (§4.4); just use it for the precip predictor and skip ERA5's Precip variable entirely.

**(b) Nearest-neighbour / hold-flat (simpler, slightly worse).**
The `HH:30` slot inherits the `HH:00` hourly value (or `HH+1:00`, depending on rounding rule). Functionally fine but creates a step discontinuity at the hour boundary that the RF will see as a feature break. Use only if (a) is implementation-expensive.

**(c) Cubic spline (slightly fancier, marginal gain).**
Mirror Youn 2024 exactly. Adds complexity for variables where the curvature over 1 hour is negligible. Not worth it unless you have a specific reason.

**Recommendation: go with (a) for all ERA5 variables and IMERG-native for precipitation.** Document this in the new §7.8.1 predictor table (already reflected above).

---

## 4. Variable list reconciliation

User-confirmed: **MERRA-2 is not a Stage B predictor** (already used in Stage A §7.4.1 — using it again as a gap-fill predictor would double-dip on the same reanalysis signal and risk circular validation, since MERRA-2 calibrated the satellite that produced the Stage A field that the gap-fill is now learning from). So CAMS is the only reanalysis-AOD predictor in Stage B. This is a defensible choice and worth a one-sentence note in §7.8.1 explaining the asymmetry.

**vs Youn 2024's 12 vars (CAMS, MERRA-2, TMP, U_WS, V_WS, BLH, LHFL, RH, HCDC, DSSF, PRES, DPT):**

- ✔ Have: CAMS, TMP (T2m), DPT (Td2m), RH, U_WS (U10), V_WS (V10), BLH (PBLH), DSSF (SolarRad), PRES (Psfc).
- ✘ Missing: **LHFL** (surface latent heat flux). Not in the user's ERA5 list. Decision: skip for v1, add as future work if model performance is poor. The dominant signal LHFL carries (atmospheric stability / boundary-layer dynamics) is partially captured by PBLH and Td2m already.
- ⚠ Substituted: Youn used **HCDC** (high cloud cover); user has **CloudCover** (total cloud cover). Total is broader; note the substitution in §7.8.1. Not expected to be a major loss — Youn reported HCDC as one of the lowest-importance vars at 2.55% relative importance.
- ✘ Excluded by user choice: **MERRA-2** (already used in Stage A, see above).

**Extra ERA5 variables the user has that Youn didn't:**

- MSLP, WS10m, WD10m, U100, V100, CBH, TCWV, CAPE, Albedo.
- Recommendation for v1 predictor table: include **TCWV** (water-vapour column — physically relevant for aerosol hygroscopic growth and a known good predictor in similar studies) and **Albedo** (Chen 2023 used it; relevant for surface-reflectance bias). Skip the rest for v1 to keep the predictor count tight; they're cheap to add in a v2 sweep if RF variable importance suggests it.
- MSLP is essentially redundant with Psfc plus elevation; drop.
- WS10m and WD10m are derivable from U10, V10; RF can learn this internally; drop to avoid multicollinearity.
- U100, V100, CBH, CAPE: potentially useful in a monsoon-transport context but outside the Youn / Chen precedent; defer to v2.

---

## 5. The cleanup checklist (one-line summary per edit)

- [ ] Delete §7.6 (Step B1 daily aggregation) entirely.
- [ ] Renumber B2 → B1, B3 → B2, OR keep section numbers and just remove old §7.6 body.
- [ ] **Replace §7.7 entirely** with the spatiotemporal kriging design (Yang & Hu 2018 sum-metric variogram, moving window, R `gstat` reference implementation). Old per-slot spatial kriging + climatology Approach A/B framing is gone.
- [ ] Add citation to **Yang & Hu (2018), _Sci. Total Environ._ 633, 677-683** in the plan's reference list (and any §2 methodological-precedents list).
- [ ] Rewrite §7.8.1 predictor table per the 13-row table above; drop MERRA-2; substitute CloudCover for HCDC; skip LHFL; add TCWV and Albedo as v1 additions.
- [ ] Rewrite §7.8.1 training-data section: pairs are (cell, slot), not (cell, day); daytime-only (per §0's data-driven per-day window from Stage A filenames, ~21 slots/day average); training table size ~2.4 × 10⁷ rows.
- [ ] Add §7.8.1 paragraph on ERA5 → slot temporal handling (linear interp; IMERG native for precip).
- [ ] **DELETE §7.8.2 (Candidate 2 / Chen 2023 SIM + TIM) entirely.**
- [ ] **DELETE §7.8.4 (selection criterion between candidates).**
- [ ] §7.8.3 output schema: **complete rewrite** to per-method parallel products. Two output trees (`output/st_kriging/` and `output/rf/`), per-slot NC files (~28,000 each, slots/day data-driven per §0), identical 4-variable layout (`aod_550nm`, `is_observed`, `uncertainty`, `stage_a_weight_sum`). Delete the merged-product schema with `gap_fill_method` enum, the polymorphic `confidence` column, and the co-shipped `aod_kriging_baseline`. See §7.8.3 rewrite.
- [ ] §7.8 intro (lines 487-496): remove the "two candidates" framing; reduce Chen 2023 reference to a one-line footnote ("TIM upsamples hourly→10-min; not applicable since Stage A is already 30-min").
- [ ] §8.0 line 601: Stage B AERONET temporal match is ±30 min slot centre, same as Stage A.
- [ ] §8.2.1 (kriging-baseline metrics): update to ST kriging — cross-validation strategy follows §0.5's 5-fold temporal CV (not Yang & Hu's 10-fold), applied to (cell, slot) pairs with day-level fold assignment to prevent within-day leakage.
- [ ] §8.2.2 and §8.2.5: validation operates on filled 30-min slots, not filled days. §8.2.2 head-to-head between candidates collapses to a single-candidate evaluation (RF vs ST kriging baseline).
- [ ] §6 pipeline diagram (lines 168-215): update Stage B block; B1 label changes from "Spatial gap-fill" → "Spatiotemporal kriging (Yang & Hu 2018)".
- [ ] §3 output specification box: reword "aggregated outputs" line.
- [ ] §6 prose (line 217): "Stage B produces the 30-min gap-filled product."
- [ ] §10 caveat 1: reword "~90% of the daily product." Add ST-kriging warm-up caveat and per-slot RF temporal-coherence caveat.
- [ ] §11 scope management: update B1/B2 names; B1 is now "ST kriging"; promote RF gap-fill to in-scope; remove Candidate 2 from stretch/future-work bullets.
- [ ] RQ4 (§1.2 line 32 + wherever it lands in §8): note daily aggregation is post-processing for the comparison only.
- [ ] Sanity pass: search-and-replace any remaining "spatial kriging" / "per-slot kriging" / "kriging variogram per day" references → "ST kriging" or "spatiotemporal kriging" as appropriate; any remaining "daily" mentions inside Stage B references; any remaining Candidate-1-vs-Candidate-2 / DNN / TIM language across §§7-11.

---

## 6. Resolved items (formerly open for separate discussion)

All previously-open items now resolved:

- **Chen 2023 / Candidate 2 / TIM port (was open item #1).** Resolved: deleted, not deferred. TIM is a temporal _upsampler_ (hourly anchors → 10-min in-between), not a temporal _gap-filler_. Our pipeline doesn't need it: Stage A is already at 30-min cadence, the per-slot RF (Candidate 1, SIM pattern) already produces full spatiotemporal coverage as a side effect of running at every slot, and no downstream consumer wants sub-30-min AOD. Detailed action items in §2 (§7.8.2 deletion) and §5 checklist.
- **Kriging's role at slot cadence (was open item #2).** Resolved: keep it. ST kriging is shipped as a **standalone parallel product** in `output/st_kriging/` (see §7.8.3 rewrite). Stage C evaluates RF and ST kriging head-to-head against AERONET; the kriging product is the reference that RF has to beat to justify the ML step. The §11 scope-management line ("Step B1 ST kriging alone produces a usable 30-min product if Step B2 is dropped") covers the fallback role; the baseline-for-evaluation role is the primary reason it stays.
- **Slot-count floor for daily rollups (was open item #3).** Resolved: not applicable. Stage B is fully 30-min cadence — no daily aggregation lives anywhere inside the pipeline. The only place a daily rollup happens is the §8 / RQ4 post-processing comparison vs Nguyen 2025, and at that point the slot count is just a provenance field on the rollup output (no hard floor required).
