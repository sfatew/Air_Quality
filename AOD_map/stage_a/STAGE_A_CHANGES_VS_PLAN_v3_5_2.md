# Stage A — Implementation Changes vs Plan v3.5.2

Reference: [thesis_aod_vietnam_Plan_v3_5_2.md](../thesis_aod_vietnam_Plan_v3_5_2.md)

This doc lists where the live code in [AOD_map/stage_a/](.) deviates from the v3.5.2 plan. Only switches currently *on* in [config.py](config.py) are listed — experimental flags left off (e.g. `SOFT_CAL_ACCEPT_ALL_MERRA2`, the disabled race baseline drop, the commented-out cross-anchor cap) are not documented.

---

## 1. Himawari L2 and L3 are first-class fusion sensors — no pre-fusion merge (v3.6)

**Plan §4.1, §7.4.3, §7.5:** "Himawari L2 vs L3 soft-calibrated independently and merged per-pixel by σ²_TC" — i.e. a *pre-fusion* L2/L3 merge that yields a single Himawari channel into TC fusion. `dominant_sensor` code "1 = Himawari (merged L2/L3)".

**Implementation:** L2 and L3 are each one of **five** equal-rank fusion inputs (`_PRODUCTION_SENSORS` in [fusion.py:48](fusion.py#L48); `_SENSOR_KEYS` in [run_stage_a.py:78-82](run_stage_a.py#L78-L82)). There is no per-pixel L2/L3 reconciliation step before fusion — the inverse-variance combiner arbitrates between them the same way it arbitrates between any LEO pair.

`SENSOR_CODES` in [config.py:275-281](config.py#L275-L281) reflects this:

| Code | Sensor |
|------|--------|
| 1 | `himawari_l2` |
| 2 | `himawari_l3` |
| 3 | `modis_maiac` |
| 4 | `viirs_snpp` |
| 5 | `viirs_noaa20` |

So the `dominant_sensor` field on the Stage A NetCDF can now report L2 and L3 separately. Plan §7.5 caveat #17 ("no per-pixel L2-vs-L3 provenance flag in the merged product") no longer applies.

---

## 2. Race mode replaces "AERONET fallback only on MERRA-2 failure" (`SOFT_CAL_RACE_ANCHORS = True`)

**Plan §7.4.1.1:** AERONET-anchored fit is a *fallback* — it fires only if the MERRA-2 anchor routes to `'none'`. A cross-anchor cap then guards against single-station AERONET fits diverging wildly from the full-band MERRA-2 view.

**Implementation:** Both anchors are fit in parallel for every (sensor, region, season) stratum that has AERONET pairs (north, south — central has no station and degenerates to MERRA-2-only). The winner is whichever passes its own gate **and** has the lower AERONET-truth RMSE. See [bias_correction.py:538-574](bias_correction.py#L538-L574) (`_select_anchor`) and [bias_correction.py:627-669](bias_correction.py#L627-L669).

Race rules in effect:

* MERRA-2 candidate is scored as `rmse(α_m·sat_aer + β_m, aer)` — fully out-of-sample.
* AERONET candidate is scored as `rmse_aer_after_cv` (mean k-fold fold-out RMSE on AERONET).
* Lower wins, strict (no tie-ε).
* The cross-anchor α/β cap (`AERONET_VS_MERRA2_ALPHA_RATIO_*`, `AERONET_VS_MERRA2_BETA_MAX_DIFF`) is **not** applied in race mode — head-to-head AERONET-RMSE is the arbiter instead. (The cap constants still live in config for the non-race path, which is currently dormant.)
* When AERONET coverage is too thin to define a baseline RMSE, the race degenerates to MERRA-2's own gate.

`★ Insight ─────────────────────────────────────`
The race scores both anchors on the *same* AERONET held-back samples, which is the actual §8 validation target. The plan's fallback-only design optimised each anchor against its own training target (MERRA-2 vs AERONET) and could therefore pick a MERRA-2-passing fit that happened to underperform AERONET on the very station the AERONET-anchored fit was tuned for. Race mode eliminates that.
`─────────────────────────────────────────────────`

`SoftCal` (the persisted record in `soft_calibration.json`) carries a new field to support the race:

* `rmse_merra2_on_aer` — out-of-sample RMSE of `α_merra2·sat + β_merra2` evaluated on AERONET pairs.

The pre-existing `anchor` field (`'merra2' | 'aeronet' | 'none'`) now records which side of the race the winning (α, β) came from, not just whether the fallback was used.

---

## 3. TC σ² is estimated in raw space and rescaled in fusion (`TC_INPUT_SPACE = 'raw'`)

**Plan §7.4.2:** Triple collocation is run *after* soft calibration is applied to each satellite member — σ² lives in calibrated space, and fusion uses `w = 1 / σ²_TC` directly.

**Implementation ([config.py:270](config.py#L270)):** TC is computed on the **raw** gridded AOD (Gruber 2016 §2.1, independence-clean — no shared MERRA-2 target in the residuals). Fusion then rescales:

```
σ²_used = α(lat)² · σ²_raw       per (sensor, region, season)
w        = 1 / σ²_used
```

Implementation:

* [triple_collocation.py:316-381](triple_collocation.py#L316-L381) — `collect_stratum_triplets` skips `apply_soft_calibration_grid` when `TC_INPUT_SPACE == 'raw'`.
* [fusion.py:122-156](fusion.py#L122-L156) — `_sigma2_grid` multiplies σ² by `α(lat)²` when an `alpha_lat` is provided.
* [fusion.py:237-243](fusion.py#L237-L243) — `fuse()` builds the per-sensor `α(lat)` via `bias_correction.build_alpha_lat` (which mirrors `SOFTCAL_BLEND_ENABLED`, so the cosine-tapered α propagates into σ² rescaling).
* `'none'` strata contribute identity α=1.0, so the existing `NONE_PENALTY_FACTOR` does not stack on top of an implicit α² penalty.

This means the variance term in `Var(α·X + β) = α²·Var(X)` is now handled explicitly rather than absorbed into the in-sample OLS regression that calibrated-mode TC silently assumed.

---

## 4. `NONE_PENALTY_FACTOR = 4.0` (plan: 2.0)

**Plan §7.4.1 / §7.5 / §10 #15:** Strata routed to `'none'` get `σ²_used = σ²_TC × 2.0` — fusion weight drops ~4×.

**Implementation ([config.py:206](config.py#L206)):** `NONE_PENALTY_FACTOR = 4.0` — weight drops ~16× per stratum routed to `'none'`. Considerably more aggressive de-weighting of strata where neither anchor (MERRA-2 or AERONET in race mode) could clear its gate.

---

## 5. Higher minimum-pair gates

| Knob | Plan | Implementation |
|------|------|----------------|
| `SOFT_CAL_MIN_PAIRS` (MERRA-2 anchor) | 100 | **500** ([config.py:86](config.py#L86)) |
| `AERONET_FALLBACK_MIN_PAIRS` | 40 (≥ `CV_FOLDS × 2`) | **200** ([config.py:122](config.py#L122)) |

The CV folds count is unchanged at 5, so both bumps simply reject thinly-supported strata earlier rather than relying on the α/β bounds to do the work after a noisy fit.

---

## 6. Per-sensor EE-envelope slopes (`SENSOR_EE_SLOPE`)

**Plan §7.4.2:** "σ²_floor = (EE_OFFSET + slope × AOD_ref)²" with a single shared slope (0.15 in the Levy 2013 reference). The plan does cite per-sensor literature envelopes in the §7.0.1 box-std table, but the TC EE floor uses one number.

**Implementation ([config.py:225-232](config.py#L225-L232)):** `ee_floor_sigma2(sensor)` in [fusion.py:54-63](fusion.py#L54-L63) picks the per-sensor slope from `SENSOR_EE_SLOPE`:

| Sensor | EE slope | Source |
|--------|----------|--------|
| `himawari_l2`, `himawari_l3` | 0.20 | Zhang 2019 |
| `viirs_snpp`, `viirs_noaa20` | 0.20 | Sayer 2019; Hsu 2019 |
| `modis_maiac` | 0.10 | Lyapustin 2018; Falah 2021 |
| `merra2` | 0.17 | Randles 2017 |

MAIAC therefore sits on a tighter floor than VIIRS/Himawari when its stratum falls back to the EE envelope.

This i have manually fixed in the thesis



## Summary of config switches currently driving the deviations

| Switch | Value | Drives |
|--------|-------|--------|
| `SOFT_CAL_RACE_ANCHORS` | `True` | §2 — anchor race instead of fallback-only |
| `TC_INPUT_SPACE` | `'raw'` | §3 — Gruber-clean σ², α²-rescale in fusion |
| `SOFTCAL_BLEND_ENABLED` | `True` | (already in plan §7.5; mirrored into σ² rescale via `build_alpha_lat`) |
| `NONE_PENALTY_FACTOR` | `4.0` | §4 — heavier de-weight on 'none' strata |
| `SOFT_CAL_MIN_PAIRS` | `500` | §5 |
| `AERONET_FALLBACK_MIN_PAIRS` | `200` | §5 |
| `SENSOR_EE_SLOPE` | per-sensor dict | §6 — per-sensor EE floor |
| `use_physics` default in `run_stage_a.py` | `True` | §9 — Stage A still emits physics fields |
