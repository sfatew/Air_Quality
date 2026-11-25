# Stage B — Implementation Changes vs Plan v3.5.2

Reference: [thesis_aod_vietnam_Plan_v3_5_2.md](../thesis_aod_vietnam_Plan_v3_5_2.md)

Deviations of live code in [AOD_map/stage_b/](.) from the v3.5.2 Stage B design. Experimental knobs currently *off* in [config.py](config.py) — decile-rebalance sample weights (`RF_USE_DECILE_REBALANCE_WEIGHTS = False`), the ST-AOD-context features (`obs_aod_mean_5x5`, `obs_aod_count_5x5`, `persistence_decay`, `slots_since_obs`), `fire_frp_log_24h`, `region_idx`, `RF_TUNE_REPORT_TRAIN_RMSE`, `train-rmse` — are not documented here.

---

## 1. B3 regression kriging is a third parallel product (`output/rf_rk/`)

**Plan §9 / §6:** the deliverable is **two** parallel gap-filled product trees — `output/st_kriging/` (B1) and `output/rf/` (B2).

**Implementation:** a third product tree, `output/rf_rk/`, is shipped alongside them. It is **regression kriging**: the RF prediction ŷ_rf is used as the drift term and the RF *residual* is kriged with its own variogram. Structurally:

* [kriging.py:81](kriging.py#L81) exposes `_TREND_KINDS = ('aod_minus_cams', 'aod_minus_rf')`; `'aod_minus_rf'` triggers the RK path.
* [kriging.py:458-481](kriging.py#L458-L481) — `fit_and_save_rk_variogram_oof` fits the RK variogram on **out-of-fold** RF residuals from a single temporal holdout inside the train window (in-sample residuals would be degenerate; test would leak). Persists to `MODELS_DIR/st_variogram_rf.json`.
* [rf_gapfill.py:762-822](rf_gapfill.py#L762-L822) — `write_pred_slot` / `predict_range` build a full-grid RF-prediction tree in `RF_PRED_DIR` so the ±time neighbourhood in kriging can read the drift instead of re-running the RF ~20-40× per overlap.
* [rf_gapfill.py:834-905](rf_gapfill.py#L834-L905) — `collect_oof_residual_pairs` returns the (lat, lon, t_h, residual) tuples the RK variogram is fit against.
* [kriging.py:721-727](kriging.py#L721-L727) — **RK graceful fallback**: gap cells whose residual pool is empty fall back to the pure RF drift ŷ_rf (kriged residual r̂ → 0). So RK is by construction never worse than the RF baseline on gap cells.
* [run_stage_b.py:115-163](run_stage_b.py#L115-L163) — new CLI verb `--rk` orchestrates the three stages (OOF variogram fit → drift precompute over apply window ±1 day → residual krige + drift add-back).

`★ Insight ─────────────────────────────────────`
This is the classic regression-kriging move (Odeh 1994; Hengl 2007): the drift model captures the covariate-driven mean field (what RF is good at — non-stationary structure), the kriged residual patches short-range spatial correlation the drift missed (what ST kriging is good at — geostatistical infilling). The RK-specific piece is fitting the variogram on *OOF* residuals: in-sample train residuals have a shrunken sill and near-zero nugget (memorised bias) which would make the kriger under-correct.
`─────────────────────────────────────────────────`

Kriging win against RF in Blind AERONET test:

Think about what actually separates the two on the blind-AERONET test. AERONET AOD ≈ satellite AOD — they measure nearly the same thing — so the single best predictor of the held-out AERONET value is the surrounding satellite AOD field itself. Kriging uses that field directly, with covariance-distance weighting that's close to optimal for an autocorrelated field. RF only gets that field through the neighbor feature you added — and a 5×5 unweighted box mean is a strictly worse interpolator than kriging's weighted one. So you handed RF a crude version of exactly the thing kriging already does well, and it did two bad things at once: it couldn't match kriging's interpolation, and leaning on it displaced the CAMS/met signal that was RF's actual comparative advantage. Add the variance cost of 4–6 partially-missing features on a small AERONET set and a train/test regime mismatch (training cells have dense neighbors, blind gap-cells don't), and "worse" is the expected outcome, not a surprise.
The reframe: RF has something kriging lacks (a covariate-driven trend — CAMS, ERA5 met, emissions, elevation — that holds where there are no neighbors), and kriging has something RF lacks (proper local interpolation of the real field). These are complementary. Don't pick a winner; compose them. The standard geostatistical answer is regression kriging (RF trend + kriged residuals), and it's the defensible thesis method here:

Walk through it concretely at one target cell:

**Step 1 — RF predicts AOD from covariates only.** No neighbor AOD features (throw those failed ones away). Just CAMS, ERA5, elevation, etc. Say it predicts `ŷ_rf = 0.60` at your target cell. It also predicts at *every observed cell*.

**Step 2 — measure how wrong RF is, at the cells where you know the truth.** At each observed Stage A cell, `residual = actual_AOD − RF_prediction`. So maybe:

```
cell A: actual 0.85, RF said 0.70  → residual = +0.15
cell B: actual 0.90, RF said 0.78  → residual = +0.12
cell C: actual 0.55, RF said 0.62  → residual = −0.07
```

These residuals are a *new field* — a map of "RF runs low by ~0.13 around here." That's structure RF systematically missed (the Hanoi haze it can't see from coarse CAMS).

**Step 3 — krige the residual field**, not the AOD field. Same kriging machinery you already have, but you feed it those `+0.15, +0.12, −0.07…` numbers instead of raw AOD. It interpolates to your target cell and says `r̂ = +0.13`.

**Step 4 — add the correction to the RF prediction:**

```
ŷ = ŷ_rf + r̂ = 0.60 + 0.13 = 0.73
```

You added `0.60` (a prediction of AOD) to `0.13` (a prediction of *RF's error*). Not two AOD predictions. The residual just happens to be in AOD units because it's a difference of AODs, which is what makes the addition valid.

Why this beats each alone:
- Near NGHIA_DO with dense obs, the residuals nearby are real and kriging recovers the haze structure → you get kriging's strength *plus* RF's baseline.
- In a real gap with no nearby obs, kriging has no residuals to interpolate so `r̂ → 0`, and you fall back to `ŷ = ŷ_rf`. Graceful.

Contrast with what you were about to do:

```
WRONG:  ŷ = ŷ_rf + ŷ_krig        (0.60 + 0.70 = 1.30, nonsense)
ENSEMBLE (not this): ŷ = (ŷ_rf + ŷ_krig)/2   (just averaging, no structure)
REGRESSION KRIGING:  ŷ = ŷ_rf + krige(residuals)   (RF baseline + map of RF's mistakes)
```

why does this works?

Because it splits the field into two parts and gives each part to the tool that's actually good at it. Three reasons, and the second one is the one that makes it *principled* rather than just a clever hack.

**1. Any spatial field decomposes into trend + local structure.**

Write the true AOD at any location as:

```
AOD(s) = trend(s) + correlated_residual(s) + noise
```

The trend is the slow, explainable part — Hanoi is systematically hazier than the coast because of emissions, humidity, boundary-layer met. That's a *function of covariates*, and it holds everywhere, including inside gaps. The residual is the fine local wiggle the covariates can't explain — today's specific plume, drifting a bit east.

RF is a trend machine: it learns AOD-as-a-function-of-covariates and evaluates it anywhere, obs or no obs. Kriging is a local-structure machine: it reads the actual field near a point and interpolates it. Neither does both well. Regression kriging assigns RF to the trend and kriging to the residual — each does the job it's built for instead of being forced to do the whole job.

**2. Kriging actually works *better* on the residuals than on raw AOD — this is the real reason, not bookkeeping.**

Kriging's math assumes stationarity: roughly constant mean across the field, with spatial covariance depending only on separation distance. Raw AOD over Vietnam badly violates that — the mean is *not* constant, Hanoi sits on a persistent high. When you krige raw AOD you're applying a stationary method to a non-stationary field, and it fights the trend with a locally-estimated mean, which is a weak patch.

Subtract the RF trend first and the residual field is approximately zero-mean and much closer to stationary — which is exactly the regime kriging's assumptions are valid in. So you're not just adding a step; you're feeding kriging the kind of field its equations were derived for. The variogram you fit on residuals is cleaner and more interpretable than one fit on trend-contaminated raw AOD (raw variograms often don't sill because the trend keeps adding variance with distance). This is the part a committee will nod at: you moved kriging from a setting where its assumptions are violated to one where they hold.

**3. The two tools' weaknesses sit in *different places*, so the composite never has both failing at once.**

This is the geometry that makes it robust:

- **Dense-obs region (NGHIA_DO interior):** residuals are available nearby, kriging is in its strong regime, `r̂` is a real correction that recovers the haze structure. RF trend + genuine local correction.
- **Real contiguous gap (no nearby obs):** kriging has nothing to interpolate, `r̂ → 0` automatically, and you fall back to the RF trend — which is the *only* signal available there anyway, and RF's actual comparative advantage.

Kriging is strong exactly where RF is weak (dense field, fine structure) and silent exactly where RF is all you've got (empty gap). Their competencies are anti-correlated in space. Compare that to your failed neighbor-feature RF, where in a gap the neighbor mean went missing/sentinel *and* RF lost the covariate signal — both failure modes hit the same cell. Regression kriging structurally can't do that.

The error-compounding worry, since it's the natural objection: you're adding two estimates, don't the errors add? Not really, because they estimate *orthogonal components*. RF's error is on the trend; kriging's error is on the residual; they're largely independent, and each is smaller than the error either model makes trying to do the entire job solo. You're not stacking two full-field errors, you're pairing two partial-field errors that live in different subspaces.

One honest caveat so you don't oversell it: this works because AOD genuinely *has* exploitable local autocorrelation for the residuals to carry. At NGHIA_DO it does — that's why kriging was already winning there. At Bac_Lieu the field is low-autocorrelation to begin with, so the residual field is close to noise, `r̂` carries little, and regression kriging collapses back toward plain RF trend. It won't rescue Bac_Lieu — nothing will, because the signal isn't there. Which is consistent with everything else: the gap-fill skill claim is a NGHIA_DO claim, and regression kriging is what lets you make it defensibly.


The plan's §8 head-to-head machinery only knows about `rf` and `st_kriging` candidates ([validate.py](validate.py) has `aeronet_pairs(..., candidate=)`). The RK product ships alongside them but has to be added into the intercomparison notebook manually.

IMPORTANT NOTE: Kriging are no longer a Baseline, it's a contender. An i works better then RF, which lead to regression-kriging move


## 3. New paths and CLI verbs beyond the plan's B1/B2 story

Beyond the plan's `output/st_kriging/` and `output/rf/`, [config.py:64-77](config.py#L64-L77) declares:

| Path | Populated by | Role |
|------|--------------|------|
| `RF_PRED_DIR = STAGE_B_DIR/output/rf_pred` | `rf_gapfill.predict_range` | Full-grid ŷ_rf per slot — RK drift field, computed once so the kriging temporal neighbourhood can re-read it |
| `RF_RK_DIR = STAGE_B_DIR/output/rf_rk` | `kriging.run(..., target_kind='aod_minus_rf')` | The B3 product tree |
| `AGGREGATED_DIR = STAGE_B_DIR/output_aggregated` | (not populated by Stage B code) | Reserved for the §8.2.6 post-processing daily roll-ups + physics normalisation |

[run_stage_b.py](run_stage_b.py) picks up:

* `--rk` — the whole B3 chain (§1 above).
* `--infer-only` — skip variogram refit / RF tune, reuse persisted artefacts. Useful for re-running slots after an ancillary backfill without paying for training again.
* `--no-tune` — B2 only; skip the RF_GRID search and train once with the `RF_*` defaults.
* `--start` / `--end` — override the apply window (defaults to `TEST_START`/`TEST_END`).

None of these are in the plan's runbook.

---

## 4. `MODIS_DIR` note aside — the plan Ancillary table has one substitution

**Plan §4.4:** MERRA-2 is excluded as a Stage B predictor "already consumed by Stage A".

**Implementation:** matches — [config.py:210-241](config.py#L210-L241) never imports MERRA-2. This is a *conformance* note, not a deviation, but worth flagging because during the Stage A → Stage B pivot the constraint is easy to miss. The [feedback memory on this rule](../../.claude/memory) references the same principle.

The plan's 24-feature list ([Plan §7.8.1](../thesis_aod_vietnam_Plan_v3_5_2.md#L537)) is honoured verbatim in `RF_FEATURES_DYNAMIC + RF_FEATURES_STATIC + RF_FEATURES_QUASISTATIC`. 20 + 3 + 1 = 24 features.


## Summary of config switches driving the deviations

| Switch | Value | Drives |
|--------|-------|--------|
| `RF_PRED_DIR`, `RF_RK_DIR` present in config | new paths | §1 — B3 RK product tree |
| `RF_GRID` | 2 combos | §2 — narrowed sweep |
| `RF_N_ESTIMATORS` | `200` | §2 — bumped default |
| `AGGREGATED_DIR` | defined but unused inside Stage B | §3 — reserved for §8.2.6 post-processing |
