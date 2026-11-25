# AOD-map product flags

What every per-cell flag/mask in the output NetCDFs means — how to read "is this cell observed, filled, or merged, and from which sensors."

Four product trees:

| Tree                                      | Writer                                                            | What's in a cell                                        |
|-------------------------------------------|-------------------------------------------------------------------|---------------------------------------------------------|
| `MERGED_DIR` (`Stage_A[_hourly]/merged/`) | [stage_a/run_stage_a.py](stage_a/run_stage_a.py)                   | Stage A fused observation (where sensors saw cloud-free pixels) |
| `ST_KRIGING_DIR` (`Stage_B[_hourly]/output/st_kriging/`) | [stage_b/kriging.py](stage_b/kriging.py)              | Stage A obs where present, ST-kriged elsewhere          |
| `RF_OUTPUT_DIR` (`Stage_B[_hourly]/output/rf/`) | [stage_b/rf_gapfill.py](stage_b/rf_gapfill.py)                 | Stage A obs where present, RF prediction elsewhere      |
| `RF_RK_DIR` (`Stage_B[_hourly]/output/rf_rk/`) | [stage_b/kriging.py](stage_b/kriging.py) (via `target_kind='aod_minus_rf'`) | Stage A obs where present, (RF drift + kriged residual) elsewhere |
| `RF_PRED_DIR` (`Stage_B[_hourly]/output/rf_pred/`) | [stage_b/rf_gapfill.py](stage_b/rf_gapfill.py) `write_pred_slot` | Pure RF drift field (no pass-through, no mask) — intermediate for B3 |

> **Pass-through rule.** Every Stage B product keeps the Stage A observation at observed cells (`final = where(is_observed, obs, filled)`). Stage B *never* overwrites a cell Stage A saw. The pure RF drift grid in `rf_pred/` is the only exception — see §4.

---

## 1. Stage A product — `merged_YYYYMMDD_HHMM.nc`

Written by [stage_a/run_stage_a.py:117-187](stage_a/run_stage_a.py#L117-L187). "No data" convention: float vars store `-9999.0` as `_FillValue`; integer provenance vars use their own sentinels.

### 1.1 Field variables

| Variable             | dtype  | Fill    | Meaning                                                                 |
|----------------------|--------|---------|-------------------------------------------------------------------------|
| `AOD_merged`         | f4     | -9999.0 | TC-weighted fused AOD at 550 nm (MERRA-2 soft-calibrated inputs)        |
| `AOD_std`            | f4     | -9999.0 | Cross-sensor weighted std-dev — only finite when `n_sensors > 1`        |
| `weight_sum`         | f4     | -9999.0 | Σ(1/σ²_TC) across contributing sensors. **This is the number Stage B carries forward as `stage_a_weight_sum`.** |
| `AOD_phys_corrected` | f4     | -9999.0 | Physics-normalised AOD: `AOD_merged × (1 − RH/100)^0.6 / PBLH`. Absent if run with `--no-physics` |
| `ERA5_RH`, `ERA5_PBLH` | f4   | -9999.0 | Bilinearly interpolated ERA5 fields (also absent with `--no-physics`)   |
| `AOD_<sensor>`       | f4     | -9999.0 | Per-sensor **soft-calibrated** grid, one variable per contributing sensor (`AOD_himawari_l2`, `AOD_himawari_l3`, `AOD_viirs_snpp`, `AOD_viirs_noaa20`, `AOD_modis_maiac`). Lets you reconstruct which sensors contributed to a cell without decoding `confidence_flag`. |

### 1.2 Provenance flags

| Variable          | dtype | Fill (= "no data") | Codes                                                                 |
|-------------------|-------|--------------------|-----------------------------------------------------------------------|
| `n_sensors`       | i1    | **-1**             | Number of sensors with a valid TC weight at this cell (0, 1, 2, …, 5) |
| `dominant_sensor` | i1    | **0** (= no data)  | Sensor with the **highest TC weight** at this cell — see table below   |
| `confidence_flag` | i1    | **0** (= no data)  | Which *families* contributed — see table below                         |

**`dominant_sensor` codes** (from `config.SENSOR_CODES`, [config.py:275-281](stage_a/config.py#L275-L281)):

| Code | Sensor                            |
|------|-----------------------------------|
| 0    | no data                           |
| 1    | Himawari L2 (10-min AHI, cloud-prone, fine-mode-fraction gated) |
| 2    | Himawari L3 (hourly AHI composite) |
| 3    | MODIS MAIAC (MCD19A2, Terra + Aqua) |
| 4    | VIIRS SNPP (Deep Blue)             |
| 5    | VIIRS NOAA-20 (Deep Blue)          |

**`confidence_flag` codes** (from `config.CONFIDENCE_FLAG`, assigned in [stage_a/fusion.py:164-190](stage_a/fusion.py#L164-L190)):

| Code | Label                 | Rule                                                                   |
|------|-----------------------|------------------------------------------------------------------------|
| 0    | `no_data`             | No sensor valid at this cell                                           |
| 1    | `himawari_only`       | At least one Himawari channel (L2 or L3), no LEO sensor                |
| 2    | `leo_only`            | At least one LEO sensor (MAIAC / SNPP / N20), no Himawari              |
| 3    | `himawari_plus_leo`   | Himawari + exactly **one** LEO sensor                                  |
| 4    | `multi_leo_himawari`  | Himawari + **two or more** LEO sensors (highest-confidence pixels)     |

**How to read the flags together**

- `n_sensors = -1` means "nothing contributed here" and should always coincide with `AOD_merged = NaN`, `dominant_sensor = 0`, `confidence_flag = 0`.
- `n_sensors = 1` plus `confidence_flag = 1` → Himawari-only pixel; `dominant_sensor ∈ {1, 2}` tells you which cadence.
- `confidence_flag = 4` is the "trust-this-pixel" level: three or more independent sensors agreed within the TC weighting.
- `dominant_sensor` is **weight-based**, not count-based. A cell with Himawari L3 + VIIRS SNPP + MAIAC can still have `dominant_sensor = 3` (MAIAC) if MAIAC's σ²_TC in that stratum is the smallest.

### 1.3 Global attributes

- `title` — product label
- `institution` — "Hanoi University of Science and Technology"
- `slot_utc` — ISO-8601 UTC timestamp of the slot centre
- `Conventions = "CF-1.8"`

No `method` / `method_version` on Stage A files (only Stage B carries those).

---

## 2. Stage B — ST-kriging product (`output/st_kriging/aod_YYYYMMDD_HHMM.nc`)

Written by [stage_b/kriging.py:621-661](stage_b/kriging.py#L621-L661). Yang & Hu (2018) ST kriging on `aod_minus_cams` by default — CAMS is added back after kriging.

| Variable             | dtype  | Fill    | Meaning                                                                 |
|----------------------|--------|---------|-------------------------------------------------------------------------|
| `aod_550nm`          | f4     | -9999.0 | **Filled** AOD (observed Stage A value, or ST-kriged estimate). Clipped to [0, 5]. NaN where kriging solve failed, trend (CAMS) was missing, or the ±W_TIME_H pool had < `B1_MIN_NEIGHBOURS` points. |
| `is_observed`        | u1     | **0**   | **1** = this cell is a Stage A pass-through (`AOD_merged` was finite). **0** = cell was filled by kriging (or still NaN). |
| `uncertainty`        | f4     | -9999.0 | Kriging variance (AOD²) at filled cells; **NaN on observed cells**     |
| `stage_a_weight_sum` | f4     | -9999.0 | Stage A ICW weight sum at observed cells; **NaN off-observed**         |

### 2.1 Reading the flags together

A single cell falls into exactly one of three states — decode from `is_observed`, `aod_550nm`, `uncertainty`:

| State      | `is_observed` | `aod_550nm` | `uncertainty`  | Meaning                                               |
|------------|---------------|-------------|----------------|-------------------------------------------------------|
| Observed   | 1             | finite      | NaN            | Stage A saw this cell; value and provenance live in the Stage A file at the same slot |
| Filled     | 0             | finite      | finite (AOD²)  | ST-kriged from the ±100 km × ±12 h neighbourhood      |
| Unfilled   | 0             | NaN         | NaN            | Kriging gave up (empty pool, missing CAMS trend, or solver fail) |

To get back to the Stage A provenance of an observed cell, open `MERGED_DIR/.../merged_<same slot>.nc` and index with the same (lat, lon) — `is_observed` is set exactly where `AOD_merged` was finite.

### 2.2 Global attributes

| Attribute          | Value                                                        |
|--------------------|--------------------------------------------------------------|
| `method`           | `METHOD_TAG` (= `"st_kriging"`)                              |
| `method_version`   | module `METHOD_VERSION` constant                             |
| `slot_utc`         | ISO-8601 UTC                                                 |
| `slot_index`       | 0 … `SLOTS_PER_DAY-1`                                        |
| `training_window`  | `"YYYY-MM-DD to YYYY-MM-DD"` — the variogram fit window      |
| `uncertainty_units`| `"AOD^2 (kriging variance)"`                                 |

---

## 3. Stage B — RF gap-fill product (`output/rf/aod_YYYYMMDD_HHMM.nc`)

Written by [stage_b/rf_gapfill.py:677-730](stage_b/rf_gapfill.py#L677-L730). Youn 2024 style, trained on `aod_minus_cams` residual by default (`RF_TARGET_KIND`).

| Variable             | dtype  | Fill    | Meaning                                                                 |
|----------------------|--------|---------|-------------------------------------------------------------------------|
| `aod_550nm`          | f4     | -9999.0 | **Filled** AOD (observed Stage A, or RF prediction). Clipped to [0, 5]. NaN where the RF couldn't predict (e.g. CAMS missing at that slot). |
| `is_observed`        | u1     | **0**   | Same semantic as kriging — **1** = Stage A pass-through, **0** = RF filled |
| `uncertainty`        | f4     | -9999.0 | **Per-tree standard deviation** across the RF ensemble (not variance). NaN on observed cells. |
| `stage_a_weight_sum` | f4     | -9999.0 | Stage A ICW weight sum at observed cells; NaN off-observed              |

### 3.1 Three states — same shape as kriging, different `uncertainty` units

| State      | `is_observed` | `aod_550nm` | `uncertainty`               |
|------------|---------------|-------------|-----------------------------|
| Observed   | 1             | finite      | NaN                         |
| Filled     | 0             | finite      | finite (per-tree **SD**, AOD units) |
| Unfilled   | 0             | NaN         | NaN                         |

**Watch out:** `uncertainty` in the RF file is **SD in AOD units**; in the kriging file it's **variance in AOD² units**. Square or square-root as needed when cross-plotting the two products.

### 3.2 Global attributes

| Attribute          | Value                                                        |
|--------------------|--------------------------------------------------------------|
| `method`           | `METHOD_TAG` from rf_gapfill (= `"rf"`)                      |
| `method_version`   | module `METHOD_VERSION`                                      |
| `slot_utc`         | ISO-8601                                                     |
| `slot_index`       | 0 … `SLOTS_PER_DAY-1`                                        |
| `training_window`  | RF bundle's training span                                    |
| `uncertainty_units`| `"AOD (per-tree SD across the RF ensemble)"`                 |

---

## 4. Stage B — RF prediction tree (`output/rf_pred/pred_YYYYMMDD_HHMM.nc`)

Intermediate for B3 regression kriging only. Written by [stage_b/rf_gapfill.py:770-797](stage_b/rf_gapfill.py#L770-L797).

| Variable  | dtype | Fill    | Meaning                                                        |
|-----------|-------|---------|----------------------------------------------------------------|
| `rf_pred` | f4    | -9999.0 | Pure RF prediction ŷ_rf at **every** cell — observed or not    |

Deliberately has **no `is_observed`** and **no pass-through**: it's the drift field RK needs at observed cells (to form the residual to krige) as well as at gap cells (as the trend to add back). Not meant for end-user consumption.

---

## 5. Stage B — regression-kriging product (`output/rf_rk/aod_YYYYMMDD_HHMM.nc`)

Same writer as the ST kriging product ([stage_b/kriging.py](stage_b/kriging.py)), run with `target_kind='aod_minus_rf'`. Schema is identical to §2:

| Variable  | `is_observed = 1`                    | `is_observed = 0`                                                      |
|-----------|--------------------------------------|-------------------------------------------------------------------------|
| `aod_550nm` | Stage A pass-through                | RF drift (ŷ_rf) + kriged RF residual. If kriging fails but ŷ_rf is finite, cell falls back to ŷ_rf alone. |
| `uncertainty` | NaN                               | Kriging variance of the residual (AOD²)                                 |

Everything else — `is_observed` semantics, `stage_a_weight_sum`, `method_version`, three-state table — matches §2. The only interpretation change is that the "filled" value is a sum of two models rather than one.

---

## 6. Cross-product provenance recipe

To fully trace a single cell at slot `T`:

1. **Was it observed in Stage A?** Open `merged_T.nc`. If `n_sensors ≥ 1` → yes.
2. **Which sensors?** Read `confidence_flag` (family) and `dominant_sensor` (which one carried the most TC weight). For sensor-level detail, inspect the `AOD_<sensor>` variables — any finite value means that sensor contributed.
3. **How strongly?** `weight_sum` in Stage A = Σ(1/σ²_TC). High `weight_sum` → tightly constrained merge.
4. **In the Stage B product, is it the Stage A value or a model fill?** Read `is_observed` in whichever Stage B file you opened. `1` → identical to Stage A. `0` → open the Stage A file at the same (lat, lon, T) and you'll see `n_sensors = -1`.
5. **How uncertain is a filled cell?** Read `uncertainty` — **AOD² for the two kriging products, AOD (SD) for the RF product**. Mind the units.

---

## 7. Sentinel-value cheat sheet

Written on disk; `xarray` + `mask_and_scale=True` (default) converts these to `NaN` for float vars and keeps the integer sentinels as-is.

| Variable                       | On-disk fill | Means             |
|--------------------------------|--------------|-------------------|
| All `f4` AOD / uncertainty / weight fields | **-9999.0** | no data   |
| Stage A `n_sensors`            | **-1**       | no data           |
| Stage A `dominant_sensor`      | **0**        | no data           |
| Stage A `confidence_flag`      | **0**        | no data           |
| Stage B `is_observed`          | **0**        | not an observation (either filled by model, or still a NaN gap) |
