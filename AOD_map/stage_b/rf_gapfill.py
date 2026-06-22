"""Stage B Step B2 — per-slot Random Forest gap-fill (stage_b_fixes §7.8).

Per-slot RF mirrors Youn 2024 / Chen 2023's SIM at 30-min cadence.  Per
fix doc §0.5:
- `oob_score = False` always — OOB-R² is optimistic under temporal
  autocorrelation; we use 5-fold contiguous temporal CV instead.
- Tuning criterion is **mean CV-R²** across the 5 folds (not OOB-R²).
- Fold assignment by UTC date so all slots of a day stay in the same fold.

Outputs — per-slot NC files in parallel product tree
`RF_OUTPUT_DIR / YYYY / MM / DD / aod_YYYYMMDD_HHMM.nc`:

    aod_550nm         (lat, lon)  filled AOD
    is_observed       (lat, lon)  True on Stage A pass-through cells
    uncertainty       (lat, lon)  per-tree SD across the RF ensemble
                                  (NOT OOB SE) — NaN on observed cells
    stage_a_weight_sum (lat, lon) Stage A ICW weight sum on observed cells
"""

from __future__ import annotations

import gc
import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd
import netCDF4 as nc

from sklearn.ensemble import RandomForestRegressor

try:
    import joblib
except ImportError as exc:
    raise ImportError('joblib is required for Stage B RF persistence') from exc

try:
    import psutil
    _PROC = psutil.Process(os.getpid())
    def _rss_gb() -> float:
        return _PROC.memory_info().rss / (1024 ** 3)
except ImportError:
    def _rss_gb() -> float:
        return float('nan')

from config import (
    LATS, LONS, NLAT, NLON,
    RF_OUTPUT_DIR, MODELS_DIR,
    RF_FEATURES,
    RF_N_ESTIMATORS, RF_MAX_DEPTH, RF_MIN_SAMPLES_LEAF,
    RF_N_JOBS, RF_RANDOM_STATE, RF_GRID,
    RF_INTERNAL_TEST_FRACTION, RF_CV_FOLDS,
    RF_TRAIN_TARGET_ROWS,
    RF_OOB_SCORE,
    RMSE_CONSISTENCY_TOLERANCE,
    TRAIN_START, TRAIN_END,
)
from features import build_feature_grid, stack_features, build_training_table
from slots import (
    load_slot, iter_local_days, iter_window_slots, slot_index,
)


METHOD_TAG = 'rf'
METHOD_VERSION = 'v1.0'


# ── Model bundle ──────────────────────────────────────────────────────────────

@dataclass
class RFBundle:
    model: RandomForestRegressor
    feature_columns: list[str]
    median_impute: np.ndarray
    training_window: tuple[str, str]
    hyperparams: dict
    metrics: dict = field(default_factory=dict)
    cv_fold_metrics: list[dict] = field(default_factory=list)

    def predict(self, X: np.ndarray) -> np.ndarray:
        X = _impute(X, self.median_impute)
        return self.model.predict(X)

    def predict_with_tree_sd(self, X: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Mean and per-tree SD across the forest's individual tree predictions.

        Per fix doc §7.8.3: uncertainty = SD across trees (NOT OOB SE).
        """
        X = _impute(X, self.median_impute)
        # (n_trees, n_samples)
        per_tree = np.stack([t.predict(X) for t in self.model.estimators_], axis=0)
        return per_tree.mean(axis=0), per_tree.std(axis=0)


def _bundle_path(name: str = 'rf_primary') -> Path:
    return MODELS_DIR / f'{name}.joblib'


def save_bundle(bundle: RFBundle, name: str = 'rf_primary') -> Path:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    p = _bundle_path(name)
    joblib.dump(bundle, p, compress=3)
    (MODELS_DIR / f'{name}.meta.json').write_text(json.dumps({
        'feature_columns':  bundle.feature_columns,
        'training_window':  list(bundle.training_window),
        'hyperparams':      bundle.hyperparams,
        'metrics':          bundle.metrics,
        'cv_fold_metrics':  bundle.cv_fold_metrics,
    }, default=str, indent=2))
    return p


def load_bundle(name: str = 'rf_primary') -> RFBundle:
    return joblib.load(_bundle_path(name))


# ── Helpers ──────────────────────────────────────────────────────────────────

def _impute(X: np.ndarray, medians: np.ndarray) -> np.ndarray:
    if not np.isnan(X).any():
        return X.astype(np.float32, copy=False)
    X = X.astype(np.float32, copy=True)
    idx = np.where(np.isnan(X))
    X[idx] = medians[idx[1]]
    X = np.where(np.isnan(X), 0.0, X)
    return X


def _rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def _r2(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    ss_res = float(np.sum((y_true - y_pred) ** 2))
    ss_tot = float(np.sum((y_true - y_true.mean()) ** 2))
    return 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan


def _fit_model(X: np.ndarray, y: np.ndarray, hp: dict) -> RandomForestRegressor:
    rf = RandomForestRegressor(
        n_estimators     = hp['n_estimators'],
        max_depth        = hp['max_depth'],
        min_samples_leaf = hp['min_samples_leaf'],
        max_features     = hp.get('max_features', 1.0),
        n_jobs           = RF_N_JOBS,
        random_state     = RF_RANDOM_STATE,
        oob_score        = RF_OOB_SCORE,   # always False per §0.5
    )
    rf.fit(X, y)
    return rf


# ── Contiguous temporal-block CV (fold by UTC date) ─────────────────────────

def temporal_block_folds(meta_dates: pd.Series, k: int) -> list[np.ndarray]:
    """`k` boolean masks identifying contiguous calendar-time blocks.

    All slots of a given UTC date land in the same fold (prevents within-day
    leakage between training and validation rows).
    """
    days = np.array(sorted(pd.unique(meta_dates)))
    if len(days) == 0:
        return []
    edges = np.linspace(0, len(days), k + 1, dtype=int)
    folds = []
    date_to_val = pd.Series(meta_dates).values
    for i in range(k):
        block = set(days[edges[i]:edges[i + 1]].tolist())
        folds.append(np.isin(date_to_val, list(block)))
    return folds


# ── Training ─────────────────────────────────────────────────────────────────

def train_rf(
    start: date = TRAIN_START,
    end:   date = TRAIN_END,
    name:  str  = 'rf_primary',
    hp:    Optional[dict] = None,
    target_rows: int = RF_TRAIN_TARGET_ROWS,
    progress: Optional[Callable] = None,
    return_metrics: bool = False,
):
    """Fit the production per-slot RF.

    Temporal 80/20 split → internal-test held out from CV; 5-fold contiguous
    block CV inside the 80 %; per-fold RMSE & R² recorded.
    """
    X_df, y_ser, meta = build_training_table(
        start, end, target_rows=target_rows, progress=progress,
    )
    if X_df.empty:
        raise RuntimeError(f'No training rows in [{start}, {end}].')

    X = X_df.to_numpy(dtype=np.float32)
    y = y_ser.to_numpy(dtype=np.float32)
    feature_columns = list(X_df.columns)
    dates = meta['date'].to_numpy()
    del X_df, y_ser, meta
    gc.collect()

    days_sorted = np.array(sorted(pd.unique(dates)))
    cut         = int(len(days_sorted) * (1 - RF_INTERNAL_TEST_FRACTION))
    test_days   = set(days_sorted[cut:].tolist())
    is_test     = np.isin(dates, list(test_days))
    train_idx   = np.where(~is_test)[0]
    test_idx    = np.where( is_test)[0]

    medians = np.nanmedian(X[train_idx], axis=0).astype(np.float32)
    medians = np.where(np.isfinite(medians), medians, 0.0).astype(np.float32)
    X_tr_imp = _impute(X[train_idx], medians)
    X_te_imp = _impute(X[test_idx],  medians)
    n_features = int(X.shape[1])
    train_dates = dates[train_idx]
    del X, dates
    gc.collect()

    hp = hp or {
        'n_estimators':     RF_N_ESTIMATORS,
        'max_depth':        RF_MAX_DEPTH,
        'min_samples_leaf': RF_MIN_SAMPLES_LEAF,
        'max_features':     1.0,
    }

    cv_masks = temporal_block_folds(pd.Series(train_dates), RF_CV_FOLDS)
    del train_dates
    cv_iter  = progress(cv_masks, total=len(cv_masks), desc='CV folds') \
               if progress is not None else cv_masks
    y_train = y[train_idx]
    cv_fold_metrics: list[dict] = []
    print(f'  train_rf: starting CV — RSS={_rss_gb():.2f} GB', flush=True)
    for val_mask in cv_iter:
        fold_train = ~val_mask
        rf_fold = _fit_model(X_tr_imp[fold_train], y_train[fold_train], hp)
        pred    = rf_fold.predict(X_tr_imp[val_mask])
        cv_fold_metrics.append({
            'rmse': _rmse(y_train[val_mask], pred),
            'r2':   _r2  (y_train[val_mask], pred),
            'n':    int(val_mask.sum()),
        })
        del rf_fold, pred
        gc.collect()
        print(f'    fold done — RSS={_rss_gb():.2f} GB', flush=True)

    print(f'  train_rf: final fit — RSS={_rss_gb():.2f} GB', flush=True)
    rf_final = _fit_model(X_tr_imp, y_train, hp)
    train_pred = rf_final.predict(X_tr_imp)
    test_pred  = rf_final.predict(X_te_imp)

    metrics = {
        'n_train':            int(len(train_idx)),
        'n_test':             int(len(test_idx)),
        'n_features':         n_features,
        'rmse_train':         _rmse(y_train, train_pred),
        'rmse_cv_mean':       float(np.mean([f['rmse'] for f in cv_fold_metrics])),
        'rmse_cv_std':        float(np.std ([f['rmse'] for f in cv_fold_metrics])),
        'r2_cv_mean':         float(np.mean([f['r2']   for f in cv_fold_metrics])),
        'r2_cv_std':          float(np.std ([f['r2']   for f in cv_fold_metrics])),
        'rmse_internal_test': _rmse(y[test_idx], test_pred),
        'r2_internal_test':   _r2  (y[test_idx], test_pred),
    }
    del X_tr_imp, X_te_imp, train_pred, test_pred, y_train
    gc.collect()

    bundle = RFBundle(
        model           = rf_final,
        feature_columns = feature_columns,
        median_impute   = medians,
        training_window = (start.isoformat(), end.isoformat()),
        hyperparams     = hp,
        metrics         = metrics,
        cv_fold_metrics = cv_fold_metrics,
    )
    save_bundle(bundle, name)
    if return_metrics:
        return bundle, metrics
    return bundle


def tune_rf(start: date = TRAIN_START, end: date = TRAIN_END,
            name: str = 'rf_tuned',
            target_rows: int = RF_TRAIN_TARGET_ROWS,
            progress: Optional[Callable] = None,
            ) -> tuple[dict, list[dict]]:
    """Grid search over RF_GRID, selection = **mean CV-R²** (NOT OOB).

    Per fix doc §0.5 + §7.8.1: OOB-R² is optimistic under temporal
    autocorrelation.  5-fold temporal-block CV is the honest signal.
    """
    X_df, y_ser, meta = build_training_table(
        start, end, target_rows=target_rows, progress=progress,
    )
    if X_df.empty:
        raise RuntimeError(f'No training rows in [{start}, {end}].')
    X = X_df.to_numpy(dtype=np.float32); y = y_ser.to_numpy(dtype=np.float32)
    dates = meta['date'].to_numpy()
    del X_df, y_ser, meta
    gc.collect()

    medians = np.nanmedian(X, axis=0).astype(np.float32)
    medians = np.where(np.isfinite(medians), medians, 0.0)
    X_imp = _impute(X, medians)
    del X
    gc.collect()

    cv_masks = temporal_block_folds(pd.Series(dates), RF_CV_FOLDS)
    del dates

    combos = [(n, d, l, mf)
              for n in RF_GRID['n_estimators']
              for d in RF_GRID['max_depth']
              for l in RF_GRID['min_samples_leaf']
              for mf in RF_GRID['max_features']]
    iterator = progress(combos, desc='hp grid') if progress is not None else combos

    print(f'  tune_rf: starting grid sweep with {len(combos)} combos × '
          f'{len(cv_masks)} folds — RSS={_rss_gb():.2f} GB', flush=True)

    results = []
    for n_est, depth, leaf, mfeat in iterator:
        hp = {'n_estimators': n_est, 'max_depth': depth,
              'min_samples_leaf': leaf, 'max_features': mfeat}
        fold_r2s = []
        for val_mask in cv_masks:
            fold_train = ~val_mask
            rf = _fit_model(X_imp[fold_train], y[fold_train], hp)
            pred = rf.predict(X_imp[val_mask])
            fold_r2s.append(_r2(y[val_mask], pred))
            del rf, pred
            gc.collect()
        results.append({**hp,
                         'cv_r2_mean': float(np.mean(fold_r2s)),
                         'cv_r2_std':  float(np.std (fold_r2s))})
        print(f'    hp={hp}  cv_r2_mean={results[-1]["cv_r2_mean"]:.3f}  '
              f'RSS={_rss_gb():.2f} GB', flush=True)

    results.sort(key=lambda r: r['cv_r2_mean'], reverse=True)
    best_hp = {k: results[0][k]
               for k in ('n_estimators', 'max_depth', 'min_samples_leaf', 'max_features')}
    train_rf(start=start, end=end, name=name, hp=best_hp,
             target_rows=target_rows, progress=progress)
    return best_hp, results


def consistency_check(metrics: dict, tol: float = RMSE_CONSISTENCY_TOLERANCE) -> bool:
    cv = metrics['rmse_cv_mean']
    lo, hi = cv * (1 - tol), cv * (1 + tol)
    return all(lo <= metrics[k] <= hi for k in ('rmse_train', 'rmse_internal_test'))


# ── Inference ────────────────────────────────────────────────────────────────

def gapfilled_path(slot_utc: datetime) -> Path:
    return (RF_OUTPUT_DIR
            / f'{slot_utc.year:04d}'
            / f'{slot_utc.month:02d}'
            / f'{slot_utc.day:02d}'
            / f'aod_{slot_utc:%Y%m%d_%H%M}.nc')


def predict_slot_grid(slot_utc: datetime, bundle: RFBundle,
                       with_uncertainty: bool = True
                       ) -> tuple[np.ndarray, Optional[np.ndarray]]:
    """RF prediction grid + per-tree-SD uncertainty (or None to skip uncertainty)."""
    grids = build_feature_grid(slot_utc)
    X     = stack_features(grids)
    if with_uncertainty:
        mean, sd = bundle.predict_with_tree_sd(X)
        y_grid = np.clip(mean.reshape(NLAT, NLON), 0.0, 5.0).astype(np.float32)
        u_grid = sd.reshape(NLAT, NLON).astype(np.float32)
        return y_grid, u_grid
    y_grid = np.clip(bundle.predict(X).reshape(NLAT, NLON), 0.0, 5.0).astype(np.float32)
    return y_grid, None


def fill_slot(slot_utc: datetime, bundle: RFBundle,
              prefer_observed: bool = True,
              training_window: str = '') -> Path:
    """Run the RF on one slot and write the NC file."""
    payload = load_slot(slot_utc)
    if payload is not None:
        obs = payload['aod'].astype(np.float32)
        is_observed = np.isfinite(obs) & (obs >= 0)
        weight_sum  = np.where(is_observed, payload['weight_sum'], np.nan).astype(np.float32)
    else:
        obs = np.full((NLAT, NLON), np.nan, dtype=np.float32)
        is_observed = np.zeros((NLAT, NLON), dtype=bool)
        weight_sum  = np.full((NLAT, NLON), np.nan, dtype=np.float32)

    pred, tree_sd = predict_slot_grid(slot_utc, bundle, with_uncertainty=True)

    final = np.where(is_observed & prefer_observed, obs, pred).astype(np.float32)
    uncertainty = np.where(is_observed & prefer_observed, np.nan, tree_sd).astype(np.float32)

    out = gapfilled_path(slot_utc)
    out.parent.mkdir(parents=True, exist_ok=True)
    s_idx = slot_index(slot_utc)
    with nc.Dataset(out, 'w', format='NETCDF4') as ds:
        ds.title              = 'Vietnam Stage B 30-min AOD — RF product (Youn 2024 style)'
        ds.method             = METHOD_TAG
        ds.method_version     = METHOD_VERSION
        ds.slot_utc           = slot_utc.replace(tzinfo=timezone.utc).isoformat()
        ds.slot_index         = s_idx
        ds.training_window    = training_window or ' to '.join(bundle.training_window)
        ds.created_at         = datetime.now(timezone.utc).isoformat()
        ds.uncertainty_units  = 'AOD (per-tree SD across the RF ensemble)'
        ds.Conventions        = 'CF-1.8'
        ds.createDimension('lat', NLAT)
        ds.createDimension('lon', NLON)
        ds.createVariable('lat', 'f4', ('lat',))[:] = LATS.astype(np.float32)
        ds.createVariable('lon', 'f4', ('lon',))[:] = LONS.astype(np.float32)

        def _add(name, arr, long_name, dtype='f4', fill=-9999.0, units='1'):
            v = ds.createVariable(name, dtype, ('lat', 'lon'),
                                  fill_value=fill, zlib=True, complevel=4)
            v.long_name = long_name; v.units = units
            data = arr if dtype != 'f4' else np.where(np.isfinite(arr), arr, fill)
            v[:] = data

        _add('aod_550nm',          final,
             'Filled AOD at 550 nm (observed or RF gap-fill)')
        _add('is_observed',        is_observed.astype(np.uint8),
             'True where this cell was a Stage A observation pass-through',
             dtype='u1', fill=0)
        _add('uncertainty',        uncertainty,
             'Per-tree standard deviation across the RF ensemble (NaN on observed)')
        _add('stage_a_weight_sum', weight_sum,
             'Stage A ICW weight sum at observed cells; NaN off-observed', units='count')
    return out


def fill_range(start: date, end: date, bundle: Optional[RFBundle] = None,
               overwrite: bool = False,
               progress: Optional[Callable] = None) -> int:
    bundle = bundle or load_bundle()
    days = list(iter_local_days(start, end))
    iterator = progress(days, desc='RF fill') if progress is not None else days
    n = 0
    for d in iterator:
        for slot_utc in iter_window_slots(d):
            out = gapfilled_path(slot_utc)
            if out.exists() and not overwrite:
                continue
            try:
                fill_slot(slot_utc, bundle=bundle)
                n += 1
            except FileNotFoundError:
                pass
    return n
