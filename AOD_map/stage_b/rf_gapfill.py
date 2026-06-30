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
    RF_TARGET_KIND,
    RF_RESIDUAL_BUNDLE_NAME,
    RF_TUNE_REPORT_TRAIN_RMSE,
    RF_USE_DECILE_REBALANCE_WEIGHTS,
    RF_AOD_DECILES,
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
    # §7.8.1: residual-target bundles store target_kind so the inference path
    # knows whether to add cams_aod back.  Default 'aod' preserves the
    # pre-§7.8.1 contract for already-saved bundles (older joblibs loaded
    # before this field existed will get the dataclass default).
    target_kind: str = 'aod'

    def _cams_column_index(self) -> Optional[int]:
        try:
            return self.feature_columns.index('cams_aod')
        except ValueError:
            return None

    def predict(self, X: np.ndarray) -> np.ndarray:
        X_imp = _impute(X, self.median_impute)
        pred  = self.model.predict(X_imp)
        if self.target_kind == 'aod_minus_cams':
            idx = self._cams_column_index()
            if idx is None:
                raise RuntimeError(
                    'Residual-target bundle declares target_kind=aod_minus_cams '
                    "but feature_columns has no 'cams_aod' entry."
                )
            pred = pred + X_imp[:, idx]
        return pred

    def predict_with_tree_sd(self, X: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Mean and per-tree SD across the forest's individual tree predictions.

        Per fix doc §7.8.3: uncertainty = SD across trees (NOT OOB SE).
        For residual-target bundles, mean is shifted back to AOD space by
        adding cams_aod; SD is invariant under that shift.
        """
        X_imp = _impute(X, self.median_impute)
        # (n_trees, n_samples)
        per_tree = np.stack([t.predict(X_imp) for t in self.model.estimators_], axis=0)
        mean = per_tree.mean(axis=0)
        sd   = per_tree.std(axis=0)
        if self.target_kind == 'aod_minus_cams':
            idx = self._cams_column_index()
            if idx is None:
                raise RuntimeError(
                    'Residual-target bundle declares target_kind=aod_minus_cams '
                    "but feature_columns has no 'cams_aod' entry."
                )
            mean = mean + X_imp[:, idx]
        return mean, sd


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
        'target_kind':      bundle.target_kind,
    }, default=str, indent=2))
    return p


def load_bundle(name: str = 'rf_primary') -> RFBundle:
    bundle = joblib.load(_bundle_path(name))
    # Older joblibs from before the target_kind field was added unpickle
    # without it — the dataclass default fills in 'aod'.
    if not hasattr(bundle, 'target_kind'):
        bundle.target_kind = 'aod'
    return bundle


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


def _fit_model(X: np.ndarray, y: np.ndarray, hp: dict,
               sample_weight: Optional[np.ndarray] = None,
               ) -> RandomForestRegressor:
    rf = RandomForestRegressor(
        n_estimators     = hp['n_estimators'],
        max_depth        = hp['max_depth'],
        min_samples_leaf = hp['min_samples_leaf'],
        max_features     = hp.get('max_features', 1.0),
        n_jobs           = RF_N_JOBS,
        random_state     = RF_RANDOM_STATE,
        oob_score        = RF_OOB_SCORE,   # always False per §0.5
    )
    rf.fit(X, y, sample_weight=sample_weight)
    return rf


# ── §8.2.4c follow-up: decile-rebalance sample weights ──────────────────────

def _decile_rebalance_weights(decile_ids: np.ndarray,
                              n_deciles: int = RF_AOD_DECILES,
                              ) -> np.ndarray:
    """Per-row sample_weight that restores natural decile prevalence.

    Equal-per-stratum sampling in `build_training_table` over-represents
    deciles whose strata exist in more (month, slot_idx) bins.  Because AOD
    decile edges are defined as equal-population by construction, the
    *natural* prevalence per decile is exactly 1/n_deciles.

    This function turns each row's decile label into a weight such that:
        sum(weight[decile==d]) / sum(weight) ≈ 1/n_deciles  for all d

    so the RF's effective training distribution matches natural prevalence
    while every row still gets a non-zero gradient signal.

    Args
    ----
    decile_ids : (N,) int array of decile labels in [0, n_deciles-1]
                 (drawn from meta['aod_decile'], post-target-kind drop).
    n_deciles  : number of decile bins (10 by default).

    Returns
    -------
    w : (N,) float32 array of per-row weights.  Should be normalised so
        sum(w) == N (preserves loss scale, makes the RMSE numbers comparable
        across rebalanced and non-rebalanced runs).

    Empty deciles (no rows in that bin) get weight 0 so we don't divide by
    zero; their absence contributes nothing to the loss, which is correct
    behaviour (we can't reweight what isn't there).
    """
    n = int(len(decile_ids))
    if n == 0:
        return np.zeros(0, dtype=np.float32)
    counts = np.bincount(decile_ids, minlength=n_deciles).astype(np.float64)
    sample_share = counts / n
    natural_share = 1.0 / n_deciles
    per_decile_w = np.where(sample_share > 0,
                            natural_share / np.where(sample_share > 0, sample_share, 1.0),
                            0.0)
    w = per_decile_w[decile_ids]
    total = float(w.sum())
    if total > 0:
        w = w * (n / total)   # normalise so Σw == n (preserves RMSE scale)
    return w.astype(np.float32)


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

def _apply_target_kind(
    X_df: pd.DataFrame, y_ser: pd.Series, meta: pd.DataFrame,
    target_kind: str,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    """Convert the (X, y, meta) training table to the requested target.

    For 'aod_minus_cams', y becomes (aod - cams_aod) and rows where cams_aod
    is NaN are dropped — there's no way to define a residual against a
    missing prior.  X[cams_aod] stays in the feature matrix so the RF can
    still condition on the CAMS magnitude when learning the residual shape
    (and so the inference path can add cams back).
    """
    if target_kind == 'aod':
        return X_df, y_ser, meta
    if target_kind != 'aod_minus_cams':
        raise ValueError(f'Unknown RF_TARGET_KIND={target_kind!r}')
    if 'cams_aod' not in X_df.columns:
        raise RuntimeError(
            "target_kind=aod_minus_cams requires 'cams_aod' in feature_columns"
        )
    cams = X_df['cams_aod'].to_numpy(dtype=np.float32)
    keep = np.isfinite(cams)
    n_drop = int((~keep).sum())
    if n_drop:
        print(f'  _apply_target_kind: dropping {n_drop} rows with NaN cams_aod', flush=True)
    X_df = X_df.loc[keep].reset_index(drop=True)
    y_ser = pd.Series(y_ser.to_numpy(dtype=np.float32)[keep] - cams[keep],
                       name='aod_minus_cams')
    meta  = meta.loc[keep].reset_index(drop=True)
    return X_df, y_ser, meta


def train_rf(
    start: date = TRAIN_START,
    end:   date = TRAIN_END,
    name:  Optional[str] = None,
    hp:    Optional[dict] = None,
    target_rows: int = RF_TRAIN_TARGET_ROWS,
    progress: Optional[Callable] = None,
    return_metrics: bool = False,
    target_kind: str = RF_TARGET_KIND,
):
    """Fit the production per-slot RF.

    Temporal 80/20 split → internal-test held out from CV; 5-fold contiguous
    block CV inside the 80 %; per-fold RMSE & R² recorded.

    When `target_kind == 'aod_minus_cams'` (default, post-§7.8.1 fix-doc),
    the RF learns Stage-A AOD − CAMS rather than AOD directly.  The bundle's
    `predict()` adds cams_aod back so callers see AOD as before.
    """
    if name is None:
        name = (RF_RESIDUAL_BUNDLE_NAME
                if target_kind == 'aod_minus_cams' else 'rf_primary')

    X_df, y_ser, meta = build_training_table(
        start, end, target_rows=target_rows, progress=progress,
    )
    if X_df.empty:
        raise RuntimeError(f'No training rows in [{start}, {end}].')

    X_df, y_ser, meta = _apply_target_kind(X_df, y_ser, meta, target_kind)

    X = X_df.to_numpy(dtype=np.float32)
    y = y_ser.to_numpy(dtype=np.float32)
    feature_columns = list(X_df.columns)
    dates = meta['date'].to_numpy()
    # Pull decile IDs *before* discarding `meta` — they're the input to the
    # rebalance-weight calc and must stay aligned with the X / y row order.
    deciles_all = meta['aod_decile'].to_numpy(dtype=np.int32) \
                   if 'aod_decile' in meta.columns else None
    # When target_kind=='aod_minus_cams', residual-space RMSE/R² are tiny-
    # variance metrics (SS_tot ≈ 0.05²) that disagree dramatically with AOD-
    # space metrics (SS_tot ≈ 0.4²).  Track the cams_aod column index so we
    # can recover y_aod = y_resid + cams and report both spaces — the AOD-
    # space numbers are what travel to the AERONET validation panel.
    cams_idx = (feature_columns.index('cams_aod')
                if target_kind == 'aod_minus_cams' and 'cams_aod' in feature_columns
                else None)
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

    # §8.2.4c follow-up: compute decile-rebalance weights once on the train
    # portion only.  Validation rows inside CV folds are scored unweighted
    # (so RMSE numbers stay comparable to the pre-rebalance baseline), but
    # the fit gradient sees the weights.
    if RF_USE_DECILE_REBALANCE_WEIGHTS and deciles_all is not None:
        deciles_train = deciles_all[train_idx]
        w_train = _decile_rebalance_weights(deciles_train).astype(np.float32)
        print(f'  train_rf: decile rebalance ON — '
              f'mean(w)={float(w_train.mean()):.3f}  '
              f'min(w)={float(w_train.min()):.3f}  '
              f'max(w)={float(w_train.max()):.3f}', flush=True)
        # §8.2.4c follow-up audit: show what the weights actually do to the
        # unconditional residual prior.  If unweighted ≈ weighted ≈ +0.05,
        # the prior is the natural Vietnam CAMS underestimation (physics,
        # not sampling).  If weighted drops to ~0, sampling was the issue
        # and the bundle should now generalise better against AERONET.
        #
        # NB: after _apply_target_kind with target_kind='aod_minus_cams',
        # y_train is ALREADY the residual (aod - cams).  The "residual the
        # RF will minimise toward zero" is just y_train itself — do not
        # subtract cams again, that double-counts the prior.
        if target_kind == 'aod_minus_cams':
            resid = y_train.astype(np.float64)
            label = 'mean(y_train) [= residual aod-cams]'
        else:
            cams_tr = X_tr_imp[:, cams_idx].astype(np.float64) \
                       if cams_idx is not None else None
            resid = (y_train.astype(np.float64) - cams_tr
                     if cams_tr is not None else y_train.astype(np.float64))
            label = 'mean(y_train - cams)'
        uw_mean = float(resid.mean())
        w_mean  = float(np.average(resid, weights=w_train.astype(np.float64)))
        print(f'    unweighted {label} = {uw_mean:+.5f}  '
              f'(natural marginal of Stage A − CAMS)',
              flush=True)
        print(f'    weighted   {label} = {w_mean:+.5f}  '
              f'(what the loss-optimum will anchor on)',
              flush=True)
        print(f'    prior shift due to reweighting  = '
              f'{w_mean - uw_mean:+.5f}  '
              f'(negative if weights pulled the prior toward 0)',
              flush=True)
    else:
        w_train = None
        if RF_USE_DECILE_REBALANCE_WEIGHTS:
            print('  train_rf: decile rebalance requested but meta has no '
                  "'aod_decile' column — running unweighted.", flush=True)

    cv_fold_metrics: list[dict] = []
    print(f'  train_rf: starting CV — RSS={_rss_gb():.2f} GB', flush=True)
    for val_mask in cv_iter:
        fold_train = ~val_mask
        fold_w = w_train[fold_train] if w_train is not None else None
        rf_fold = _fit_model(X_tr_imp[fold_train], y_train[fold_train], hp,
                             sample_weight=fold_w)
        pred    = rf_fold.predict(X_tr_imp[val_mask])
        fold_metrics = {
            'rmse': _rmse(y_train[val_mask], pred),
            'r2':   _r2  (y_train[val_mask], pred),
            'n':    int(val_mask.sum()),
        }
        if cams_idx is not None:
            cams_val = X_tr_imp[val_mask, cams_idx]
            y_aod   = y_train[val_mask] + cams_val
            p_aod   = pred + cams_val
            fold_metrics['rmse_aod'] = _rmse(y_aod, p_aod)
            fold_metrics['r2_aod']   = _r2  (y_aod, p_aod)
        cv_fold_metrics.append(fold_metrics)
        del rf_fold, pred
        gc.collect()
        aod_blurb = (f'  rmse_aod={fold_metrics["rmse_aod"]:.4f}  '
                     f'r2_aod={fold_metrics["r2_aod"]:.4f}'
                     if cams_idx is not None else '')
        print(f'    fold done — rmse_val={fold_metrics["rmse"]:.4f}  '
              f'r2_val={fold_metrics["r2"]:.4f}{aod_blurb}  '
              f'RSS={_rss_gb():.2f} GB', flush=True)

    print(f'  train_rf: final fit — RSS={_rss_gb():.2f} GB', flush=True)
    rf_final = _fit_model(X_tr_imp, y_train, hp, sample_weight=w_train)
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

    # AOD-space twins of the same metrics — these are what compare against
    # the §8.2.4 / §8.2.5 AERONET validation numbers.  Residual-space
    # metrics have small SS_tot (var(aod-cams)) and can rise while AOD-space
    # generalisation drops, which is exactly the trap the fire/region run
    # fell into.
    if cams_idx is not None:
        cams_tr   = X_tr_imp[:, cams_idx]
        cams_te   = X_te_imp[:, cams_idx]
        y_tr_aod  = y_train       + cams_tr
        p_tr_aod  = train_pred    + cams_tr
        y_te_aod  = y[test_idx]   + cams_te
        p_te_aod  = test_pred     + cams_te
        metrics.update({
            'rmse_train_aod':         _rmse(y_tr_aod, p_tr_aod),
            'rmse_cv_mean_aod':       float(np.mean([f['rmse_aod']
                                                     for f in cv_fold_metrics])),
            'rmse_cv_std_aod':        float(np.std ([f['rmse_aod']
                                                     for f in cv_fold_metrics])),
            'r2_cv_mean_aod':         float(np.mean([f['r2_aod']
                                                     for f in cv_fold_metrics])),
            'r2_cv_std_aod':          float(np.std ([f['r2_aod']
                                                     for f in cv_fold_metrics])),
            'rmse_internal_test_aod': _rmse(y_te_aod, p_te_aod),
            'r2_internal_test_aod':   _r2  (y_te_aod, p_te_aod),
        })

    del X_tr_imp, X_te_imp, train_pred, test_pred, y_train
    gc.collect()

    print(f'  train_rf metrics (residual space):', flush=True)
    print(f'    rmse_train             = {metrics["rmse_train"]:.4f}', flush=True)
    print(f'    rmse_cv_mean           = {metrics["rmse_cv_mean"]:.4f}'
          f'  (± {metrics["rmse_cv_std"]:.4f})', flush=True)
    print(f'    rmse_internal_test     = {metrics["rmse_internal_test"]:.4f}', flush=True)
    print(f'    r2_cv_mean             = {metrics["r2_cv_mean"]:.4f}'
          f'  (± {metrics["r2_cv_std"]:.4f})', flush=True)
    print(f'    r2_internal_test       = {metrics["r2_internal_test"]:.4f}', flush=True)
    if 'rmse_train_aod' in metrics:
        print(f'  train_rf metrics (AOD space — the AERONET-comparable ones):',
              flush=True)
        print(f'    rmse_train_aod         = {metrics["rmse_train_aod"]:.4f}',
              flush=True)
        print(f'    rmse_cv_mean_aod       = {metrics["rmse_cv_mean_aod"]:.4f}'
              f'  (± {metrics["rmse_cv_std_aod"]:.4f})', flush=True)
        print(f'    rmse_internal_test_aod = {metrics["rmse_internal_test_aod"]:.4f}',
              flush=True)
        print(f'    r2_cv_mean_aod         = {metrics["r2_cv_mean_aod"]:.4f}'
              f'  (± {metrics["r2_cv_std_aod"]:.4f})', flush=True)
        print(f'    r2_internal_test_aod   = {metrics["r2_internal_test_aod"]:.4f}',
              flush=True)

    bundle = RFBundle(
        model           = rf_final,
        feature_columns = feature_columns,
        median_impute   = medians,
        training_window = (start.isoformat(), end.isoformat()),
        hyperparams     = hp,
        metrics         = metrics,
        cv_fold_metrics = cv_fold_metrics,
        target_kind     = target_kind,
    )
    save_bundle(bundle, name)
    if return_metrics:
        return bundle, metrics
    return bundle


def tune_rf(start: date = TRAIN_START, end: date = TRAIN_END,
            name: Optional[str] = None,
            target_rows: int = RF_TRAIN_TARGET_ROWS,
            progress: Optional[Callable] = None,
            target_kind: str = RF_TARGET_KIND,
            report_train_rmse: bool = RF_TUNE_REPORT_TRAIN_RMSE,
            ) -> tuple[dict, list[dict]]:
    """Grid search over RF_GRID, selection = **mean CV-R²** (NOT OOB).

    Per fix doc §0.5 + §7.8.1: OOB-R² is optimistic under temporal
    autocorrelation.  5-fold temporal-block CV is the honest signal.

    Final fit is delegated to `train_rf` so the residual-target inverse
    transform and bundle naming stay consistent.
    """
    if name is None:
        name = (RF_RESIDUAL_BUNDLE_NAME
                if target_kind == 'aod_minus_cams' else 'rf_tuned')

    X_df, y_ser, meta = build_training_table(
        start, end, target_rows=target_rows, progress=progress,
    )
    if X_df.empty:
        raise RuntimeError(f'No training rows in [{start}, {end}].')
    X_df, y_ser, meta = _apply_target_kind(X_df, y_ser, meta, target_kind)
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
        fold_r2s, fold_rmses, fold_train_rmses = [], [], []
        for val_mask in cv_masks:
            fold_train = ~val_mask
            rf = _fit_model(X_imp[fold_train], y[fold_train], hp)
            pred = rf.predict(X_imp[val_mask])
            fold_r2s.append(_r2(y[val_mask], pred))
            fold_rmses.append(_rmse(y[val_mask], pred))
            if report_train_rmse:
                train_pred = rf.predict(X_imp[fold_train])
                fold_train_rmses.append(_rmse(y[fold_train], train_pred))
                del train_pred
            del rf, pred
            gc.collect()
        entry = {**hp,
                 'cv_r2_mean':   float(np.mean(fold_r2s)),
                 'cv_r2_std':    float(np.std (fold_r2s)),
                 'cv_rmse_mean': float(np.mean(fold_rmses)),
                 'cv_rmse_std':  float(np.std (fold_rmses))}
        if report_train_rmse:
            entry['train_rmse_mean'] = float(np.mean(fold_train_rmses))
            entry['train_rmse_std']  = float(np.std (fold_train_rmses))
        results.append(entry)
        msg = (f'    hp={hp}  '
               f'cv_r2_mean={entry["cv_r2_mean"]:.3f}  '
               f'cv_rmse_mean={entry["cv_rmse_mean"]:.4f}  ')
        if report_train_rmse:
            msg += f'train_rmse_mean={entry["train_rmse_mean"]:.4f}  '
        msg += f'RSS={_rss_gb():.2f} GB'
        print(msg, flush=True)

    results.sort(key=lambda r: r['cv_r2_mean'], reverse=True)
    best_hp = {k: results[0][k]
               for k in ('n_estimators', 'max_depth', 'min_samples_leaf', 'max_features')}
    train_rf(start=start, end=end, name=name, hp=best_hp,
             target_rows=target_rows, progress=progress,
             target_kind=target_kind)
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
    """RF prediction grid + per-tree-SD uncertainty (or None to skip uncertainty).

    Uses `bundle.feature_columns` so pre-§7.8.1 bundles (rf_tuned, which lists
    lat_rad/lon_rad/tp) and post-§7.8.1 bundles (rf_tuned_residual, which
    drops them and adds antecedent precip / cyclical time / CAMS lag) both
    predict correctly against the current `build_feature_grid` output.
    """
    grids = build_feature_grid(slot_utc, columns=bundle.feature_columns)
    X     = stack_features(grids, columns=bundle.feature_columns)
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
