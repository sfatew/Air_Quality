"""Stage B end-to-end runner — script translation of run_stage_b.ipynb.

Designed for long unattended runs inside tmux. Run a single phase or the whole
pipeline; validation artefacts are written to a timestamped sub-directory of
`cfg.VALIDATION_DIR` (== /home/slow_data/Air_Quality/Stage_B/validation).

Examples
--------
    # Everything end-to-end (recommended first run)
    python run_stage_b.py --all

    # Only B1 (variogram fit + ST kriging products)
    python run_stage_b.py --b1

    # Only B2 (RF tune + fill_range), reusing the existing variogram
    python run_stage_b.py --b2

    # Only the validation block (assumes B1+B2 already produced NC files)
    python run_stage_b.py --validate

    # Re-run validation against an alternative RF bundle name
    python run_stage_b.py --validate --model-name rf_tuned

    # Force overwrite of any existing NC files
    python run_stage_b.py --all --overwrite
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

os.environ.setdefault('HDF5_USE_FILE_LOCKING', 'FALSE')

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import config as cfg
import kriging as kg
import rf_gapfill as rf
import validate as vb


# ── helpers ─────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime('%Y%m%d_%H%M%S')


def _save_df(df: pd.DataFrame, out_dir: Path, name: str) -> Path:
    path = out_dir / f'{name}.csv'
    df.to_csv(path, index=False)
    print(f'  wrote {path}  ({len(df)} rows)')
    return path


def _save_json(obj: dict, out_dir: Path, name: str) -> Path:
    path = out_dir / f'{name}.json'

    def _coerce(v):
        if isinstance(v, (np.floating,)): return float(v)
        if isinstance(v, (np.integer,)):  return int(v)
        if isinstance(v, (np.bool_,)):    return bool(v)
        return v

    serial = {k: _coerce(v) for k, v in obj.items()}
    path.write_text(json.dumps(serial, indent=2, default=str))
    print(f'  wrote {path}')
    return path


# ── phases ──────────────────────────────────────────────────────────────────

def _parse_date(s: str) -> date:
    return datetime.strptime(s, '%Y-%m-%d').date()


def run_b1(overwrite: bool, run_start, run_end, infer_only: bool = False) -> None:
    if infer_only:
        print(f'\n=== B1: ST kriging  (reuse saved variogram, '
              f'{run_start} → {run_end} apply) ===')
        vgm = kg.load_variogram()
        print(f'  loaded variogram from {kg._VGM_PATH}')
    else:
        print(f'\n=== B1: ST kriging  ({cfg.TRAIN_START} → {cfg.TRAIN_END} fit, '
              f'{run_start} → {run_end} apply) ===')
        vgm = kg.fit_and_save_variogram(start=cfg.TRAIN_START, end=cfg.TRAIN_END,
                                        progress=tqdm)
    print(f'  target              : {cfg.B1_VARIOGRAM_TARGET}')
    print(f'  metric var          : {vgm.metric.var:.4f}')
    print(f'  metric range_km     : {vgm.metric.len_scale:.2f}')
    print(f'  metric nugget       : {vgm.metric.nugget:.4f}')
    print(f'  anisotropy k (km/h) : {vgm.k_km_per_hour:.3f}')

    t0 = time.time()
    n = kg.run(run_start, run_end, vgm=vgm,
               overwrite=overwrite, progress=tqdm)
    print(f'  B1 wrote {n} NC files in {time.time() - t0:.1f}s.')


def run_b2(model_name: str, overwrite: bool, run_start, run_end,
           report_train_rmse: bool = cfg.RF_TUNE_REPORT_TRAIN_RMSE,
           infer_only: bool = False,
           no_tune: bool = False) -> None:
    if infer_only:
        print(f'\n=== B2: Random Forest  (reuse saved bundle {model_name!r}, '
              f'{run_start} → {run_end} apply) ===')
    elif no_tune:
        print(f'\n=== B2: Random Forest  (train with config defaults, no grid '
              f'search, {cfg.TRAIN_START} → {cfg.TRAIN_END}) ===')
        hp = {
            'n_estimators':     cfg.RF_N_ESTIMATORS,
            'max_depth':        cfg.RF_MAX_DEPTH,
            'min_samples_leaf': cfg.RF_MIN_SAMPLES_LEAF,
            'max_features':     cfg.RF_MAX_FEATURES,
        }
        print(f'  hyperparameters: {hp}')
        rf.train_rf(
            start=cfg.TRAIN_START, end=cfg.TRAIN_END,
            name=model_name, hp=hp, progress=tqdm,
        )
    else:
        print(f'\n=== B2: Random Forest  (tune on {cfg.TRAIN_START} → {cfg.TRAIN_END}) ===')
        best_hp, tune_results = rf.tune_rf(
            start=cfg.TRAIN_START, end=cfg.TRAIN_END,
            name=model_name, progress=tqdm,
            report_train_rmse=report_train_rmse,
        )
        print(f'  best hyperparameters: {best_hp}')
        pd.DataFrame(tune_results).to_csv(
            cfg.MODELS_DIR / f'{model_name}_tune_results.csv', index=False
        )

    bundle = rf.load_bundle(model_name)
    print(f'  training window : {bundle.training_window}')
    print(f'  internal consistency : {vb.internal_consistency(bundle.metrics)}')

    t0 = time.time()
    n = rf.fill_range(start=run_start, end=run_end,
                      bundle=bundle, overwrite=overwrite, progress=tqdm)
    print(f'  B2 wrote {n} NC files in {time.time() - t0:.1f}s.')


def run_validation(model_name: str, test_start, test_end) -> Path:
    out_dir = cfg.VALIDATION_DIR / f'run_{_ts()}'
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f'\n=== Validation  ({test_start} → {test_end}) ===')
    print(f'  output dir : {out_dir}')

    # 1. blind-only AERONET pairs for both candidates
    pairs_rf   = vb.aeronet_pairs(test_start, test_end, candidate='rf',
                                   blind_only=True, progress=tqdm)
    pairs_krig = vb.aeronet_pairs(test_start, test_end, candidate='st_kriging',
                                   blind_only=True, progress=tqdm)
    print(f'  AERONET-blind pairs - RF: {len(pairs_rf)}, '
          f'ST kriging: {len(pairs_krig)}')
    _save_df(pairs_rf,   out_dir, 'pairs_rf_blind')
    _save_df(pairs_krig, out_dir, 'pairs_st_kriging_blind')

    # 2. per-site metric panel (RF)
    _save_df(vb.metric_panel(pairs_rf), out_dir, 'metric_panel_rf')

    # 3. head-to-head comparison
    panel = vb.compare_candidates({'B2_RF': pairs_rf, 'B1_ST_krig': pairs_krig})
    _save_df(panel, out_dir, 'compare_candidates')

    # 4. paired skill (RF − ST kriging on identical keys)
    paired = vb.paired_skill(pairs_rf, pairs_krig)
    _save_json(paired, out_dir, 'paired_skill_rf_minus_stk')

    # 5. coverage audit (RF)
    cov_rf = vb.coverage_audit(test_start, test_end, candidate='rf',
                               progress=tqdm)
    _save_df(cov_rf, out_dir, 'coverage_audit_rf')

    # 6. SSO-stratified RMSE
    _save_df(vb.sso_stratified_rmse(pairs_rf), out_dir, 'sso_stratified_rmse_rf')

    # 7. RF variable importance
    try:
        vi = vb.variable_importance(model_name)
        _save_df(vi, out_dir, 'variable_importance')
    except Exception as exc:
        print(f'  variable_importance skipped: {exc}')

    # 8. cloud-period recovery (RF)
    _save_df(vb.cloud_period_recovery(test_start, test_end, candidate='rf'),
             out_dir, 'cloud_period_recovery_rf')

    # 9. success table (uses full — not blind-only — pairs)
    pairs_full_rf = vb.aeronet_pairs(test_start, test_end, candidate='rf',
                                      blind_only=False, progress=tqdm)
    _save_df(pairs_full_rf, out_dir, 'pairs_rf_all')
    _save_df(vb.success_table(pairs_full_rf, cov_rf), out_dir, 'success_table')

    # 10. internal-consistency snapshot of the bundle in use
    try:
        bundle = rf.load_bundle(model_name)
        _save_json(vb.internal_consistency(bundle.metrics),
                   out_dir, 'internal_consistency')
    except Exception as exc:
        print(f'  internal_consistency skipped: {exc}')

    print(f'\nValidation complete → {out_dir}')
    return out_dir


# ── entrypoint ──────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--b1',       action='store_true', help='run B1 (ST kriging)')
    ap.add_argument('--b2',       action='store_true', help='run B2 (RF gap-fill)')
    ap.add_argument('--validate', action='store_true', help='run validation block')
    ap.add_argument('--all',      action='store_true', help='run B1 + B2 + validation')
    ap.add_argument('--overwrite', action='store_true',
                    help='overwrite existing NC outputs (default: skip existing)')
    default_model_name = (
        cfg.RF_RESIDUAL_BUNDLE_NAME
        if cfg.RF_TARGET_KIND == 'aod_minus_cams'
        else 'rf_tuned'
    )
    ap.add_argument('--model-name', default=default_model_name,
                    help=f'RF bundle name (default: {default_model_name}, '
                         f'derived from RF_TARGET_KIND={cfg.RF_TARGET_KIND!r}). '
                         f'Pass --model-name rf_tuned to keep training/load '
                         f'the legacy direct-AOD bundle.')
    ap.add_argument('--train-rmse', action='store_true',
                    default=cfg.RF_TUNE_REPORT_TRAIN_RMSE,
                    help=f'tune_rf: also report per-combo train_rmse_mean '
                         f'(adds ~10–20%% wall-clock to grid sweep). '
                         f'Default from config.RF_TUNE_REPORT_TRAIN_RMSE='
                         f'{cfg.RF_TUNE_REPORT_TRAIN_RMSE}.')
    ap.add_argument('--start', type=_parse_date, default=None,
                    metavar='YYYY-MM-DD',
                    help='override apply window start (default: cfg.TEST_START). '
                         'Applies to B1, B2, and validation phases.')
    ap.add_argument('--end', type=_parse_date, default=None,
                    metavar='YYYY-MM-DD',
                    help='override apply window end (default: cfg.TEST_END).')
    ap.add_argument('--infer-only', action='store_true',
                    help='skip variogram refit (B1) and RF tune (B2); reuse the '
                         'saved st_variogram.json and model bundle. Use after a '
                         'CAMS backfill to re-run inference on new slots without '
                         'paying for training again. Pair with --overwrite to '
                         'replace existing per-slot NC files.')
    ap.add_argument('--no-tune', action='store_true',
                    help='B2 only: skip the RF_GRID grid search and train once '
                         'with the RF_* defaults from config.py '
                         '(n_estimators, max_depth, min_samples_leaf, '
                         'max_features). Mutually exclusive with --infer-only.')
    args = ap.parse_args()

    if not (args.b1 or args.b2 or args.validate or args.all):
        ap.error('pick at least one of --b1 --b2 --validate --all')

    if args.no_tune and args.infer_only:
        ap.error('--no-tune and --infer-only are mutually exclusive')

    for d in (cfg.ST_KRIGING_DIR, cfg.RF_OUTPUT_DIR, cfg.MODELS_DIR,
              cfg.VALIDATION_DIR):
        d.mkdir(parents=True, exist_ok=True)

    run_start = args.start if args.start is not None else cfg.TEST_START
    run_end   = args.end   if args.end   is not None else cfg.TEST_END
    print(f'Training : {cfg.TRAIN_START} → {cfg.TRAIN_END}')
    print(f'Held-out : {cfg.TEST_START} → {cfg.TEST_END}')
    print(f'Full run : {run_start} → {run_end}'
          f'{"  [infer-only: reusing saved artifacts]" if args.infer_only else ""}')
    print(f'Validation root : {cfg.VALIDATION_DIR}')

    if args.all or args.b1:
        run_b1(args.overwrite, run_start, run_end, infer_only=args.infer_only)
    if args.all or args.b2:
        run_b2(args.model_name, args.overwrite, run_start, run_end,
               report_train_rmse=args.train_rmse,
               infer_only=args.infer_only,
               no_tune=args.no_tune)
    if args.all or args.validate:
        run_validation(args.model_name, run_start, run_end)


if __name__ == '__main__':
    main()
