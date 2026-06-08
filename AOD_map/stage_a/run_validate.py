"""CLI driver: run all Stage A validation checks and write summary reports.

Implements §8.1–8.3, §8.5–8.6 from the thesis plan.

Usage
-----
# Full held-out validation (Jan 2025 – Apr 2026)
python run_validate.py --mode all

# Training-period self-validation (Sep 2022 – Dec 2024)
python run_validate.py --mode all --split train

# Individual checks
python run_validate.py --mode aeronet
python run_validate.py --mode consistency
python run_validate.py --mode precip
python run_validate.py --mode ransac
python run_validate.py --mode coverage

# Custom date range
python run_validate.py --mode all --start 2025-01-01 --end 2025-06-30

Output files are written to OUTPUT_DIR / 'validation' / <timestamp>/ :
    aeronet_pairs.csv          — raw matched pairs
    metrics_stratified.csv     — §8.1 stratified metrics table
    inter_sensor_consistency.csv — §8.2 sensor pair R²
    precip_aware.csv           — §8.3 dry vs wet intervals
    baseline_comparison.csv    — §8.5 merged vs VIIRS-only
    ransac_diagnostic.csv      — §8.6 OLS vs RANSAC
    coverage_daily.csv         — daily spatial coverage %
    summary.txt                — human-readable summary
"""

from __future__ import annotations
import argparse
import sys
import time
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from config import OUTPUT_DIR

# Validation output directory
VALID_DIR = OUTPUT_DIR / 'validation'

# Thesis target values (§9, Table) for pass/fail reporting
TARGETS = {
    'R_nghia_do':  0.90,
    'R_bac_lieu':  0.85,
    'RMSE_nghia_do': 0.30,
    'coverage_pct':  95.0,
}

SPLIT_DATES = {
    'test':  (date(2025, 1, 1),  date(2026, 4, 30)),
    'train': (date(2022, 9, 1),  date(2024, 12, 31)),
    'full':  (date(2022, 9, 1),  date(2026, 4, 30)),
}


# ── Environment banner ────────────────────────────────────────────────────────

def _print_env_banner() -> None:
    """Print library versions to stdout."""
    W = 64
    print("=" * W)
    print("Stage A — Validation — Runtime Environment")
    print("-" * W)
    print(f"  {'Python':<12} {sys.version.split()[0]}")

    _libs = [
        ("numpy",   "NumPy"),
        ("pandas",  "pandas"),
        ("scipy",   "SciPy"),
        ("sklearn", "scikit-learn"),
        ("netCDF4", "netCDF4"),
        ("tqdm",    "tqdm"),
    ]
    for mod_name, label in _libs:
        try:
            mod = __import__(mod_name)
            ver = getattr(mod, "__version__", "?")
            print(f"  {label:<14} {ver}")
        except ImportError:
            print(f"  {label:<14} not installed")

    print("=" * W)


def _fmt_duration(seconds: float) -> str:
    if seconds >= 60:
        m = int(seconds // 60)
        s = seconds - m * 60
        return f"{m}m {s:.1f}s"
    return f"{seconds:.1f}s"


# ── Report helpers ────────────────────────────────────────────────────────────

def _fmt_metric(val, decimals: int = 3) -> str:
    return f'{val:.{decimals}f}' if pd.notna(val) else 'N/A'


def _print_table(df: pd.DataFrame, title: str) -> None:
    print(f'\n{"─"*60}')
    print(f'  {title}')
    print('─' * 60)
    print(df.to_string(index=False))


def _write_summary(
    run_dir: Path,
    metrics: pd.DataFrame,
    consistency: pd.DataFrame,
    coverage: pd.DataFrame,
    ransac: pd.DataFrame,
    pm25_metrics: pd.DataFrame,
    pm25_cases: pd.DataFrame,
) -> None:
    lines = []
    lines.append('=' * 70)
    lines.append('  Stage A Validation Summary')
    lines.append(f'  Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    lines.append('=' * 70)

    lines.append('\n§8.1  AERONET Validation Metrics\n')
    for _, row in metrics.groupby('site').apply(
            lambda g: g.iloc[0]).reset_index(drop=True).iterrows():
        lines.append(
            f"  {row.get('site','?'):15s}  N={int(row.get('N',0)):5d}  "
            f"R={_fmt_metric(row.get('R'))}  "
            f"RMSE={_fmt_metric(row.get('RMSE'))}  "
            f"Bias={_fmt_metric(row.get('Bias'))}  "
            f"%EE={_fmt_metric(row.get('pct_EE'),1)}%"
        )

    # Thesis target check
    lines.append('\nTarget check (§9):')
    if not metrics.empty:
        for site_key, target_key, target_val in [
            ('NGHIA_DO', 'R_nghia_do',   TARGETS['R_nghia_do']),
            ('Bac_Lieu', 'R_bac_lieu',   TARGETS['R_bac_lieu']),
            ('NGHIA_DO', 'RMSE_nghia_do', TARGETS['RMSE_nghia_do']),
        ]:
            m_site = metrics[metrics.get('site', pd.Series()) == site_key]
            if not m_site.empty:
                val_col = 'R' if 'R_' in target_key else 'RMSE'
                val     = m_site.iloc[0].get(val_col, np.nan)
                if pd.notna(val):
                    is_r   = (val_col == 'R')
                    passed = (val >= target_val) if is_r else (val <= target_val)
                    mark   = '✓ PASS' if passed else '✗ FAIL'
                    lines.append(f'  {mark}  {target_key}: {_fmt_metric(val)} '
                                 f'(target {"≥" if is_r else "≤"} {target_val})')

    lines.append('\n§8.2  Inter-sensor R²\n')
    if not consistency.empty:
        for _, row in consistency.iterrows():
            delta = row.get('R2_delta')
            delta_str = f' (Δ {delta:+.3f} vs Nguyen 2025)' if pd.notna(delta) else ''
            lines.append(
                f"  {row['sensor_a']:25s} vs {row['sensor_b']:25s}  "
                f"{row['region']:8s}  N={int(row['N']):6d}  "
                f"R²={_fmt_metric(row['R2'])}{delta_str}"
            )

    lines.append('\n§8.6  RANSAC Diagnostic\n')
    if not ransac.empty:
        for _, row in ransac.iterrows():
            lines.append(
                f"  site={str(row.get('site','?')):10s} "
                f"region={str(row.get('region','ALL')):8s} "
                f"season={str(row.get('season','ALL')):4s}  "
                f"N={int(row.get('N',0)):5d}  "
                f"OLS R²={_fmt_metric(row.get('OLS_R2'))}  "
                f"RANSAC R²={_fmt_metric(row.get('RANSAC_R2'))}  "
                f"lift={_fmt_metric(row.get('RANSAC_R2_lift'))}  "
                f"inlier={_fmt_metric(row.get('inlier_frac'),2)}"
            )

    lines.append('\n§8.4  AOD–PM2.5 Coupling\n')
    if not pm25_metrics.empty:
        overall_pm25 = pm25_metrics[pm25_metrics['label'] == 'ALL']
        for _, row in overall_pm25.iterrows():
            lines.append(
                f"  {row.get('aod_type','?'):22s}  N={int(row.get('N',0)):5d}  "
                f"Pearson r={_fmt_metric(row.get('pearson_r'))}  "
                f"RANSAC R²={_fmt_metric(row.get('ransac_r2'))}  "
                f"Δ={_fmt_metric(row.get('ransac_r2_delta'))} vs Nguyen 2025"
            )
        if not pm25_cases.empty:
            lines.append('\n  Episode case studies (Spearman r, |r|>0.3 = pass):')
            for ep_type, grp in pm25_cases.groupby('episode_type'):
                sr_mean = grp['spearman_r'].mean()
                n_pass  = (grp['spearman_r'].abs() > 0.3).sum()
                lines.append(
                    f"  {ep_type:30s}  mean r={_fmt_metric(sr_mean)}  "
                    f"pass={n_pass}/{len(grp)} stations"
                )
    else:
        lines.append('  No PM2.5 pairs computed (run --mode pm25 or --mode all).')

    if not coverage.empty:
        lines.append('\nSpatial Coverage Summary:')
        monthly = coverage.groupby('month')['coverage_pct'].mean()
        for m, c in monthly.items():
            lines.append(f'  Month {m:2d}: {c:.1f}%')

    out = '\n'.join(lines)
    print(out)
    (run_dir / 'summary.txt').write_text(out)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description='Stage A validation driver',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument('--mode',  default='all',
                   choices=['all', 'aeronet', 'consistency', 'precip', 'pm25',
                            'baseline', 'ransac', 'coverage'],
                   help='Which validation check to run')
    p.add_argument('--split', default='test',
                   choices=['test', 'train', 'full'],
                   help='Date split: test=held-out (2025–2026), train=training, full=all')
    p.add_argument('--start', default=None, help='Override start date YYYY-MM-DD')
    p.add_argument('--end',   default=None, help='Override end date YYYY-MM-DD')
    p.add_argument('--out',   default=None, help='Override output directory')
    return p.parse_args()


def main():
    args  = parse_args()
    start, end = SPLIT_DATES[args.split]
    if args.start:
        start = date.fromisoformat(args.start)
    if args.end:
        end   = date.fromisoformat(args.end)

    # Create timestamped output directory
    run_tag = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_dir = Path(args.out) if args.out else VALID_DIR / f'{args.split}_{run_tag}'
    run_dir.mkdir(parents=True, exist_ok=True)

    _print_env_banner()

    print(f'Stage A Validation')
    print(f'  Period : {start} → {end}  ({args.split})')
    print(f'  Mode   : {args.mode}')
    print(f'  Output : {run_dir}')

    from validate import (
        extract_aeronet_pairs,
        stratified_metrics,
        inter_sensor_consistency,
        precip_aware_validation,
        baseline_comparison,
        ransac_diagnostic,
        spatial_coverage_stats,
        extract_aod_pm25_pairs,
        pm25_coupling_metrics,
        pm25_case_studies,
    )

    wall_t0    = time.perf_counter()
    pairs      = pd.DataFrame()
    metrics_df = pd.DataFrame()

    # ── §8.1 AERONET pairs ─────────────────────────────────────────────────
    if args.mode in ('all', 'aeronet', 'precip', 'ransac', 'baseline'):
        print('\n[§8.1] Extracting AERONET matched pairs …')
        t0    = time.perf_counter()
        pairs = extract_aeronet_pairs(start, end)
        print(f'  → {len(pairs)} matched pairs  [{_fmt_duration(time.perf_counter() - t0)}]')

        if pairs.empty:
            print('  No pairs found. Check MERGED_DIR or date range.')
        else:
            pairs.to_csv(run_dir / 'aeronet_pairs.csv', index=False)
            print(f'  Saved: aeronet_pairs.csv')

            metrics_df = stratified_metrics(pairs)
            metrics_df.to_csv(run_dir / 'metrics_stratified.csv', index=False)
            _print_table(
                metrics_df[['site', 'season', 'confidence_flag', 'N', 'R', 'RMSE', 'Bias', 'pct_EE']]
                           .dropna(how='all'),
                '§8.1  Stratified AERONET validation',
            )

    # ── §8.2 Internal consistency ─────────────────────────────────────────
    consistency_df = pd.DataFrame()

    if args.mode in ('all', 'consistency'):
        print('\n[§8.2] Inter-sensor consistency check …')
        t0             = time.perf_counter()
        consistency_df = inter_sensor_consistency(start, end)
        print(f'  [{_fmt_duration(time.perf_counter() - t0)}]')
        if not consistency_df.empty:
            consistency_df.to_csv(run_dir / 'inter_sensor_consistency.csv', index=False)
            _print_table(consistency_df, '§8.2  Inter-sensor R² vs Nguyen 2025 baseline')
        else:
            print('  No overlapping sensor data found in merged files.')

    # ── §8.3 Precipitation-aware ──────────────────────────────────────────
    if args.mode in ('all', 'precip') and not pairs.empty:
        print('\n[§8.3] Precipitation-aware validation …')
        t0        = time.perf_counter()
        precip_df = precip_aware_validation(pairs)
        print(f'  [{_fmt_duration(time.perf_counter() - t0)}]')
        if not precip_df.empty:
            precip_df.to_csv(run_dir / 'precip_aware.csv', index=False)
            _print_table(precip_df[['site', 'wet_flag', 'N', 'R', 'RMSE', 'Bias', 'pct_EE']],
                         '§8.3  Dry vs post-rain validation')
        else:
            print('  ERA5_Precip not found in merged files (run with --physics enabled).')

    # ── §8.4 PM2.5 coupling ───────────────────────────────────────────────
    pm25_met_df  = pd.DataFrame()
    pm25_case_df = pd.DataFrame()

    if args.mode in ('all', 'pm25'):
        print('\n[§8.4] AOD–PM2.5 coupling …')
        t0      = time.perf_counter()
        pm25_df = extract_aod_pm25_pairs(start, end)
        print(f'  → {len(pm25_df)} station-days  [{_fmt_duration(time.perf_counter() - t0)}]')
        if pm25_df.empty:
            print('  No PM2.5 pairs found.  Check PM25_DIR / STATIONS_META path.')
        else:
            pm25_df.to_csv(run_dir / 'pm25_pairs.csv', index=False)
            print(f'  Saved: pm25_pairs.csv')

            pm25_met_df = pm25_coupling_metrics(pm25_df)
            pm25_met_df.to_csv(run_dir / 'pm25_coupling_metrics.csv', index=False)
            _print_table(
                pm25_met_df[['label', 'aod_type', 'N', 'pearson_r', 'spearman_r',
                              'ransac_r2', 'ransac_r2_delta', 'inlier_frac']]
                           .dropna(how='all'),
                '§8.4  AOD–PM2.5 coupling metrics',
            )

            pm25_case_df = pm25_case_studies(pm25_df)
            if not pm25_case_df.empty:
                pm25_case_df.to_csv(run_dir / 'pm25_case_studies.csv', index=False)
                _print_table(
                    pm25_case_df[['episode_type', 'station_name', 'region',
                                  'N_days', 'spearman_r', 'spearman_p',
                                  'peak_pm25', 'mean_aod_phys']],
                    '§8.4  PM2.5 episode case studies',
                )
            else:
                print('  No episodes met criteria (too few days or missing AOD_phys).')

    # ── §8.5 Baseline comparison ──────────────────────────────────────────
    if args.mode in ('all', 'baseline') and not pairs.empty:
        print('\n[§8.5] Baseline comparison (merged vs VIIRS-only) …')
        t0          = time.perf_counter()
        baseline_df = baseline_comparison(pairs)
        print(f'  [{_fmt_duration(time.perf_counter() - t0)}]')
        if not baseline_df.empty:
            baseline_df.to_csv(run_dir / 'baseline_comparison.csv', index=False)
            _print_table(baseline_df[['site', 'product', 'N', 'R', 'RMSE', 'Bias', 'pct_EE']],
                         '§8.5  Merged vs single-sensor baseline')

    # ── §8.6 RANSAC diagnostic ────────────────────────────────────────────
    ransac_df = pd.DataFrame()

    if args.mode in ('all', 'ransac') and not pairs.empty:
        print('\n[§8.6] RANSAC diagnostic …')
        t0        = time.perf_counter()
        ransac_df = ransac_diagnostic(pairs)
        print(f'  [{_fmt_duration(time.perf_counter() - t0)}]')
        if not ransac_df.empty:
            ransac_df.to_csv(run_dir / 'ransac_diagnostic.csv', index=False)
            _print_table(
                ransac_df[['site', 'region', 'season', 'N',
                            'OLS_R2', 'RANSAC_R2', 'RANSAC_R2_lift',
                            'inlier_frac', 'ransac_slope', 'ransac_intercept']],
                '§8.6  RANSAC outlier diagnostic',
            )

    # ── Spatial coverage ──────────────────────────────────────────────────
    coverage_df = pd.DataFrame()

    if args.mode in ('all', 'coverage'):
        print('\n[Coverage] Computing daily spatial coverage …')
        t0          = time.perf_counter()
        coverage_df = spatial_coverage_stats(start, end)
        print(f'  [{_fmt_duration(time.perf_counter() - t0)}]')
        if not coverage_df.empty:
            coverage_df.to_csv(run_dir / 'coverage_daily.csv', index=False)
            monthly = coverage_df.groupby('month')['coverage_pct'].agg(['mean', 'min', 'max'])
            _print_table(monthly.reset_index(), 'Daily spatial coverage % by month')

    # ── Summary report ────────────────────────────────────────────────────
    if args.mode == 'all':
        _write_summary(run_dir, metrics_df, consistency_df, coverage_df, ransac_df,
                       pm25_met_df, pm25_case_df)

    print(f'\n{"─" * 64}')
    print(f'Done.  [total: {_fmt_duration(time.perf_counter() - wall_t0)}]')
    print(f'Results → {run_dir}')


if __name__ == '__main__':
    main()
