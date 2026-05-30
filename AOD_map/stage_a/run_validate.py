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
) -> None:
    lines = []
    lines.append('=' * 70)
    lines.append('  Stage A Validation Summary')
    lines.append(f'  Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    lines.append('=' * 70)

    lines.append('\n§8.1  AERONET Validation Metrics\n')
    overall = metrics[metrics.get('confidence_flag', pd.Series()).isna()
                      if 'confidence_flag' in metrics.columns else
                      metrics['site'].isin(['NGHIA_DO', 'Bac_Lieu'])]
    for _, row in metrics[metrics.get('season', '').isna()
                          if 'season' in metrics.columns else
                          metrics.index < 2].iterrows():
        pass

    # Summary: one line per station
    by_site = metrics[
        ~metrics.get('season', pd.Series(index=metrics.index, dtype=str)).notna()
        | True
    ].groupby('site', dropna=False).first().reset_index()
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
                    passed = (val >= target_val) if 'R' in val_col else (val <= target_val)
                    mark   = '✓ PASS' if passed else '✗ FAIL'
                    lines.append(f'  {mark}  {target_key}: {_fmt_metric(val)} '
                                 f'(target {"≥" if "R" in val_col else "≤"} {target_val})')

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
                f"  {row.get('site','?'):10s}  N={int(row.get('N',0)):5d}  "
                f"OLS R²={_fmt_metric(row.get('OLS_R2'))}  "
                f"RANSAC R²={_fmt_metric(row.get('RANSAC_R2'))}  "
                f"lift={_fmt_metric(row.get('RANSAC_R2_lift'))}  "
                f"inlier={_fmt_metric(row.get('inlier_frac'),2)}"
            )

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
                   choices=['all', 'aeronet', 'consistency', 'precip', 'ransac', 'coverage'],
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

    print(f'Stage A Validation')
    print(f'  Period: {start} → {end}  ({args.split})')
    print(f'  Output: {run_dir}')

    from validate import (
        extract_aeronet_pairs,
        stratified_metrics,
        inter_sensor_consistency,
        precip_aware_validation,
        baseline_comparison,
        ransac_diagnostic,
        spatial_coverage_stats,
    )

    # ── §8.1 AERONET pairs ─────────────────────────────────────────────────
    pairs      = pd.DataFrame()
    metrics_df = pd.DataFrame()

    if args.mode in ('all', 'aeronet', 'precip', 'ransac', 'baseline'):
        print('\n[§8.1] Extracting AERONET matched pairs …')
        pairs = extract_aeronet_pairs(start, end)
        print(f'  → {len(pairs)} matched pairs')

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
        consistency_df = inter_sensor_consistency(start, end)
        if not consistency_df.empty:
            consistency_df.to_csv(run_dir / 'inter_sensor_consistency.csv', index=False)
            _print_table(consistency_df, '§8.2  Inter-sensor R² vs Nguyen 2025 baseline')
        else:
            print('  No overlapping sensor data found in merged files.')

    # ── §8.3 Precipitation-aware ──────────────────────────────────────────
    if args.mode in ('all', 'precip') and not pairs.empty:
        print('\n[§8.3] Precipitation-aware validation …')
        precip_df = precip_aware_validation(pairs)
        if not precip_df.empty:
            precip_df.to_csv(run_dir / 'precip_aware.csv', index=False)
            _print_table(precip_df[['site', 'wet_flag', 'N', 'R', 'RMSE', 'Bias', 'pct_EE']],
                         '§8.3  Dry vs post-rain validation')
        else:
            print('  ERA5_Precip not found in merged files (run with --physics enabled).')

    # ── §8.5 Baseline comparison ──────────────────────────────────────────
    if args.mode in ('all', 'baseline') and not pairs.empty:
        print('\n[§8.5] Baseline comparison (merged vs VIIRS-only) …')
        baseline_df = baseline_comparison(pairs)
        if not baseline_df.empty:
            baseline_df.to_csv(run_dir / 'baseline_comparison.csv', index=False)
            _print_table(baseline_df[['site', 'product', 'N', 'R', 'RMSE', 'Bias', 'pct_EE']],
                         '§8.5  Merged vs single-sensor baseline')

    # ── §8.6 RANSAC diagnostic ────────────────────────────────────────────
    ransac_df = pd.DataFrame()

    if args.mode in ('all', 'ransac') and not pairs.empty:
        print('\n[§8.6] RANSAC diagnostic …')
        ransac_df = ransac_diagnostic(pairs)
        if not ransac_df.empty:
            ransac_df.to_csv(run_dir / 'ransac_diagnostic.csv', index=False)
            _print_table(
                ransac_df[['site', 'N', 'OLS_R2', 'RANSAC_R2', 'RANSAC_R2_lift',
                            'inlier_frac', 'ransac_slope', 'ransac_intercept']],
                '§8.6  RANSAC outlier diagnostic',
            )

    # ── Spatial coverage ──────────────────────────────────────────────────
    coverage_df = pd.DataFrame()

    if args.mode in ('all', 'coverage'):
        print('\n[Coverage] Computing daily spatial coverage …')
        coverage_df = spatial_coverage_stats(start, end)
        if not coverage_df.empty:
            coverage_df.to_csv(run_dir / 'coverage_daily.csv', index=False)
            monthly = coverage_df.groupby('month')['coverage_pct'].agg(['mean', 'min', 'max'])
            _print_table(monthly.reset_index(), 'Daily spatial coverage % by month')

    # ── Summary report ────────────────────────────────────────────────────
    if args.mode == 'all':
        _write_summary(run_dir, metrics_df, consistency_df, coverage_df, ransac_df)

    print(f'\nDone. Results → {run_dir}')


if __name__ == '__main__':
    main()
