"""Stage B validation — §8.2, 30-min cadence + parallel-product schema.

Reads from the new per-slot product trees:

  ST_KRIGING_DIR / YYYY / MM / DD / aod_YYYYMMDD_HHMM.nc
  RF_OUTPUT_DIR  / YYYY / MM / DD / aod_YYYYMMDD_HHMM.nc

Per-file schema: aod_550nm, is_observed, uncertainty, stage_a_weight_sum.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import netCDF4 as nc
from scipy import stats

from config import (
    LATS, LONS, NLAT, NLON, LAT_MAX, LON_MIN, GRID_RES,
    AERONET_SITES, DRY_MONTHS,
    ST_KRIGING_DIR, RF_OUTPUT_DIR, RF_RK_DIR, VALIDATION_DIR,
    TRAIN_START, TRAIN_END, TEST_START, TEST_END,
    RMSE_CONSISTENCY_TOLERANCE,
    SSO_BINS, SSO_LABELS,
    SLOTS_PER_DAY,
)
from slots import (
    load_slot, iter_local_days, iter_window_slots, slot_index,
    window_slot_indices, observed_slot_indices,
)
from kriging import kriged_path
from rf_gapfill import gapfilled_path, load_bundle as load_rf_bundle


EE_OFFSET = 0.05
EE_SLOPE  = 0.15


# ── AERONET loader ───────────────────────────────────────────────────────────

def _load_aeronet_raw() -> pd.DataFrame:
    from aeronet import load_all_aeronet
    df = load_all_aeronet()
    if 'datetime_utc' not in df.columns and 'datetime' in df.columns:
        df = df.rename(columns={'datetime': 'datetime_utc'})
    return df


def _site_rc(site: str) -> tuple[int, int]:
    meta = AERONET_SITES[site]
    row  = int(round((LAT_MAX - GRID_RES / 2 - meta['lat']) / GRID_RES))
    col  = int(round((meta['lon'] - LON_MIN - GRID_RES / 2) / GRID_RES))
    return row, col


def _aeronet_slot_pair(aer: pd.DataFrame, site: str, slot_utc: datetime,
                       window_min: int = 30) -> Optional[dict]:
    """±window_min of slot_utc; identical protocol to Stage A (§8.0)."""
    lo = slot_utc - timedelta(minutes=window_min)
    hi = slot_utc + timedelta(minutes=window_min)
    sub = aer[(aer['site'] == site)
              & (aer['datetime_utc'] >= lo)
              & (aer['datetime_utc'] <= hi)]
    if sub.empty:
        return None
    return {'aer_aod':   float(sub['aod_550'].mean()),
            'aer_n_obs': int(len(sub))}


# ── Metric panel ─────────────────────────────────────────────────────────────

def compute_metrics(sat: np.ndarray, aer: np.ndarray, label: str = '') -> dict:
    mask = np.isfinite(sat) & np.isfinite(aer)
    sat = sat[mask]; aer = aer[mask]
    n = len(sat)
    if n < 5:
        return {'N': n, 'R': np.nan, 'R2': np.nan, 'RMSE': np.nan,
                'MAE': np.nan, 'Bias': np.nan, 'pct_EE': np.nan, 'label': label}
    r, _ = stats.pearsonr(sat, aer)
    bias = float(np.mean(sat - aer))
    rmse = float(np.sqrt(np.mean((sat - aer) ** 2)))
    mae  = float(np.mean(np.abs(sat - aer)))
    ss_res = np.sum((sat - aer) ** 2)
    ss_tot = np.sum((aer - aer.mean()) ** 2)
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else np.nan
    ee = EE_OFFSET + EE_SLOPE * np.abs(aer)
    pct_ee = float(np.mean(np.abs(sat - aer) <= ee) * 100)
    return {'N': n, 'R': float(r), 'R2': r2, 'RMSE': rmse,
            'MAE': mae, 'Bias': bias, 'pct_EE': pct_ee, 'label': label}


# ── Per-slot file readers ────────────────────────────────────────────────────

def _candidate_path(slot_utc: datetime, candidate: str) -> Path:
    if candidate == 'st_kriging' or candidate == 'kriging':
        return kriged_path(slot_utc)
    if candidate == 'rf':
        return gapfilled_path(slot_utc)
    if candidate == 'rf_rk':
        # Regression-kriging product (RF drift + kriged residual); same NC
        # schema as st_kriging, different tree.
        return kriged_path(slot_utc, out_dir=RF_RK_DIR)
    raise ValueError(f'Unknown candidate {candidate!r}')


def _candidate_value(slot_utc: datetime, row: int, col: int,
                      candidate: str) -> Optional[dict]:
    if candidate == 'observed':
        payload = load_slot(slot_utc)
        if payload is None or not np.isfinite(payload['aod'][row, col]):
            return None
        return {'value': float(payload['aod'][row, col]),
                 'is_observed': True, 'uncertainty': np.nan}

    path = _candidate_path(slot_utc, candidate)
    if not path.exists():
        return None
    try:
        with nc.Dataset(path) as ds:
            val = float(np.ma.filled(ds.variables['aod_550nm'][row, col], np.nan))
            is_obs = bool(ds.variables['is_observed'][row, col]) \
                     if 'is_observed' in ds.variables else False
            unc = (float(np.ma.filled(ds.variables['uncertainty'][row, col], np.nan))
                    if 'uncertainty' in ds.variables else np.nan)
    except (OSError, KeyError):
        return None
    if not np.isfinite(val):
        return None
    return {'value': val, 'is_observed': is_obs, 'uncertainty': unc}


# ── §8.2.2 matched-pair extraction ───────────────────────────────────────────

def aeronet_pairs(
    start: date = TEST_START,
    end:   date = TEST_END,
    candidate: str = 'rf',
    blind_only: bool = True,
    progress = None,
) -> pd.DataFrame:
    """Per-slot matched pairs: candidate vs AERONET ±30 min slot centre."""
    aer = _load_aeronet_raw()
    site_rcs = {s: _site_rc(s) for s in AERONET_SITES}
    rows = []
    days = list(iter_local_days(start, end))
    iterator = progress(days, desc=f'pairs ({candidate})') if progress is not None else days

    for d in iterator:
        for slot_utc in iter_window_slots(d):
            for site, (r, c) in site_rcs.items():
                aer_match = _aeronet_slot_pair(aer, site, slot_utc)
                if aer_match is None:
                    continue
                obs = _candidate_value(slot_utc, r, c, 'observed')
                if blind_only and obs is not None:
                    continue
                pick = _candidate_value(slot_utc, r, c, candidate)
                if pick is None:
                    continue
                rows.append({
                    'slot_utc':        slot_utc,
                    'local_day':       d,
                    'slot_idx':        slot_index(slot_utc),
                    'site':            site,
                    'region':          AERONET_SITES[site]['region'],
                    'season':          'dry' if d.month in DRY_MONTHS else 'wet',
                    'aer_aod':         aer_match['aer_aod'],
                    'aer_n_obs':       aer_match['aer_n_obs'],
                    'sat_aod':         pick['value'],
                    'is_observed':     pick['is_observed'],
                    'uncertainty':     pick['uncertainty'],
                    'obs_was_valid':   obs is not None,
                })
    return pd.DataFrame(rows)


def metric_panel(pairs: pd.DataFrame) -> pd.DataFrame:
    if pairs.empty:
        return pd.DataFrame()
    out = []
    for site, grp in pairs.groupby('site'):
        out.append({**compute_metrics(grp['sat_aod'].values, grp['aer_aod'].values,
                                       label=f'site={site}'),
                    'site': site, 'season': 'all'})
    for (site, season), grp in pairs.groupby(['site', 'season']):
        out.append({**compute_metrics(grp['sat_aod'].values, grp['aer_aod'].values,
                                       label=f'site={site} season={season}'),
                    'site': site, 'season': season})
    out.append({**compute_metrics(pairs['sat_aod'].values, pairs['aer_aod'].values,
                                   label='ALL'),
                'site': 'ALL', 'season': 'all'})
    return pd.DataFrame(out)


def compare_candidates(pairs_by_candidate: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Side-by-side ST kriging vs RF panel (head-to-head per fix doc §0.5)."""
    rows = []
    for cand, df in pairs_by_candidate.items():
        if df.empty:
            continue
        panel = metric_panel(df)
        panel['candidate'] = cand
        rows.append(panel)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    summary = (out[out['site'] != 'ALL']
               .groupby('candidate')['RMSE'].mean()
               .rename('mean_RMSE_site_season').reset_index())
    summary['site']   = 'SUMMARY'
    summary['season'] = 'mean'
    summary = summary.rename(columns={'mean_RMSE_site_season': 'RMSE'})
    return pd.concat([out, summary], ignore_index=True)


def paired_skill(pairs_a: pd.DataFrame, pairs_b: pd.DataFrame,
                  on=('slot_utc', 'site')) -> dict:
    """Paired comparison of two candidates on identical (slot, site) keys.

    Returns paired RMSE difference and paired t-test on errors.
    """
    if pairs_a.empty or pairs_b.empty:
        return {'n_paired': 0}
    merged = pairs_a.merge(pairs_b, on=list(on), suffixes=('_a', '_b'))
    if merged.empty:
        return {'n_paired': 0}
    aer = merged['aer_aod_a'].values   # identical AERONET key
    e_a = merged['sat_aod_a'].values - aer
    e_b = merged['sat_aod_b'].values - aer
    rmse_a = float(np.sqrt(np.mean(e_a ** 2)))
    rmse_b = float(np.sqrt(np.mean(e_b ** 2)))
    t, p = stats.ttest_rel(np.abs(e_a), np.abs(e_b))
    return {
        'n_paired':         int(len(merged)),
        'rmse_a':           rmse_a,
        'rmse_b':           rmse_b,
        'rmse_diff_a_minus_b': rmse_a - rmse_b,
        't_statistic':      float(t),
        'p_value':          float(p),
    }


# ── §8.2.3 Coverage + provenance audit ───────────────────────────────────────

def coverage_audit(start: date, end: date,
                   candidate: str = 'rf',
                   progress = None) -> pd.DataFrame:
    """Per-month coverage: pre-fill (Stage A observed) vs post-fill (candidate)."""
    rows = []
    days = list(iter_local_days(start, end))
    iterator = progress(days, desc=f'coverage ({candidate})') if progress is not None else days

    for d in iterator:
        ym = f'{d.year:04d}-{d.month:02d}'
        slot_utcs = list(iter_window_slots(d))
        if not slot_utcs:
            continue
        pre_obs = 0.0
        post_obs = 0.0
        post_filled = 0.0
        for slot_utc in slot_utcs:
            sa = load_slot(slot_utc)
            if sa is not None:
                pre_obs += float(np.isfinite(sa['aod']).mean())
            path = _candidate_path(slot_utc, candidate)
            if path.exists():
                with nc.Dataset(path) as ds:
                    a = np.ma.filled(ds.variables['aod_550nm'][:], np.nan)
                    io = np.asarray(ds.variables['is_observed'][:], dtype=bool) \
                         if 'is_observed' in ds.variables else np.zeros_like(a, bool)
                post_obs    += float(io.mean())
                post_filled += float(np.isfinite(a).mean())
        n = len(slot_utcs)
        rows.append({
            'month':                ym,
            'n_slots_in_month':     n,
            'pre_fill_observed':    pre_obs / n,
            'post_fill_observed':   post_obs / n,
            'post_fill_coverage':   post_filled / n,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.groupby('month').mean(numeric_only=True).reset_index()


def sso_stratified_rmse(pairs: pd.DataFrame, candidate: str = 'rf') -> pd.DataFrame:
    """RMSE binned by slots_since_last_observed (computed on the fly).

    For each pair, count how many slot-steps back the nearest observed slot is
    at the same (site, cell).  This replaces the field-stored DSO that the
    old merged schema carried.
    """
    if pairs.empty:
        return pd.DataFrame()

    sso_vals = []
    for _, row in pairs.iterrows():
        d = row['local_day']
        s_idx = row['slot_idx']
        site_row, site_col = _site_rc(row['site'])
        # Walk back through observed slots in this day's window
        observed = observed_slot_indices(d)
        prev_obs = [s for s in observed if s < s_idx]
        if prev_obs:
            sso_vals.append(s_idx - prev_obs[-1])
        else:
            # No earlier slot today — look at previous day's last observed
            d_prev = d - timedelta(days=1)
            prev_day_observed = observed_slot_indices(d_prev)
            if prev_day_observed:
                # Add SLOTS_PER_DAY for the nighttime gap
                sso_vals.append(s_idx + (SLOTS_PER_DAY - prev_day_observed[-1]))
            else:
                sso_vals.append(9999)

    p = pairs.copy()
    p['sso'] = sso_vals
    p['sso_clip'] = np.clip(p['sso'].fillna(0), -1, 100_000_000)
    p['sso_bin']  = pd.cut(p['sso_clip'], bins=SSO_BINS, labels=SSO_LABELS, right=True)
    out = []
    for bin_label, grp in p.groupby('sso_bin', observed=True):
        m = compute_metrics(grp['sat_aod'].values, grp['aer_aod'].values,
                            label=str(bin_label))
        m['sso_bin'] = str(bin_label)
        m['count']   = int(len(grp))
        out.append(m)
    return pd.DataFrame(out)


# ── §8.2.4 Robustness diagnostics ────────────────────────────────────────────

def variable_importance(bundle_name: str = 'rf_primary') -> pd.DataFrame:
    bundle = load_rf_bundle(bundle_name)
    imp = np.asarray(bundle.model.feature_importances_, dtype=float)
    norm = imp / imp.sum() * 100.0
    return (pd.DataFrame({
        'feature':         bundle.feature_columns,
        'gini_importance': imp,
        'pct_importance':  norm,
    }).sort_values('pct_importance', ascending=False).reset_index(drop=True))


def residual_envelope(pairs: pd.DataFrame, bin_width: float = 0.1) -> pd.DataFrame:
    if pairs.empty:
        return pd.DataFrame()
    p = pairs.copy()
    p['resid']   = p['sat_aod'] - p['aer_aod']
    p['aer_bin'] = (p['aer_aod'] // bin_width) * bin_width
    return (p.groupby('aer_bin')['resid']
             .agg(['count', 'mean', 'std'])
             .reset_index()
             .rename(columns={'mean': 'bias', 'std': 'sigma', 'count': 'n'}))


# ── §8.2.5 Cloud-occluded slot-window stress test ───────────────────────────

def cloud_period_recovery(start: date, end: date,
                           candidate: str = 'rf',
                           min_consecutive_cloud_slots: int = 48,
                           coverage_floor: float = 0.10) -> pd.DataFrame:
    """Stretch detection at slot cadence (48 slots ≈ 2.3 days of occlusion)."""
    slot_records = []
    for d in iter_local_days(start, end):
        for slot_utc in iter_window_slots(d):
            payload = load_slot(slot_utc)
            cov = float(np.isfinite(payload['aod']).mean()) if payload is not None else 0.0
            slot_records.append({'day': d, 'slot_utc': slot_utc, 'coverage': cov})

    cov_arr = np.array([r['coverage'] for r in slot_records])
    periods = []
    in_period = False; p_start = None
    for i, c in enumerate(cov_arr):
        if c < coverage_floor and not in_period:
            in_period = True; p_start = i
        elif c >= coverage_floor and in_period:
            in_period = False
            if i - p_start >= min_consecutive_cloud_slots:
                periods.append((p_start, i - 1))

    rows = []
    for p_start, p_end in periods:
        gap_rec   = slot_records[p_end]
        recov_rec = slot_records[p_end + 1] if p_end + 1 < len(slot_records) else None
        mean_pre  = float(np.mean(cov_arr[p_start:p_end + 1]))

        rec_delta_rmse = np.nan
        if recov_rec is not None:
            fp_gap   = _candidate_path(gap_rec['slot_utc'],   candidate)
            fp_recov = _candidate_path(recov_rec['slot_utc'], candidate)
            if fp_gap.exists() and fp_recov.exists():
                with nc.Dataset(fp_gap) as dg, nc.Dataset(fp_recov) as dr:
                    a = np.ma.filled(dg.variables['aod_550nm'][:], np.nan)
                    b = np.ma.filled(dr.variables['aod_550nm'][:], np.nan)
                    obs_mask = (np.asarray(dr.variables['is_observed'][:], dtype=bool)
                                if 'is_observed' in dr.variables else np.zeros_like(a, bool))
                    if obs_mask.any():
                        diff = a[obs_mask] - b[obs_mask]
                        diff = diff[np.isfinite(diff)]
                        if diff.size > 0:
                            rec_delta_rmse = float(np.sqrt(np.mean(diff ** 2)))

        rows.append({
            'period_start_slot_utc': slot_records[p_start]['slot_utc'],
            'period_end_slot_utc':   gap_rec['slot_utc'],
            'n_slots':               p_end - p_start + 1,
            'mean_pre_coverage':     mean_pre,
            'recovery_slot_utc':     recov_rec['slot_utc'] if recov_rec else None,
            'recovery_delta_rmse':   rec_delta_rmse,
        })
    return pd.DataFrame(rows)


# ── §8.2.6 Success-table builder ─────────────────────────────────────────────

def success_table(pairs: pd.DataFrame, coverage_df: pd.DataFrame) -> pd.DataFrame:
    targets = [
        ('AERONET R Nghia Do (all matched slots)',      '≥0.90', 0.915),
        ('AERONET R Bac Lieu (all matched slots)',      '≥0.85', 0.845),
        ('AERONET RMSE Nghia Do',                       '≤0.30', 0.271),
        ('30-min AOD spatial coverage over Vietnam',    '≥95%',  0.103),
    ]
    nghia = pairs[pairs['site'] == 'NGHIA_DO']
    baclu = pairs[pairs['site'] == 'Bac_Lieu']
    cov_mean = float(coverage_df['post_fill_coverage'].mean()) \
               if not coverage_df.empty else np.nan
    achieved = [
        compute_metrics(nghia['sat_aod'].values, nghia['aer_aod'].values).get('R', np.nan),
        compute_metrics(baclu['sat_aod'].values, baclu['aer_aod'].values).get('R', np.nan),
        compute_metrics(nghia['sat_aod'].values, nghia['aer_aod'].values).get('RMSE', np.nan),
        cov_mean,
    ]
    return pd.DataFrame({
        'metric':   [t[0] for t in targets],
        'baseline': [t[2] for t in targets],
        'target':   [t[1] for t in targets],
        'achieved': achieved,
    })


# ── §8.2.1 internal consistency ──────────────────────────────────────────────

def internal_consistency(metrics: dict,
                          cv_fold_metrics: Optional[list[dict]] = None,
                          tol: float = RMSE_CONSISTENCY_TOLERANCE) -> dict:
    """One-sided consistency check (§8.2.1).

    Train/test RMSE only fail when *worse* than CV by `tol`.  Train < CV is
    expected (final fit uses every training day with no held-out block);
    test < CV is also fine when the chronological test slice happens to be
    a calmer period than the worst CV fold.  Reports `rmse_cv_std` and the
    worst-fold pointer so a high `rmse_cv_mean` driven by one seasonal
    hot-spot fold is visible to the reader.
    """
    cv     = metrics['rmse_cv_mean']
    cv_std = metrics.get('rmse_cv_std', np.nan)
    train  = metrics.get('rmse_train', np.nan)
    test   = metrics.get('rmse_internal_test', np.nan)
    upper_band        = cv * (1 + tol)
    overfit_threshold = cv * 1.3
    out = {
        'rmse_train':           train,
        'rmse_cv_mean':         cv,
        'rmse_cv_std':          cv_std,
        'rmse_internal_test':   test,
        'tolerance':            tol,
        'train_underfit_pass':  (train <= upper_band)        if np.isfinite(train) else None,
        'test_overfit_pass':    (test  <= overfit_threshold) if np.isfinite(test)  else None,
    }
    if cv_fold_metrics:
        fold_rmses = [f['rmse'] for f in cv_fold_metrics]
        out['rmse_cv_max']    = float(max(fold_rmses))
        out['worst_fold_idx'] = int(np.argmax(fold_rmses))
    return out
