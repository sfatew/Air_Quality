"""Step A4: region/season-aware bias correction via CDF quantile mapping.

Design (Ahn et al. 2021, adapted for Vietnam):

  1. For each (sensor, region, season) stratum, fit a transfer function from
     collocated (satellite, AERONET) pairs using the following decision tree
     (from validate_collocate_coverage.ipynb):

     Compute (N, R, linear_slope, KS_matched_vs_full, range_ratio,
              decile_coverage) per stratum, then:

       if R < 0.30 or slope ∉ [0.30, 3.00] or N < CDF_MIN_PAIRS_NONE (30):
           → correction_type='none', down-weight in fusion
       elif N < CDF_MIN_PAIRS (100) or decile_coverage < 7:
           → 'linear'; if range_ratio < 0.70, also clip above matched 90th pct
       elif KS > 0.20:
           → 'cdf_clipped' above matched 90th pct
       elif KS > 0.10 or range_ratio < 0.85:
           → 'cdf_clipped' above matched 95th pct
       else:
           → 'cdf' (200-quantile full CDF, no clipping)

     KS, range_ratio, and decile_coverage compare the matched AERONET subsample
     against the full AERONET record at the site (all obs in the training period,
     not just AERONET-coincident pairs).  When full_aer_aod is not supplied to
     fit(), these diagnostics are NaN/0 and the tree defaults to 'cdf' for
     qualifying strata.

  2. Spatial extension (no IDW):
     North cells (lat ≥ NORTH_CENTRAL_LAT) → Nghia-Do-trained correction.
     South cells (lat < CENTRAL_SOUTH_LAT) → Bac-Lieu-trained correction.
     Central cells → pass through; central Himawari bias is handled by the
     §7.4.2 LEO–Himawari spatial-offset map.

  3. Saved as pickle files in BIASC_DIR so training and production are decoupled.
"""

from __future__ import annotations
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
from scipy.interpolate import PchipInterpolator

try:
    from tqdm import tqdm as _tqdm_cls
    _HAS_TQDM = True
except ImportError:
    _tqdm_cls = None  # type: ignore[assignment]
    _HAS_TQDM = False


def _make_bar(iterable, **kwargs):
    if _HAS_TQDM and _tqdm_cls is not None:
        return _tqdm_cls(iterable, **kwargs)
    return None

from config import (
    DRY_MONTHS, WET_MONTHS,
    CDF_N_QUANTILES, CDF_MIN_PAIRS, CDF_MIN_PAIRS_NONE,
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
    BIASC_DIR,
    LATS, LONS, NLAT, NLON,
    LEO_HIMAWARI_OFFSET_FILE, LEO_HIMAWARI_MIN_PAIRS, LEO_HIMAWARI_SMOOTH_SIGMA,
    MERGED_DIR, AERONET_SITES,
)

_FIT_MIN_PEARSON = 0.30
_FIT_MIN_SLOPE   = 0.30
_FIT_MAX_SLOPE   = 3.00
_CV_FOLDS        = 5


# ── Stratum helpers ───────────────────────────────────────────────────────────

def get_stratum(month: int, lat: float) -> tuple[str, str]:
    """Return (region, season) stratum for a given month and latitude."""
    season = 'dry' if month in DRY_MONTHS else 'wet'
    region = (
        'north' if lat >= NORTH_CENTRAL_LAT else
        'south' if lat < CENTRAL_SOUTH_LAT  else
        'central'
    )
    return region, season


# ── Full AERONET loader (used by train_all_corrections) ───────────────────────

def _load_full_aeronet_distributions(
    train_start,
    train_end,
) -> dict[tuple[str, str], np.ndarray]:
    """Return full AERONET AOD distributions per (region, season).

    Loads every AERONET observation in [train_start, train_end] at each site,
    groups by (region, season), and returns the `aod_550` arrays.  These are
    used to compute the KS statistic and range_ratio that drive the decision
    tree — we check whether the collocated AERONET subsample (those timestamps
    that happened to coincide with a valid satellite retrieval) is representative
    of the full AERONET record.  The comparison is entirely in AERONET space;
    satellite values are not used here.
    """
    import pandas as pd
    from aeronet import load_aeronet

    full: dict[tuple[str, str], np.ndarray] = {}
    t_start = pd.Timestamp(train_start)
    t_end   = pd.Timestamp(train_end) + pd.Timedelta(days=1)

    for site, meta in AERONET_SITES.items():
        region = meta['region']
        try:
            df = load_aeronet(site)
        except Exception as exc:
            print(f'[bias_correction] warning: could not load AERONET {site}: {exc}')
            continue

        df = df[(df['datetime'] >= t_start) & (df['datetime'] <= t_end)]
        df = df[df['aod_550'].notna() & (df['aod_550'] >= 0)]

        for season in ('dry', 'wet'):
            sea_months = DRY_MONTHS if season == 'dry' else WET_MONTHS
            vals = df[df['datetime'].dt.month.isin(sea_months)]['aod_550'].values.astype(float)
            if len(vals) >= 10:
                full[(region, season)] = np.asarray(vals, dtype=float)

    return full


# ── CDF correction fitting ────────────────────────────────────────────────────

class CDFCorrection:
    """Quantile-mapping transfer function for one (sensor, region, season) stratum.

    Attributes
    ----------
    sensor, region, season : stratum identifiers
    n_pairs                : number of training pairs
    correction_type        : 'cdf' | 'cdf_clipped' | 'linear' | 'none'
    clip_above             : AOD threshold above which apply() passes through
                             (no correction); None means no clipping
    ks_stat                : KS statistic, matched sample vs full distribution
    range_ratio            : p99 of matched / p99 of full satellite distribution
    decile_coverage        : how many of the full distribution's 10 decile bins
                             contain at least one matched pair (0–10)
    n_quantiles_used       : number of quantile points used (200 for CDF, 0 for linear)
    notes                  : human-readable annotation of the fit decision
    rmse_before, rmse_after: RMSE vs AERONET before and after correction
    rmse_after_cv          : k-fold out-of-sample RMSE (used for fusion weights)
    """

    def __init__(self, sensor: str, region: str, season: str):
        self.sensor  = sensor
        self.region  = region
        self.season  = season
        self.n_pairs = 0
        self.correction_type: str = 'none'
        self.rmse_before:  float = np.nan
        self.rmse_after:   float = np.nan
        self.rmse_after_cv: float = np.nan
        self.pearson_r:    float = np.nan
        self.reject_reason: str  = ''
        self._interp          = None
        self._lin_slope:     float = 1.0
        self._lin_intercept: float = 0.0
        # Distribution-diagnostic attributes (decision tree inputs)
        self.ks_stat:         float         = np.nan
        self.range_ratio:     float         = np.nan
        self.decile_coverage: int           = 0
        self.n_quantiles_used: int          = 0
        self.clip_above:      Optional[float] = None
        self.notes:           str           = ''

    # ── Private helpers ────────────────────────────────────────────────────────

    def _compute_diagnostics(
        self,
        aer: np.ndarray,
        full_aer_aod: Optional[np.ndarray],
    ) -> None:
        """Compute KS, range_ratio, decile_coverage — all in AERONET space.

        Parameters
        ----------
        aer          : AERONET AOD from the collocated pairs (the matched subset)
        full_aer_aod : all AERONET obs at the site/season in the training period

        The question answered: does the collocated AERONET subsample (those
        timestamps coinciding with a valid satellite retrieval) represent the
        full AERONET distribution?  A large KS or low range_ratio means the
        satellite systematically missed high-AOD events, so the CDF transfer
        function is trained on a clean-biased sample and will distort the tail.

        When full_aer_aod is None the metrics are left as NaN and the decision
        tree defaults to the full-CDF path for qualifying strata.
        """
        from scipy.stats import ks_2samp

        if full_aer_aod is not None:
            full_clean = full_aer_aod[np.isfinite(full_aer_aod)
                                      & (full_aer_aod.astype(float) >= 0)]
        else:
            full_clean = None

        ref = full_clean if (full_clean is not None and len(full_clean) >= 10) else aer

        # decile_coverage: how many of the full-AERONET decile bins contain ≥1 pair
        if len(ref) >= 10 and len(aer) >= 1:
            edges = np.percentile(ref, np.linspace(0, 100, 11))
            edges = np.unique(edges)
            if len(edges) >= 2:
                counts, _ = np.histogram(aer, bins=edges)
                self.decile_coverage = int(np.sum(counts > 0))
            else:
                self.decile_coverage = 1
        else:
            self.decile_coverage = min(10, len(aer))

        if full_clean is not None and len(full_clean) >= 5 and len(aer) >= 5:
            try:
                ks, _ = ks_2samp(aer, full_clean)
                self.ks_stat = float(ks)
            except Exception:
                self.ks_stat = np.nan

            # range_ratio uses p95 (matches notebook acceptance check)
            p95_matched      = float(np.percentile(aer, 95))
            p95_full         = float(np.percentile(full_clean, 95))
            self.range_ratio = float(p95_matched / p95_full) if p95_full > 0 else np.nan
        else:
            self.ks_stat     = np.nan
            self.range_ratio = np.nan

    def _fit_cdf(self, sat: np.ndarray, aer: np.ndarray) -> PchipInterpolator:
        """Build PCHIP CDF transfer function from paired (sat, aer) arrays."""
        q     = np.linspace(0.005, 0.995, CDF_N_QUANTILES)
        sat_q = np.quantile(sat, q)
        aer_q = np.quantile(aer, q)
        sat_all = np.concatenate([[0.0], sat_q])
        aer_all = np.concatenate([[min(0.0, float(aer_q[0]))], aer_q])
        order   = np.argsort(sat_all)
        sat_all = sat_all[order]
        aer_all = aer_all[order]
        _, unique = np.unique(sat_all, return_index=True)
        return PchipInterpolator(sat_all[unique], aer_all[unique], extrapolate=True)

    def _fit_pair(self, sat: np.ndarray, aer: np.ndarray,
                  force_type: Optional[str] = None):
        """Fit one transfer function; used by fit() and CV folds.

        force_type='linear' → linear regardless of N.
        force_type=anything_else (e.g. 'cdf') → quantile_map regardless of N.
        force_type=None → decide by N vs CDF_MIN_PAIRS.

        Returns (callable, fit_type_str, params_dict).
        Raises ValueError on sanity failures so callers can fall back.
        """
        from scipy.stats import pearsonr
        if len(sat) < 5:
            raise ValueError('fewer than 5 pairs')

        try:
            r, _ = pearsonr(sat, aer)
        except Exception:
            r = float('nan')
        if not np.isfinite(r) or r < _FIT_MIN_PEARSON:
            raise ValueError(f'pearson R={r:.3f} below {_FIT_MIN_PEARSON}')

        use_linear = (force_type == 'linear') if force_type is not None \
            else (len(sat) < CDF_MIN_PAIRS)

        if use_linear:
            slope, intercept = np.polyfit(sat, aer, 1)
            slope     = float(slope)
            intercept = float(intercept)
            if not (_FIT_MIN_SLOPE <= slope <= _FIT_MAX_SLOPE):
                raise ValueError(f'linear slope {slope:.3f} outside '
                                  f'[{_FIT_MIN_SLOPE}, {_FIT_MAX_SLOPE}]')
            return (
                (lambda x: np.clip(slope * x + intercept, 0, None)),
                'linear',
                {'slope': slope, 'intercept': intercept, 'pearson_r': float(r)},
            )

        interp = self._fit_cdf(sat, aer)
        return (
            (lambda x, _f=interp: np.clip(_f(x), 0, None)),
            'quantile_map',
            {'interp': interp, 'pearson_r': float(r)},
        )

    def _compute_cv_rmse(self, sat: np.ndarray, aer: np.ndarray) -> None:
        """Set rmse_after_cv via k-fold cross-validation on the deployed model type."""
        k = min(_CV_FOLDS, self.n_pairs)
        if k < 2:
            self.rmse_after_cv = self.rmse_after
            return
        rng      = np.random.default_rng(seed=42)
        perm     = rng.permutation(self.n_pairs)
        cv_resid = []
        force    = 'linear' if self.correction_type == 'linear' else 'cdf'
        for fold in range(k):
            test_idx  = perm[fold::k]
            train_idx = np.setdiff1d(perm, test_idx, assume_unique=True)
            if len(train_idx) < 5:
                continue
            try:
                fn_tr, _, _ = self._fit_pair(sat[train_idx], aer[train_idx],
                                              force_type=force)
            except ValueError:
                cv_resid.append(sat[test_idx] - aer[test_idx])
                continue
            cv_resid.append(fn_tr(sat[test_idx]) - aer[test_idx])
        if cv_resid:
            resid = np.concatenate(cv_resid)
            self.rmse_after_cv = float(np.sqrt(np.mean(resid ** 2)))
        else:
            self.rmse_after_cv = self.rmse_after

    # ── Main fit ───────────────────────────────────────────────────────────────

    def fit(
        self,
        sat_aod: np.ndarray,
        aer_aod: np.ndarray,
        full_aer_aod: Optional[np.ndarray] = None,
    ) -> 'CDFCorrection':
        """Fit the transfer function using the decision tree from the thesis plan.

        Parameters
        ----------
        sat_aod, aer_aod  : matched satellite / AERONET pair arrays
        full_aer_aod      : all AERONET observations at the site/season in the
                            training period (not just those with satellite matches);
                            used to compute KS and range_ratio.  Pass None to skip
                            those diagnostics (decision tree defaults to 'cdf').
        """
        from scipy.stats import pearsonr

        mask = (np.isfinite(sat_aod) & np.isfinite(aer_aod)
                & (sat_aod.astype(float) >= 0) & (aer_aod.astype(float) >= 0))
        sat, aer     = sat_aod[mask], aer_aod[mask]
        self.n_pairs = len(sat)

        if self.n_pairs < 5:
            self.correction_type = 'none'
            self.reject_reason   = f'N={self.n_pairs} < 5'
            return self

        self.rmse_before = float(np.sqrt(np.mean((sat - aer) ** 2)))

        # ── Preliminary R and slope (required by every branch gate) ───────────
        try:
            r, _ = pearsonr(sat, aer)
        except Exception:
            r = float('nan')
        self.pearson_r = float(r) if np.isfinite(r) else np.nan

        try:
            slope_val, intercept_val = np.polyfit(sat, aer, 1)
            slope_val     = float(slope_val)
            intercept_val = float(intercept_val)
        except Exception:
            slope_val     = float('nan')
            intercept_val = 0.0

        # ── Distribution diagnostics (KS, range_ratio, decile_coverage) ───────
        self._compute_diagnostics(aer, full_aer_aod)

        # ── Branch 1: hard reject ─────────────────────────────────────────────
        reasons: list[str] = []
        if not np.isfinite(r) or r < _FIT_MIN_PEARSON:
            reasons.append(f'R={r:.3f}<{_FIT_MIN_PEARSON}')
        if (not np.isfinite(slope_val)
                or not (_FIT_MIN_SLOPE <= slope_val <= _FIT_MAX_SLOPE)):
            reasons.append(f'slope={slope_val:.3f}∉[{_FIT_MIN_SLOPE},{_FIT_MAX_SLOPE}]')
        if self.n_pairs < CDF_MIN_PAIRS_NONE:
            reasons.append(f'N={self.n_pairs}<{CDF_MIN_PAIRS_NONE}')
        if reasons:
            self.correction_type = 'none'
            self.reject_reason   = '; '.join(reasons)
            return self

        # ── Branch 2: linear (N too small or decile coverage too sparse) ──────
        if self.n_pairs < CDF_MIN_PAIRS or self.decile_coverage < 7:
            # Slope already validated in Branch 1 gate above.
            self.correction_type  = 'linear'
            self._lin_slope       = slope_val
            self._lin_intercept   = intercept_val
            self.n_quantiles_used = 0

            rr = self.range_ratio
            if not np.isnan(rr) and rr < 0.70:
                self.clip_above = float(np.percentile(sat, 90))
                self.notes = (f'linear+clip@p90={self.clip_above:.3f} '
                              f'(range_ratio={rr:.3f})')
            else:
                self.clip_above = None
                self.notes = (f'linear (N={self.n_pairs}, '
                              f'decile_cov={self.decile_coverage})')

            corrected = np.clip(slope_val * sat + intercept_val, 0, None)
            self.rmse_after = float(np.sqrt(np.mean((corrected - aer) ** 2)))
            self._compute_cv_rmse(sat, aer)
            return self

        # ── Branches 3–5: CDF (N ≥ 100 and decile_coverage ≥ 7) ─────────────
        ks = self.ks_stat
        rr = self.range_ratio

        if not np.isnan(ks) and ks > 0.20:
            self.clip_above = float(np.percentile(sat, 90))
            ctype      = 'cdf_clipped'
            self.notes = (f'cdf_clipped@p90={self.clip_above:.3f} '
                          f'(KS={ks:.3f}>0.20)')
        elif (not np.isnan(ks) and ks > 0.10) or (not np.isnan(rr) and rr < 0.85):
            self.clip_above = float(np.percentile(sat, 95))
            ctype      = 'cdf_clipped'
            self.notes = (f'cdf_clipped@p95={self.clip_above:.3f} '
                          f'(KS={ks:.3f}, range_ratio={rr:.3f})')
        else:
            self.clip_above = None
            ctype      = 'cdf'
            ks_s  = f'KS={ks:.3f}'  if not np.isnan(ks) else 'KS=n/a'
            rr_s  = f'rr={rr:.3f}'  if not np.isnan(rr) else 'rr=n/a'
            self.notes = f'full_cdf ({ks_s}, {rr_s})'

        try:
            fn, _, params = self._fit_pair(sat, aer, force_type='cdf')
        except ValueError as exc:
            self.correction_type = 'none'
            self.reject_reason   = str(exc)
            return self

        self.correction_type  = ctype
        self._interp          = params['interp']
        self.n_quantiles_used = CDF_N_QUANTILES

        corrected = fn(sat)
        self.rmse_after = float(np.sqrt(np.mean((corrected - aer) ** 2)))
        self._compute_cv_rmse(sat, aer)
        return self

    # ── Inference ─────────────────────────────────────────────────────────────

    def apply(self, aod: np.ndarray) -> np.ndarray:
        """Transform satellite AOD to bias-corrected AOD.

        NaN inputs pass through as NaN; outputs are clipped to [0, ∞).
        For 'cdf_clipped' and clipped 'linear': values above clip_above pass
        through unchanged (the correction is unreliable in that tail).
        Backward-compatible: old pickles without clip_above behave as before.
        """
        if self.correction_type == 'none':
            return aod.copy()

        out   = np.full_like(aod, np.nan, dtype=np.float32)
        valid = np.isfinite(aod) & (aod >= 0)

        if self.correction_type == 'linear':
            out[valid] = self._lin_slope * aod[valid] + self._lin_intercept
        else:
            # 'cdf', 'cdf_clipped', or legacy 'quantile_map'
            out[valid] = self._interp(aod[valid])

        out = np.clip(out, 0, None)

        # Pass-through above clip threshold (backward-compat via getattr)
        clip_above = getattr(self, 'clip_above', None)
        if clip_above is not None:
            above = valid & (aod > clip_above)
            if np.any(above):
                out[above] = aod[above].astype(np.float32)

        return out

    # ── Persistence ───────────────────────────────────────────────────────────

    def save(self, directory: Path | str) -> Path:
        """Pickle to directory as {sensor}_{region}_{season}.pkl."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        fpath = directory / f'{self.sensor}_{self.region}_{self.season}.pkl'
        with open(fpath, 'wb') as f:
            pickle.dump(self, f)
        return fpath

    @classmethod
    def load(cls, sensor: str, region: str, season: str,
             directory: Path | str) -> Optional['CDFCorrection']:
        fpath = Path(directory) / f'{sensor}_{region}_{season}.pkl'
        if not fpath.exists():
            return None
        with open(fpath, 'rb') as f:
            return pickle.load(f)

    def __repr__(self) -> str:
        clip_s = (f' clip@{self.clip_above:.3f}'
                  if getattr(self, 'clip_above', None) is not None else '')
        ks_s   = (f' KS={self.ks_stat:.3f}'
                  if not np.isnan(getattr(self, 'ks_stat', np.nan)) else '')
        return (f'CDFCorrection({self.sensor}, {self.region}, {self.season}) '
                f'type={self.correction_type}{clip_s}  N={self.n_pairs}  '
                f'RMSE {self.rmse_before:.3f}→{self.rmse_after:.3f}{ks_s}')


# ── Per-region spatial application ───────────────────────────────────────────

def apply_correction_grid(
    aod_grid: np.ndarray,
    sensor: str,
    month: int,
    lat_2d: np.ndarray,
    lon_2d: np.ndarray,
    corrections: dict[tuple[str, str, str], CDFCorrection],
) -> np.ndarray:
    """Apply per-region bias correction to a 2-D AOD grid.

    North cells (lat ≥ NORTH_CENTRAL_LAT) → Nghia-Do CDF.
    South cells (lat <  CENTRAL_SOUTH_LAT) → Bac-Lieu CDF.
    Central cells → pass through (no AERONET anchor; handled by §7.4.2 offset).
    correction_type='none' strata are treated as missing (pass-through).
    """
    del lon_2d  # unused

    season = 'dry' if month in DRY_MONTHS else 'wet'
    corr_N = corrections.get((sensor, 'north', season))
    corr_S = corrections.get((sensor, 'south', season))
    if corr_N is not None and corr_N.correction_type == 'none':
        corr_N = None
    if corr_S is not None and corr_S.correction_type == 'none':
        corr_S = None

    out = aod_grid.astype(np.float32, copy=True)
    if corr_N is None and corr_S is None:
        return out

    region_code = np.where(
        lat_2d >= NORTH_CENTRAL_LAT, 2,
        np.where(lat_2d < CENTRAL_SOUTH_LAT, 0, 1)
    )
    valid = np.isfinite(aod_grid)

    if corr_N is not None:
        mask_n = valid & (region_code == 2)
        if np.any(mask_n):
            out[mask_n] = corr_N.apply(aod_grid[mask_n]).astype(np.float32)

    if corr_S is not None:
        mask_s = valid & (region_code == 0)
        if np.any(mask_s):
            out[mask_s] = corr_S.apply(aod_grid[mask_s]).astype(np.float32)

    return out


# ── Convenience: load all corrections for a sensor set ───────────────────────

def load_all_corrections(
    sensors: list[str],
    directory: Path | str = BIASC_DIR,
) -> dict[tuple[str, str, str], CDFCorrection]:
    """Load all saved CDFCorrection objects for the given sensor list."""
    directory   = Path(directory)
    corrections = {}
    for sensor in sensors:
        for region in ('north', 'south'):
            for season in ('dry', 'wet'):
                c = CDFCorrection.load(sensor, region, season, directory)
                if c is not None:
                    corrections[(sensor, region, season)] = c
    return corrections


# ── Training driver (called from run_collocate.py) ────────────────────────────

def train_all_corrections(
    collocated_csv_dir: Path | str,
    sensors: list[str],
    output_dir: Path | str = BIASC_DIR,
) -> dict[tuple[str, str, str], CDFCorrection]:
    """Train and save CDF corrections from collocated CSV files.

    Expects one CSV per (sensor, site) in collocated_csv_dir named
    {sensor}_{site}.csv with columns:
        satellite_aod, aeronet_aod, sensor, region, season, month
    """
    import pandas as pd

    collocated_csv_dir = Path(collocated_csv_dir)
    output_dir         = Path(output_dir)

    # ── Load collocated CSVs ──────────────────────────────────────────────────
    csv_files = sorted(collocated_csv_dir.glob('*.csv'))
    print(f'[bias_correction] Loading {len(csv_files)} collocated CSV file(s) …')

    all_frames: list = []
    csv_bar = _make_bar(csv_files, desc='  Loading CSVs', unit='file',
                        ncols=72, leave=False)
    for fpath in (csv_bar if csv_bar is not None else csv_files):
        try:
            all_frames.append(pd.read_csv(fpath))
        except Exception as exc:
            print(f'[bias_correction] warning: skipping {fpath.name}: {exc}')
    if csv_bar is not None:
        csv_bar.close()

    if not all_frames:
        print('[bias_correction] No collocated CSVs found; skipping training.')
        return {}

    df_all = pd.concat(all_frames, ignore_index=True)
    print(f'  Total pairs: {len(df_all):,}  '
          f'(sensors: {sorted(df_all["sensor"].unique())})')

    # ── Load full AERONET distributions (for KS / range_ratio / decile_coverage)
    train_start = df_all['date'].min()
    train_end   = df_all['date'].max()
    full_aer_distributions = _load_full_aeronet_distributions(train_start, train_end)
    print(f'[bias_correction] Full AERONET distributions loaded for '
          f'{len(full_aer_distributions)} (region, season) strata.')

    # ── Fit one CDFCorrection per (sensor, region, season) ───────────────────
    strata = [
        (sensor, region, season)
        for sensor in sensors
        for region in ('north', 'south')
        for season in ('dry', 'wet')
    ]

    corrections: dict[tuple[str, str, str], CDFCorrection] = {}
    fit_bar = _make_bar(strata, desc='Training strata', unit='stratum', ncols=72)

    for sensor, region, season in (fit_bar if fit_bar is not None else strata):
        if fit_bar is not None:
            fit_bar.set_description(f'  {sensor[:12]}/{region[:5]}/{season[:3]}')

        mask  = ((df_all['sensor'] == sensor)
                 & (df_all['region'] == region)
                 & (df_all['season'] == season))
        df_st = df_all[mask]

        full_aer_dist = full_aer_distributions.get((region, season))

        c = CDFCorrection(sensor, region, season)
        c.fit(df_st['satellite_aod'].values, df_st['aeronet_aod'].values,
              full_aer_aod=full_aer_dist)
        c.save(output_dir)
        corrections[(sensor, region, season)] = c

        msg = f'  trained: {c}'
        if fit_bar is not None:
            fit_bar.write(msg)
        else:
            print(msg)

    if fit_bar is not None:
        fit_bar.close()

    # ── Summary table ─────────────────────────────────────────────────────────
    if corrections:
        _print_training_summary(corrections)

    return corrections


def _print_training_summary(
    corrections: dict[tuple[str, str, str], CDFCorrection],
) -> None:
    """Print a formatted summary of all trained strata."""
    def _f(x: float, w: int = 6) -> str:
        return f'{x:.4f}' if np.isfinite(x) else ' ' * (w - 3) + 'N/A'

    col_w = 15
    print(f'\n[bias_correction] Summary — {len(corrections)} strata trained:')
    hdr = (f"  {'Sensor':<{col_w}} {'Reg':<8} {'Sea':<4} "
           f"{'Type':<13} {'N':>6}  {'R':>5}  "
           f"{'KS':>5}  {'RR':>5}  {'DC':>2}  {'Clip':>6}  "
           f"{'RMSE_b':>7}  {'RMSE_a':>7}  {'RMSE_cv':>7}  Note")
    print(hdr)
    print('  ' + '─' * (len(hdr) - 2))

    for (s, reg, sea), c in sorted(corrections.items()):
        r_s    = f'{c.pearson_r:.2f}' if np.isfinite(c.pearson_r) else '  N/A'
        ks_s   = f'{c.ks_stat:.3f}'   if np.isfinite(getattr(c, 'ks_stat', np.nan)) else '  N/A'
        rr_s   = f'{c.range_ratio:.3f}' if np.isfinite(getattr(c, 'range_ratio', np.nan)) else '  N/A'
        dc_s   = str(getattr(c, 'decile_coverage', '-'))
        clip_s = (f'{c.clip_above:.3f}' if getattr(c, 'clip_above', None) is not None
                  else '  none')
        note   = (c.reject_reason if c.correction_type == 'none'
                  else getattr(c, 'notes', ''))
        print(f'  {s:<{col_w}} {reg:<8} {sea:<4} '
              f'{c.correction_type:<13} {c.n_pairs:>6}  {r_s:>5}  '
              f'{ks_s:>5}  {rr_s:>5}  {dc_s:>2}  {clip_s:>6}  '
              f'{_f(c.rmse_before)}  {_f(c.rmse_after)}  {_f(c.rmse_after_cv)}  {note}')


# ── Step A4b: LEO–Himawari spatial-offset correction (thesis §7.4.2) ─────────

def _scan_merged_files_for_offset(start_d, end_d):
    """Yield (Path, date) for merged NetCDFs in [start_d, end_d]."""
    import glob
    from datetime import timedelta
    d = start_d
    while d <= end_d:
        pattern = str(
            MERGED_DIR / d.strftime('%Y') / d.strftime('%m') /
            d.strftime('%d') / 'merged_*.nc'
        )
        for fpath in sorted(glob.glob(pattern)):
            yield Path(fpath), d
        d += timedelta(days=1)


def build_leo_himawari_offset(
    start_d,
    end_d,
    output_path: Path | str = LEO_HIMAWARI_OFFSET_FILE,
    min_pairs: int = LEO_HIMAWARI_MIN_PAIRS,
    smooth_sigma: float = LEO_HIMAWARI_SMOOTH_SIGMA,
) -> None:
    """Compute and persist the Himawari↔LEO spatial offset map (thesis §7.4.2).

    For each grid cell, accumulates the mean residual:

        offset[lat, lon, season] = mean( Himawari_corrected
                                         − ICW_mean(LEO_sensors_corrected) )

    over the training period, then Gaussian-smooths and writes to NetCDF.

    Prerequisite: a Stage A run exists for [start_d, end_d] so MERGED_DIR
    contains AOD_himawari and at least one LEO sensor per slot.
    """
    import netCDF4 as nc
    from scipy.ndimage import gaussian_filter
    from config import SENSOR_RMSE_FLOOR, MODIS_SOUTH_WEIGHT_FACTOR
    from fusion import load_rmse

    levels   = ('himawari',)
    seasons  = ('dry', 'wet')
    leo_vars = ('AOD_modis_maiac', 'AOD_viirs_snpp', 'AOD_viirs_noaa20')

    rmse_dict   = load_rmse()
    lat_2d, _   = np.meshgrid(LATS, LONS, indexing='ij')
    region_code = np.where(lat_2d >= NORTH_CENTRAL_LAT, 2,
                           np.where(lat_2d < CENTRAL_SOUTH_LAT, 0, 1)).astype(np.int8)
    _reg_codes  = ((0, 'south'), (1, 'central'), (2, 'north'))

    def _weight_grid(sensor: str, season: str) -> np.ndarray:
        w = np.zeros((NLAT, NLON), dtype=np.float64)
        for code, reg in _reg_codes:
            rmse_val = rmse_dict.get(
                (sensor, reg, season),
                rmse_dict.get((sensor, reg),
                              rmse_dict.get((sensor, 'north'), np.nan)),
            )
            if not np.isfinite(rmse_val):
                continue
            rmse_val = max(float(rmse_val), SENSOR_RMSE_FLOOR)
            w[region_code == code] = 1.0 / rmse_val ** 2
        if sensor == 'modis_maiac':
            w[region_code == 0] *= MODIS_SOUTH_WEIGHT_FACTOR
        return w

    weight_grids: dict[tuple[str, str], np.ndarray] = {
        (v, sea): _weight_grid(v.replace('AOD_', ''), sea)
        for v in leo_vars for sea in seasons
    }

    shape  = (NLAT, NLON)
    sums   = {(lv, sea): np.zeros(shape, dtype=np.float64) for lv in levels for sea in seasons}
    counts = {(lv, sea): np.zeros(shape, dtype=np.int32)   for lv in levels for sea in seasons}

    files = list(_scan_merged_files_for_offset(start_d, end_d))
    print(f'[leo_himawari_offset] scanning {len(files)} merged files '
          f'in {start_d} → {end_d}')

    bar = _make_bar(files, desc='LEO–Himawari scan', unit='slot', ncols=80)
    for fpath, day in (bar if bar is not None else files):
        season = 'dry' if day.month in DRY_MONTHS else 'wet'
        try:
            with nc.Dataset(str(fpath)) as ds:
                num     = np.zeros(shape, dtype=np.float64)
                denom   = np.zeros(shape, dtype=np.float64)
                any_leo = False
                for v in leo_vars:
                    if v not in ds.variables:
                        continue
                    any_leo = True
                    a = np.ma.filled(ds.variables[v][:].astype(np.float32), np.nan)
                    finite = np.isfinite(a)
                    if not np.any(finite):
                        continue
                    wv = np.where(finite, weight_grids[(v, season)], 0.0)
                    num   += wv * np.where(finite, a, 0.0)
                    denom += wv
                if not any_leo:
                    continue
                with np.errstate(invalid='ignore', divide='ignore'):
                    leo_mean = np.where(denom > 0, num / denom, np.nan)

                for lv in levels:
                    var = 'AOD_himawari' if lv == 'himawari' else f'AOD_himawari_{lv}'
                    if var not in ds.variables:
                        continue
                    hi    = np.ma.filled(ds.variables[var][:].astype(np.float32), np.nan)
                    valid = np.isfinite(hi) & np.isfinite(leo_mean)
                    if not np.any(valid):
                        continue
                    key = (lv, season)
                    sums[key][valid]   += (hi[valid] - leo_mean[valid]).astype(np.float64)
                    counts[key][valid] += 1
        except Exception as exc:
            print(f'  skip {fpath.name}: {exc}')
    if bar is not None:
        bar.close()

    # ── Mean, mask-weighted smoothing, NaN → 0 outside coverage ─────────────
    offsets:     dict[tuple, np.ndarray] = {}
    pair_counts: dict[tuple, np.ndarray] = {}
    for key in sums:
        cnt = counts[key]
        ok  = cnt >= min_pairs
        with np.errstate(invalid='ignore', divide='ignore'):
            raw = np.where(ok, sums[key] / np.maximum(cnt, 1), 0.0)
        if smooth_sigma > 0 and np.any(ok):
            num_s   = gaussian_filter(raw * ok, sigma=smooth_sigma)
            denom_s = gaussian_filter(ok.astype(np.float64), sigma=smooth_sigma)
            with np.errstate(invalid='ignore', divide='ignore'):
                smooth = np.where(denom_s > 0.01, num_s / denom_s, 0.0)
            offsets[key] = smooth.astype(np.float32)
        else:
            offsets[key] = raw.astype(np.float32)
        pair_counts[key] = cnt.astype(np.int32)

    # ── Write NetCDF ──────────────────────────────────────────────────────────
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with nc.Dataset(str(output_path), 'w', format='NETCDF4') as ds:
        ds.title        = 'Himawari↔LEO spatial offset map (thesis §7.4.2)'
        ds.train_start  = start_d.isoformat()
        ds.train_end    = end_d.isoformat()
        ds.min_pairs    = int(min_pairs)
        ds.smooth_sigma = float(smooth_sigma)
        ds.createDimension('lat', NLAT)
        ds.createDimension('lon', NLON)
        v_lat     = ds.createVariable('lat', 'f4', ('lat',))
        v_lat[:]  = LATS.astype(np.float32)
        v_lon     = ds.createVariable('lon', 'f4', ('lon',))
        v_lon[:]  = LONS.astype(np.float32)

        for (lv, sea), arr in offsets.items():
            name = f'offset_{lv}_{sea}'
            v = ds.createVariable(name, 'f4', ('lat', 'lon'), zlib=True, complevel=4)
            v.long_name = f'Mean Himawari ({lv}) minus LEO_mean, {sea} season'
            v.units     = '1'
            v[:] = arr
        for (lv, sea), arr in pair_counts.items():
            name = f'n_pairs_{lv}_{sea}'
            v = ds.createVariable(name, 'i4', ('lat', 'lon'), zlib=True, complevel=4)
            v.long_name = f'N co-located slots, Himawari {lv} / LEO, {sea}'
            v[:] = arr

    print(f'[leo_himawari_offset] saved → {output_path}')
    for (lv, sea), arr in offsets.items():
        cnt = pair_counts[(lv, sea)]
        ok  = cnt >= min_pairs
        if np.any(ok):
            print(f'  {lv}/{sea}: n_cells_ok={int(ok.sum()):5d}  '
                  f'mean_offset={float(arr[ok].mean()):+.4f}  '
                  f'min={float(arr[ok].min()):+.4f}  '
                  f'max={float(arr[ok].max()):+.4f}')
        else:
            print(f'  {lv}/{sea}: no cells met min_pairs={min_pairs}')


def load_leo_himawari_offset(
    fpath: Path | str = LEO_HIMAWARI_OFFSET_FILE,
) -> Optional[dict[tuple[str, str], np.ndarray]]:
    """Return offsets dict keyed by (level, season).

    Legacy v3.1 files with 'l2'/'l3' levels are still readable.
    """
    import netCDF4 as nc
    fpath = Path(fpath)
    if not fpath.exists():
        return None
    out: dict[tuple[str, str], np.ndarray] = {}
    try:
        with nc.Dataset(str(fpath)) as ds:
            for lv in ('himawari', 'l2', 'l3'):
                for sea in ('dry', 'wet'):
                    name = f'offset_{lv}_{sea}'
                    if name in ds.variables:
                        out[(lv, sea)] = np.ma.filled(
                            ds.variables[name][:].astype(np.float32), 0.0)
    except Exception:
        return None
    return out if out else None


def apply_leo_himawari_offset(
    aod_grid: np.ndarray,
    sensor: str,
    month: int,
    offsets: dict[tuple[str, str], np.ndarray],
) -> np.ndarray:
    """Subtract the LEO-anchored spatial offset from the merged Himawari grid.

    Returns aod_grid unchanged if sensor is not 'himawari', if the offset
    entry is missing, or for NaN input pixels.
    """
    if sensor != 'himawari':
        return aod_grid
    sea = 'dry' if month in DRY_MONTHS else 'wet'
    off = offsets.get(('himawari', sea))
    if off is None:
        return aod_grid
    out   = aod_grid.astype(np.float32, copy=True)
    valid = np.isfinite(out)
    out[valid] = np.clip(out[valid] - off[valid], 0.0, None)
    return out
