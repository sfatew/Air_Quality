"""Step A4: region/season-aware bias correction via CDF quantile mapping + IDW.

Design (Ahn et al. 2021, adapted for Vietnam):

  1. For each (sensor, region, season) stratum, fit a CDF quantile-mapping
     transfer function from collocated (satellite, AERONET) pairs.

  2. The transfer function is a monotone cubic interpolation over 200 quantile
     points, equivalent to Ahn 2021's piecewise cubic over 0.1-wide AOD bins.
     Where N_pairs < CDF_MIN_PAIRS, fall back to a linear regression.

  3. Spatial extension (Ahn 2021 IDW):
     The two AERONET anchors (Nghia Do = north, Bac Lieu = south) provide
     bias-correction functions at two points. For any grid cell at (lat, lon),
     the corrected AOD is computed by IDW-blending the two site corrections:

         w_N = 1 / d(cell, NghiaDo)^2
         w_S = 1 / d(cell, BacLieu)^2
         AOD_corr(cell) = (w_N * f_N(AOD) + w_S * f_S(AOD)) / (w_N + w_S)

     For cells far from both anchors (central Vietnam), this gives a smooth
     interpolation; the plan acknowledges this is unconstrained by direct
     AERONET validation.

  4. Saved as pickle files in BIASC_DIR so the trained corrections can be
     reused without re-training.
"""

from __future__ import annotations
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
from scipy.interpolate import PchipInterpolator

try:
    import cupy as cp
    cp.array([0])  # triggers CUDA init — raises if no GPU device
    _xp = cp
except Exception:
    cp = None
    _xp = np

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
    AERONET_SITES, DRY_MONTHS, WET_MONTHS,
    CDF_N_QUANTILES, CDF_MIN_PAIRS,
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
    EARTH_RADIUS_KM, BIASC_DIR,
    LATS, LONS, NLAT, NLON,
    LEO_HIMAWARI_OFFSET_FILE, LEO_HIMAWARI_MIN_PAIRS, LEO_HIMAWARI_SMOOTH_SIGMA,
    MERGED_DIR,
)

# Quality gates for the linear/quantile fallback paths (Bug 3 fix):
# strata that fail these checks have correction_type='none', so the fusion
# falls back to the SENSOR_RMSE_PRIOR weights for that sensor/region/season.
_FIT_MIN_PEARSON   = 0.30   # minimum Pearson R for any correction to be trusted
_FIT_MIN_SLOPE     = 0.30   # linear slope sanity range
_FIT_MAX_SLOPE     = 3.00
_CV_FOLDS          = 5      # cross-validated RMSE evaluation folds


# ── Haversine distance ────────────────────────────────────────────────────────

def _haversine_km(lat1: float, lon1: float, lat2: np.ndarray,
                  lon2: np.ndarray) -> np.ndarray:
    """Vectorised great-circle distance (km)."""
    xp   = _xp
    lat2 = xp.asarray(lat2)
    lon2 = xp.asarray(lon2)
    cos_lat1 = float(np.cos(np.radians(lat1)))
    R    = EARTH_RADIUS_KM
    dlat = xp.radians(lat2 - lat1)
    dlon = xp.radians(lon2 - lon1)
    a = xp.sin(dlat / 2) ** 2 + cos_lat1 * xp.cos(xp.radians(lat2)) * xp.sin(dlon / 2) ** 2
    return R * 2 * xp.arctan2(xp.sqrt(a), xp.sqrt(1 - a))


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


# ── CDF correction fitting ────────────────────────────────────────────────────

class CDFCorrection:
    """Quantile-mapping transfer function for one (sensor, region, season) stratum.

    Fitting approach:
        • Compute N quantile points on the empirical CDFs of both satellite
          and AERONET paired observations (Ahn 2021 Eq. 1 / Fig. 2–3).
        • Fit a monotone PCHIP interpolant from satellite quantiles to AERONET
          quantiles: this is the per-stratum bias-correction function.
        • Boundary: extrapolated linearly beyond the training range.
        • Fallback: if N_pairs < CDF_MIN_PAIRS, use a simple linear fit.

    Attributes
    ----------
    sensor, region, season : stratum identifiers
    n_pairs                : number of training pairs
    correction_type        : 'quantile_map' or 'linear'
    rmse_before, rmse_after: RMSE vs AERONET before and after correction
    """

    def __init__(self, sensor: str, region: str, season: str):
        self.sensor = sensor
        self.region = region
        self.season = season
        self.n_pairs = 0
        self.correction_type: str = 'none'
        self.rmse_before: float = np.nan
        self.rmse_after:  float = np.nan
        self.rmse_after_cv: float = np.nan   # k-fold out-of-sample RMSE (Bug 2)
        self.pearson_r: float = np.nan
        self.reject_reason: str = ''
        self._interp = None          # PchipInterpolator or None
        self._lin_slope: float = 1.0
        self._lin_intercept: float = 0.0

    # ── Training ──────────────────────────────────────────────────────────────

    def _fit_pair(self, sat: np.ndarray, aer: np.ndarray,
                  force_type: Optional[str] = None):
        """Fit one transfer function on (sat, aer) and return a callable correction(x).

        Returns (callable, fit_type, params) where params is a dict of
        type-specific scalars used to populate self.* if this is the final fit.
        Raises ValueError if the data fails the sanity gates (Bug 3).

        ``force_type`` pins the model class regardless of N. CV folds pass the
        production model's type so rmse_after_cv estimates the *deployed*
        model's out-of-sample error, not a linear fallback.
        """
        from scipy.stats import pearsonr
        if len(sat) < 5:
            raise ValueError('fewer than 5 pairs')

        # Pearson R sanity gate
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
            slope = float(slope); intercept = float(intercept)
            if not (_FIT_MIN_SLOPE <= slope <= _FIT_MAX_SLOPE):
                raise ValueError(f'linear slope {slope:.3f} outside [{_FIT_MIN_SLOPE}, {_FIT_MAX_SLOPE}]')
            return (
                (lambda x: np.clip(slope * x + intercept, 0, None)),
                'linear',
                {'slope': slope, 'intercept': intercept, 'pearson_r': float(r)},
            )

        # Quantile-mapping (Ahn 2021)
        q     = np.linspace(0.005, 0.995, CDF_N_QUANTILES)
        sat_q = np.quantile(sat, q)
        aer_q = np.quantile(aer, q)
        sat_all = np.concatenate([[0.0], sat_q, [max(sat.max(), aer.max()) * 1.1]])
        aer_all = np.concatenate(
            [[min(0.0, float(aer_q[0]))], aer_q, [max(sat.max(), aer.max()) * 1.1]]
        )
        order   = np.argsort(sat_all)
        sat_all = sat_all[order]
        aer_all = aer_all[order]
        _, unique = np.unique(sat_all, return_index=True)
        sat_all = sat_all[unique]
        aer_all = aer_all[unique]
        interp = PchipInterpolator(sat_all, aer_all, extrapolate=True)
        return (
            (lambda x, _f=interp: np.clip(_f(x), 0, None)),
            'quantile_map',
            {'interp': interp, 'pearson_r': float(r)},
        )

    def fit(self, sat_aod: np.ndarray, aer_aod: np.ndarray) -> 'CDFCorrection':
        """Fit the transfer function and compute in-sample + CV RMSE.

        Bug 2 fix: report rmse_after_cv from k-fold cross-validation so the
        fusion weights are not driven by in-sample overfit.

        Bug 3 fix: reject fits whose Pearson R or linear slope are out of
        sanity range — correction_type stays 'none' and the fusion falls
        back to SENSOR_RMSE_PRIOR for that stratum.
        """
        mask = np.isfinite(sat_aod) & np.isfinite(aer_aod) & (sat_aod >= 0) & (aer_aod >= 0)
        sat, aer = sat_aod[mask], aer_aod[mask]
        self.n_pairs = len(sat)

        if self.n_pairs < 5:
            self.correction_type = 'none'
            self.reject_reason = f'N={self.n_pairs} < 5'
            return self

        self.rmse_before = float(np.sqrt(np.mean((sat - aer) ** 2)))

        try:
            fn, ftype, params = self._fit_pair(sat, aer)
        except ValueError as exc:
            self.correction_type = 'none'
            self.reject_reason = str(exc)
            return self

        self.correction_type = ftype
        self.pearson_r       = params.get('pearson_r', np.nan)
        if ftype == 'linear':
            self._lin_slope     = params['slope']
            self._lin_intercept = params['intercept']
        else:
            self._interp = params['interp']

        corrected = fn(sat)
        self.rmse_after = float(np.sqrt(np.mean((corrected - aer) ** 2)))

        # ── k-fold cross-validated RMSE (Bug 2) ──────────────────────────────
        # For very small samples, fall back to leave-one-out; for typical
        # strata, k=5 is enough.  Folds that fail the sanity gate fall back to
        # the global rmse_before so the stratum is not falsely promoted.
        k = min(_CV_FOLDS, self.n_pairs)
        if k < 2:
            self.rmse_after_cv = self.rmse_after
        else:
            rng       = np.random.default_rng(seed=42)
            perm      = rng.permutation(self.n_pairs)
            cv_resid  = []
            for fold in range(k):
                test_idx  = perm[fold::k]
                train_idx = np.setdiff1d(perm, test_idx, assume_unique=True)
                if len(train_idx) < 5:
                    continue
                try:
                    fn_tr, _, _ = self._fit_pair(
                        sat[train_idx], aer[train_idx],
                        force_type=self.correction_type,
                    )
                except ValueError:
                    # Bad fold → treat as raw residuals (no correction applied)
                    cv_resid.append(sat[test_idx] - aer[test_idx])
                    continue
                cv_resid.append(fn_tr(sat[test_idx]) - aer[test_idx])
            if cv_resid:
                resid = np.concatenate(cv_resid)
                self.rmse_after_cv = float(np.sqrt(np.mean(resid ** 2)))
            else:
                self.rmse_after_cv = self.rmse_after

        return self

    # ── Inference ─────────────────────────────────────────────────────────────

    def apply(self, aod: np.ndarray) -> np.ndarray:
        """Transform satellite AOD values to bias-corrected AOD.

        NaN inputs pass through as NaN; outputs are clipped to [0, ∞).
        """
        if self.correction_type == 'none':
            return aod.copy()

        out = np.full_like(aod, np.nan, dtype=np.float32)
        valid = np.isfinite(aod) & (aod >= 0)

        if self.correction_type == 'linear':
            out[valid] = self._lin_slope * aod[valid] + self._lin_intercept
        else:
            out[valid] = self._interp(aod[valid])

        out = np.clip(out, 0, None)
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
        return (f'CDFCorrection({self.sensor}, {self.region}, {self.season}) '
                f'type={self.correction_type}  N={self.n_pairs}  '
                f'RMSE {self.rmse_before:.3f} → {self.rmse_after:.3f}')


# ── IDW-blended spatial application ──────────────────────────────────────────

def _idw_weights(lat_2d: np.ndarray, lon_2d: np.ndarray,
                 anchor_lat: float, anchor_lon: float,
                 power: float = 2.0) -> np.ndarray:
    """Return IDW weight array (1 / d^power) for a single anchor site."""
    xp   = _xp
    dist = _haversine_km(anchor_lat, anchor_lon, lat_2d, lon_2d)
    dist = xp.maximum(dist, 0.001)
    return 1.0 / dist ** power


def apply_correction_grid(
    aod_grid: np.ndarray,
    sensor: str,
    month: int,
    lat_2d: np.ndarray,
    lon_2d: np.ndarray,
    corrections: dict[tuple[str, str, str], CDFCorrection],
) -> np.ndarray:
    """Apply IDW-blended bias correction to a 2-D AOD grid.

    Strategy:
      • Look up the north-stratum correction (anchored at Nghia Do)
        and the south-stratum correction (anchored at Bac Lieu).
      • For each grid cell compute IDW weights from both anchors.
      • The corrected AOD is the weight-normalised blend of the two
        site corrections evaluated at the cell's AOD value.
      • If only one anchor has a trained correction, that correction
        is applied uniformly across the grid.
      • If neither anchor has a correction, return aod_grid unchanged.

    Parameters
    ----------
    aod_grid : (NLAT, NLON) float32 array, NaN = no data
    sensor   : sensor key (e.g. 'himawari_l2')
    month    : calendar month (1–12) used to select dry/wet season
    lat_2d, lon_2d : (NLAT, NLON) coordinate arrays
    corrections    : dict mapping (sensor, region, season) → CDFCorrection

    Returns (NLAT, NLON) float32 corrected AOD array.
    """
    season = 'dry' if month in DRY_MONTHS else 'wet'

    corr_N = corrections.get((sensor, 'north', season))
    corr_S = corrections.get((sensor, 'south', season))

    # A correction whose quality gates rejected the fit has correction_type='none'
    # and would pass through unchanged in apply().  Treat it the same as a missing
    # anchor so the IDW blend never mixes raw AOD with bias-corrected AOD.
    if corr_N is not None and corr_N.correction_type == 'none':
        corr_N = None
    if corr_S is not None and corr_S.correction_type == 'none':
        corr_S = None

    # No corrections available: pass through
    if corr_N is None and corr_S is None:
        return aod_grid.copy()

    shape = aod_grid.shape
    out   = np.full(shape, np.nan, dtype=np.float32)
    valid = np.isfinite(aod_grid)

    if corr_N is None:
        # Only south anchor available
        out[valid] = corr_S.apply(aod_grid[valid])
        return out

    if corr_S is None:
        # Only north anchor available
        out[valid] = corr_N.apply(aod_grid[valid])
        return out

    # Both anchors available: IDW blend (weights on GPU, corrections stay CPU)
    xp     = _xp
    nghia  = AERONET_SITES['NGHIA_DO']
    bac    = AERONET_SITES['Bac_Lieu']
    w_N    = _idw_weights(lat_2d, lon_2d, nghia['lat'], nghia['lon'])  # GPU array
    w_S    = _idw_weights(lat_2d, lon_2d, bac['lat'],  bac['lon'])     # GPU array
    w_total = w_N + w_S

    corr_n_vals = xp.asarray(corr_N.apply(aod_grid))   # PchipInterpolator on CPU → GPU
    corr_s_vals = xp.asarray(corr_S.apply(aod_grid))

    blended = (w_N * corr_n_vals + w_S * corr_s_vals) / w_total
    out = xp.where(xp.asarray(valid), blended, np.nan).astype(np.float32)
    return np.asarray(out)


# ── Convenience: load all corrections for a sensor set ───────────────────────

def load_all_corrections(
    sensors: list[str],
    directory: Path | str = BIASC_DIR,
) -> dict[tuple[str, str, str], CDFCorrection]:
    """Load all saved CDFCorrection objects for the given sensor list.

    Returns a dict keyed by (sensor, region, season).
    Missing combinations are silently skipped.
    """
    directory = Path(directory)
    corrections = {}
    regions = ('north', 'south')      # only AERONET-anchored strata are saved
    seasons = ('dry', 'wet')
    for sensor in sensors:
        for region in regions:
            for season in seasons:
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

    Expects one CSV per (sensor, site) combination in collocated_csv_dir,
    named {sensor}_{site}.csv, with columns:
        satellite_aod, aeronet_aod, month, region, season

    Returns the dict of fitted CDFCorrection objects.
    """
    import pandas as pd

    collocated_csv_dir = Path(collocated_csv_dir)
    output_dir         = Path(output_dir)

    regions = ('north', 'south')
    seasons = ('dry', 'wet')

    # ── Load all collocated CSVs ───────────────────────────────────────────
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
            continue
    if csv_bar is not None:
        csv_bar.close()

    if not all_frames:
        print('[bias_correction] No collocated CSVs found; skipping training.')
        return {}

    df_all = pd.concat(all_frames, ignore_index=True)
    print(f'  Total pairs: {len(df_all):,}  '
          f'(sensors in data: {sorted(df_all["sensor"].unique())})')

    # ── Fit one CDFCorrection per (sensor, region, season) stratum ─────────
    strata = [
        (sensor, region, season)
        for sensor in sensors
        for region in regions
        for season in seasons
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

        c = CDFCorrection(sensor, region, season)
        c.fit(df_st['satellite_aod'].values, df_st['aeronet_aod'].values)
        c.save(output_dir)
        corrections[(sensor, region, season)] = c

        msg = f'  trained: {c}'
        if fit_bar is not None:
            fit_bar.write(msg)
        else:
            print(msg)

    if fit_bar is not None:
        fit_bar.close()

    # ── RMSE improvement summary ───────────────────────────────────────────
    if corrections:
        col_w = 15
        print(f'\n[bias_correction] Summary — {len(corrections)} strata trained:')
        hdr = (f"  {'Sensor':<{col_w}} {'Region':<8} {'Season':<5} "
               f"{'Type':<13} {'N':>6}  {'R':>5}  "
               f"{'RMSE_b':>7}  {'RMSE_a':>7}  {'RMSE_cv':>7}  Note")
        print(hdr)
        print('  ' + '─' * (len(hdr) - 2))
        for (s, reg, sea), c in sorted(corrections.items()):
            def _f(x, w=7):
                return f'{x:.4f}' if not np.isnan(x) else '    N/A'
            r_s    = f'{c.pearson_r:.2f}' if not np.isnan(c.pearson_r) else '  N/A'
            note   = c.reject_reason if c.correction_type == 'none' else ''
            print(f'  {s:<{col_w}} {reg:<8} {sea:<5} '
                  f'{c.correction_type:<13} {c.n_pairs:>6}  {r_s:>5}  '
                  f'{_f(c.rmse_before)}  {_f(c.rmse_after)}  {_f(c.rmse_after_cv)}  {note}')

    return corrections


# ── Step A4b: LEO–Himawari spatial-offset correction (thesis §7.4.2) ─────────

def _scan_merged_files_for_offset(start_d, end_d):
    """Yield Path objects for merged NetCDFs in the [start_d, end_d] range."""
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

        offset[lat, lon, level, season] = mean( Himawari_corrected
                                                − mean(LEO_sensors_corrected) )

    over the training period.  The map is then Gaussian-smoothed (mask-weighted
    so no-data cells do not pollute their neighbours) and written as a NetCDF
    with one offset grid per (level, season) combination.

    Prerequisite: a Stage A run already exists for [start_d, end_d] so that
    MERGED_DIR contains `AOD_himawari_l2`, `AOD_himawari_l3`, and at least one
    of `AOD_modis_maiac` / `AOD_viirs_*` per slot.  Run **after** Bug 1 has
    been fixed so the L2 and L3 diagnostic grids carry distinct data.

    Parameters
    ----------
    start_d, end_d : datetime.date
        Inclusive date range to scan in MERGED_DIR.
    output_path : Path
        Destination NetCDF (default config.LEO_HIMAWARI_OFFSET_FILE).
    min_pairs : int
        Cells with fewer co-located pairs are masked (set to 0 offset).
    smooth_sigma : float
        Gaussian sigma in grid-cells for spatial smoothing.
    """
    import netCDF4 as nc
    from scipy.ndimage import gaussian_filter
    from config import SENSOR_RMSE_FLOOR, MODIS_SOUTH_WEIGHT_FACTOR
    from fusion import load_rmse

    levels   = ('l2', 'l3')
    seasons  = ('dry', 'wet')
    leo_vars = ('AOD_modis_maiac', 'AOD_viirs_snpp', 'AOD_viirs_noaa20')

    # ── Per-cell, per-sensor, per-season ICW weight maps ────────────────────
    # Anchoring on a single LEO would discard the most-common sensors outside
    # that LEO's overpass window; a plain nanmean lets per-slot availability
    # swing the reference (MAIAC-only at noon vs VIIRS at 13:30 give a
    # different "truth").  An ICW-weighted mean with the same 1/RMSE²
    # convention as fusion.py keeps the reference identity-of-mix-invariant
    # whenever ≥1 LEO is present.
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

    shape = (NLAT, NLON)
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
                # ICW-weighted LEO reference: Σ(w_i · a_i) / Σ(w_i) per cell,
                # over whatever LEO subset is valid for this slot.
                num     = np.zeros(shape, dtype=np.float64)
                denom   = np.zeros(shape, dtype=np.float64)
                any_leo = False
                for v in leo_vars:
                    if v not in ds.variables:
                        continue
                    any_leo = True
                    a = np.ma.filled(
                        ds.variables[v][:].astype(np.float32), np.nan)
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
                    var = f'AOD_himawari_{lv}'
                    if var not in ds.variables:
                        continue
                    hi = np.ma.filled(
                        ds.variables[var][:].astype(np.float32), np.nan)
                    valid = np.isfinite(hi) & np.isfinite(leo_mean)
                    if not np.any(valid):
                        continue
                    key = (lv, season)
                    sums[key][valid]   += (hi[valid] - leo_mean[valid]).astype(np.float64)
                    counts[key][valid] += 1
        except Exception as exc:
            print(f'  skip {fpath.name}: {exc}')
            continue
    if bar is not None:
        bar.close()

    # ── Mean, mask-weighted smoothing, NaN → 0 outside coverage ─────────────
    offsets: dict[tuple, np.ndarray] = {}
    pair_counts: dict[tuple, np.ndarray] = {}
    for key in sums:
        cnt = counts[key]
        ok  = cnt >= min_pairs
        with np.errstate(invalid='ignore', divide='ignore'):
            raw = np.where(ok, sums[key] / np.maximum(cnt, 1), 0.0)
        if smooth_sigma > 0 and np.any(ok):
            # Mask-weighted Gaussian: numerator carries the value × mask,
            # denominator carries the mask itself; ratio reproduces a valid
            # weighted average without leaking 0s into surrounding cells.
            num   = gaussian_filter(raw * ok, sigma=smooth_sigma)
            denom = gaussian_filter(ok.astype(np.float64), sigma=smooth_sigma)
            with np.errstate(invalid='ignore', divide='ignore'):
                smooth = np.where(denom > 0.01, num / denom, 0.0)
            offsets[key] = smooth.astype(np.float32)
        else:
            offsets[key] = raw.astype(np.float32)
        pair_counts[key] = cnt.astype(np.int32)

    # ── Write NetCDF ─────────────────────────────────────────────────────────
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with nc.Dataset(str(output_path), 'w', format='NETCDF4') as ds:
        ds.title       = 'Himawari↔LEO spatial offset map (thesis §7.4.2)'
        ds.train_start = start_d.isoformat()
        ds.train_end   = end_d.isoformat()
        ds.min_pairs   = int(min_pairs)
        ds.smooth_sigma = float(smooth_sigma)
        ds.createDimension('lat', NLAT)
        ds.createDimension('lon', NLON)
        v_lat = ds.createVariable('lat', 'f4', ('lat',))
        v_lat[:] = LATS.astype(np.float32)
        v_lon = ds.createVariable('lon', 'f4', ('lon',))
        v_lon[:] = LONS.astype(np.float32)

        for (lv, sea), arr in offsets.items():
            name = f'offset_{lv}_{sea}'
            v = ds.createVariable(name, 'f4', ('lat', 'lon'),
                                  zlib=True, complevel=4)
            v.long_name = f'Mean Himawari-{lv.upper()} minus LEO_mean, {sea} season'
            v.units     = '1'
            v[:] = arr
        for (lv, sea), arr in pair_counts.items():
            name = f'n_pairs_{lv}_{sea}'
            v = ds.createVariable(name, 'i4', ('lat', 'lon'),
                                  zlib=True, complevel=4)
            v.long_name = f'Number of (Himawari-{lv.upper()}, LEO) co-located slots, {sea} season'
            v[:] = arr

    # ── Summary ────────────────────────────────────────────────────────────
    print(f'[leo_himawari_offset] saved → {output_path}')
    for (lv, sea), arr in offsets.items():
        cnt = pair_counts[(lv, sea)]
        ok = cnt >= min_pairs
        if np.any(ok):
            print(f'  {lv}/{sea}: n_cells_ok={int(ok.sum()):5d}  '
                  f'mean_offset={float(arr[ok].mean()):+.4f}  '
                  f'min={float(arr[ok].min()):+.4f}  max={float(arr[ok].max()):+.4f}')
        else:
            print(f'  {lv}/{sea}: no cells met min_pairs={min_pairs}')


def load_leo_himawari_offset(
    fpath: Path | str = LEO_HIMAWARI_OFFSET_FILE,
) -> Optional[dict[tuple[str, str], np.ndarray]]:
    """Return offsets dict keyed by (level, season) ∈ {l2,l3}×{dry,wet}, or None."""
    import netCDF4 as nc
    fpath = Path(fpath)
    if not fpath.exists():
        return None
    out: dict[tuple[str, str], np.ndarray] = {}
    try:
        with nc.Dataset(str(fpath)) as ds:
            for lv in ('l2', 'l3'):
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
    """Subtract the LEO-anchored spatial offset from a Himawari grid.

    Returns aod_grid unchanged if `sensor` is not Himawari, if `offsets`
    lacks the relevant (level, season) entry, or for NaN input pixels.
    """
    if sensor not in ('himawari_l2', 'himawari_l3'):
        return aod_grid
    lv  = 'l2' if sensor == 'himawari_l2' else 'l3'
    sea = 'dry' if month in DRY_MONTHS else 'wet'
    off = offsets.get((lv, sea))
    if off is None:
        return aod_grid
    out = aod_grid.astype(np.float32, copy=True)
    valid = np.isfinite(out)
    out[valid] = np.clip(out[valid] - off[valid], 0.0, None)
    return out
