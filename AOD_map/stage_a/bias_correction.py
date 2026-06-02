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
    _xp = cp
except ImportError:
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
    AERONET_SITES, DRY_MONTHS,
    CDF_N_QUANTILES, CDF_MIN_PAIRS,
    NORTH_CENTRAL_LAT, CENTRAL_SOUTH_LAT,
    EARTH_RADIUS_KM, BIASC_DIR,
)


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
        self._interp = None          # PchipInterpolator or None
        self._lin_slope: float = 1.0
        self._lin_intercept: float = 0.0

    # ── Training ──────────────────────────────────────────────────────────────

    def fit(self, sat_aod: np.ndarray, aer_aod: np.ndarray) -> 'CDFCorrection':
        """Fit the transfer function from matched (satellite, AERONET) pairs."""
        sat = sat_aod[np.isfinite(sat_aod) & np.isfinite(aer_aod) & (sat_aod >= 0) & (aer_aod >= 0)]
        aer = aer_aod[np.isfinite(sat_aod) & np.isfinite(aer_aod) & (sat_aod >= 0) & (aer_aod >= 0)]

        self.n_pairs = len(sat)
        if self.n_pairs < 5:
            self.correction_type = 'none'
            return self

        # RMSE before correction
        self.rmse_before = float(np.sqrt(np.mean((sat - aer) ** 2)))

        if self.n_pairs < CDF_MIN_PAIRS:
            # Linear fallback
            self.correction_type = 'linear'
            coeffs = np.polyfit(sat, aer, 1)
            self._lin_slope     = float(coeffs[0])
            self._lin_intercept = float(coeffs[1])
            corrected = self._lin_slope * sat + self._lin_intercept
        else:
            # Quantile mapping (Ahn 2021 CDF approach)
            self.correction_type = 'quantile_map'
            q = np.linspace(0.005, 0.995, CDF_N_QUANTILES)
            sat_q = np.quantile(sat, q)
            aer_q = np.quantile(aer, q)

            # Add endpoints to anchor extrapolation
            sat_all = np.concatenate([[0.0],           sat_q, [max(sat.max(), aer.max()) * 1.1]])
            aer_all = np.concatenate([[min(0.0, aer_q[0])], aer_q, [max(sat.max(), aer.max()) * 1.1]])

            # Enforce monotonicity in x (required by PchipInterpolator)
            order = np.argsort(sat_all)
            sat_all = sat_all[order]
            aer_all = aer_all[order]
            # Remove duplicates
            _, unique = np.unique(sat_all, return_index=True)
            sat_all = sat_all[unique]
            aer_all = aer_all[unique]

            self._interp = PchipInterpolator(sat_all, aer_all, extrapolate=True)
            corrected = np.clip(self._interp(sat), 0, None)

        self.rmse_after = float(np.sqrt(np.mean((corrected - aer) ** 2)))
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
        except Exception:
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
               f"{'Type':<13} {'N':>6}  {'RMSE_before':>11} → {'RMSE_after':<10}")
        print(hdr)
        print('  ' + '─' * (len(hdr) - 2))
        for (s, reg, sea), c in sorted(corrections.items()):
            rmse_b = f'{c.rmse_before:.4f}' if not np.isnan(c.rmse_before) else '   N/A'
            rmse_a = f'{c.rmse_after:.4f}'  if not np.isnan(c.rmse_after)  else '   N/A'
            print(f'  {s:<{col_w}} {reg:<8} {sea:<5} '
                  f'{c.correction_type:<13} {c.n_pairs:>6}  {rmse_b:>11} → {rmse_a}')

    return corrections
