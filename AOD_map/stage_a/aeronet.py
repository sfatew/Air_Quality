"""Load and preprocess AERONET V3 L2.0 data for the two Vietnam anchor sites."""

from __future__ import annotations
import importlib.util
import sys
from pathlib import Path
import numpy as np
import pandas as pd

# Load stage_a/config.py by absolute file path so this module works correctly
# when imported from stage_b (where a sibling stage_b/config.py would otherwise
# win on sys.path). Reuse the already-loaded module when present.
if 'stage_a_config' in sys.modules:
    _cfg = sys.modules['stage_a_config']
else:
    _spec = importlib.util.spec_from_file_location(
        'stage_a_config', Path(__file__).resolve().parent / 'config.py'
    )
    _cfg = importlib.util.module_from_spec(_spec)
    sys.modules['stage_a_config'] = _cfg
    _spec.loader.exec_module(_cfg)

AERONET_DIR        = _cfg.AERONET_DIR
AERONET_SITES      = _cfg.AERONET_SITES
DRY_MONTHS         = _cfg.DRY_MONTHS
NORTH_CENTRAL_LAT  = _cfg.NORTH_CENTRAL_LAT
CENTRAL_SOUTH_LAT  = _cfg.CENTRAL_SOUTH_LAT


# ── Helper ────────────────────────────────────────────────────────────────────

def aod_at_550(aod_500: np.ndarray, alpha: np.ndarray) -> np.ndarray:
    """Ångström interpolation from 500 nm to 550 nm.

    τ_550 = τ_500 × (550 / 500)^(−α)
    """
    return aod_500 * (550.0 / 500.0) ** (-alpha)


def get_region(lat: float) -> str:
    """Map a latitude to one of three Vietnam climate regions."""
    if lat >= NORTH_CENTRAL_LAT:
        return 'north'
    elif lat >= CENTRAL_SOUTH_LAT:
        return 'central'
    return 'south'


# ── Per-site loader ───────────────────────────────────────────────────────────

def load_aeronet(site: str) -> pd.DataFrame:
    """Load AERONET V3 L2.0 CSV for one site.

    Returns a clean DataFrame with columns:
        datetime        UTC+7 / Vietnam local time (tz-naive pandas Timestamp;
                        crawl.py adds +7 h to raw AERONET UTC timestamps)
        site            site name string
        lat, lon        site coordinates (degrees)
        aod_500         AOD at 500 nm
        aod_550         AOD at 550 nm (Ångström interpolated)
        alpha_550       Ångström exponent used for the 500→550 interpolation
                        (440-675 preferred, 500-870 fallback, 440-870 last)
        aod_675, aod_440
        region          'north' | 'central' | 'south'
        season          'dry' | 'wet'
    """
    csv_path = Path(AERONET_DIR) / f'{site}.csv'
    df = pd.read_csv(csv_path, parse_dates=['datetime'])
    df['datetime'] = pd.to_datetime(df['datetime'])

    df['aod_500']       = pd.to_numeric(df['AOD_500nm'],             errors='coerce')
    df['aod_675']       = pd.to_numeric(df['AOD_675nm'],             errors='coerce')
    df['aod_440']       = pd.to_numeric(df['AOD_440nm'],             errors='coerce')

    # Ångström exponent for 500→550 nm interpolation.
    # Preference order (closest spectral match wins):
    #   1. 440-675  — brackets both 500 and 550 nm (most accurate)
    #   2. 500-870  — anchored at 500 nm
    #   3. 440-870  — broader, used only when (1)/(2) are NaN
    # Polarimetric variants are skipped: they are sparsely populated in
    # standard AERONET CSVs and would add NaNs at the coalesce step.
    ae_preferred = pd.to_numeric(df['440-675_Angstrom_Exponent'], errors='coerce')
    ae_500_870   = pd.to_numeric(df['500-870_Angstrom_Exponent'], errors='coerce')
    ae_440_870   = pd.to_numeric(df['440-870_Angstrom_Exponent'], errors='coerce')
    df['alpha_550'] = ae_preferred.fillna(ae_500_870).fillna(ae_440_870)

    # Remove obviously bad values (instrument noise, fill sentinels -999)
    for col in ['aod_500', 'aod_675', 'aod_440']:
        df[col] = df[col].where(df[col] >= 0, other=np.nan)

    # Ångström interpolation 500 → 550 nm
    valid = df['aod_500'].notna() & df['alpha_550'].notna()
    df['aod_550'] = np.where(
        valid,
        aod_at_550(df['aod_500'].values, df['alpha_550'].values),
        np.nan,
    )
    df['aod_550'] = df['aod_550'].where(df['aod_550'] >= 0, other=np.nan)

    meta = AERONET_SITES[site]
    df['site']   = site
    df['lat']    = meta['lat']
    df['lon']    = meta['lon']
    df['region'] = meta['region']
    df['season'] = df['datetime'].dt.month.map(
        lambda m: 'dry' if m in DRY_MONTHS else 'wet'
    )

    keep = ['datetime', 'site', 'lat', 'lon',
            'aod_500', 'aod_550', 'alpha_550', 'aod_675', 'aod_440',
            'region', 'season']
    df = df[keep].dropna(subset=['aod_550']).reset_index(drop=True)
    return df


def load_all_aeronet() -> pd.DataFrame:
    """Load and concatenate AERONET data from all configured Vietnam sites."""
    frames = [load_aeronet(site) for site in AERONET_SITES]
    return pd.concat(frames, ignore_index=True)


# ── Temporal aggregation ──────────────────────────────────────────────────────

def daily_mean(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate AERONET observations to daily means per site.

    Returns DataFrame with columns:
        date, site, lat, lon, region, season,
        aod_550_mean, aod_550_std, n_obs
    """
    df = df.copy()
    df['date'] = df['datetime'].dt.normalize()
    grp = (df.groupby(['date', 'site', 'lat', 'lon', 'region'])
             .agg(
                 aod_550_mean=('aod_550', 'mean'),
                 aod_550_std=('aod_550', 'std'),
                 n_obs=('aod_550', 'count'),
             )
             .reset_index())
    grp['season'] = grp['date'].dt.month.map(
        lambda m: 'dry' if m in DRY_MONTHS else 'wet'
    )
    return grp


def window_mean(df: pd.DataFrame, center_dt: pd.Timestamp,
                window_min: int = 30) -> pd.DataFrame:
    """Return AERONET rows within ±window_min minutes of center_dt."""
    delta = pd.Timedelta(minutes=window_min)
    mask = (df['datetime'] >= center_dt - delta) & (df['datetime'] <= center_dt + delta)
    return df.loc[mask].copy()
