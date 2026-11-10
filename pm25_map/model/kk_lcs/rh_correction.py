"""RH correction of LCS PM2.5 (variant D), fitted only on collocated LCS–KK pairs.

    h(RH)    = max(0, (min(RH, RH_CAP) − 60) / 10)        RH = on-site LCS humidity (%); RH > 95 capped + flagged
    model    LCS = b_s + a_s · KK · exp(β_g · h)            robust least squares (soft-L1), per collocated station
    correct  LCS_corr = (LCS − b_s) / (a_s · exp(β_g h))   collocated station with its own fit
             LCS_corr = LCS / exp(β_g h)                    other stations of group g (a_s, b_s not identifiable)
             unchanged                                      stations outside the groups that have a β_g

β_g is the pooled fit over the group's collocated pairs (one pair exists in the network, so β_g is that
pair's β). Nearby — not collocated — pairs are never used: their LCS/KK ratio mixes real PM gradients and
local sources with the instrument response. If a group has no collocated pair in the training set, or the
pair does not span enough RH, `fit_group` raises NotIdentifiable — callers must not invent coefficients.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd
from scipy.optimize import least_squares

RH_REF, RH_STEP, RH_CAP = 60.0, 10.0, 95.0
MIN_HOURS, MIN_HOURS_LOW, MIN_HOURS_HIGH = 200, 50, 50     # identifiability: total, h == 0, h ≥ 1.5
F_SCALE = 10.0                                            # soft-L1 scale (µg/m³)


class NotIdentifiable(RuntimeError):
    pass


@dataclass
class PairFit:
    station: str
    b: float
    a: float
    beta: float
    n_hours: int
    n_h0: int
    n_hhigh: int
    t_min: str
    t_max: str


def h_of(rh) -> np.ndarray:
    rh = np.asarray(rh, dtype=float)
    return np.maximum(0.0, (np.minimum(rh, RH_CAP) - RH_REF) / RH_STEP)


def _fit_arrays(lcs, kk, h, x0=(0.0, 1.0, 0.0)) -> np.ndarray:
    res = least_squares(lambda p: p[0] + p[1] * kk * np.exp(p[2] * h) - lcs, x0=list(x0),
                        loss="soft_l1", f_scale=F_SCALE, bounds=([-50, 0.05, -1.0], [50, 20, 1.0]))
    return res.x


def check_identifiable(df: pd.DataFrame, name: str) -> tuple[int, int]:
    h = h_of(df.rh)
    n0, nh = int((h == 0).sum()), int((h >= 1.5).sum())
    if len(df) < MIN_HOURS or n0 < MIN_HOURS_LOW or nh < MIN_HOURS_HIGH:
        raise NotIdentifiable(f"{name}: {len(df)} h, {n0} h with RH≤60, {nh} h with RH≥75 "
                              f"(need ≥{MIN_HOURS}, ≥{MIN_HOURS_LOW}, ≥{MIN_HOURS_HIGH})")
    return n0, nh


def fit_pair(name: str, df: pd.DataFrame) -> PairFit:
    df = df.dropna(subset=["lcs", "kk", "rh"])
    n0, nh = check_identifiable(df, name)
    b, a, beta = _fit_arrays(df.lcs.to_numpy(float), df.kk.to_numpy(float), h_of(df.rh))
    return PairFit(name, float(b), float(a), float(beta), len(df), n0, nh, str(df.index.min()), str(df.index.max()))


def fit_group(pairs: dict[str, pd.DataFrame]) -> tuple[float, dict[str, PairFit]]:
    """β_g shared by the group, (a_s, b_s) per collocated station — fitted jointly when there are several pairs."""
    if not pairs:
        raise NotIdentifiable("no collocated LCS–KK pair available in the training data")
    fits = {n: fit_pair(n, d) for n, d in pairs.items()}
    if len(fits) == 1:
        f = next(iter(fits.values()))
        return f.beta, fits
    names = list(pairs)
    dd = [pairs[n].dropna(subset=["lcs", "kk", "rh"]) for n in names]

    def resid(p):
        beta = p[-1]
        return np.concatenate([p[2 * i] + p[2 * i + 1] * d.kk.to_numpy(float) * np.exp(beta * h_of(d.rh))
                               - d.lcs.to_numpy(float) for i, d in enumerate(dd)])
    x0 = sum(([fits[n].b, fits[n].a] for n in names), []) + [float(np.mean([f.beta for f in fits.values()]))]
    p = least_squares(resid, x0=x0, loss="soft_l1", f_scale=F_SCALE).x
    for i, n in enumerate(names):
        fits[n] = PairFit(n, float(p[2 * i]), float(p[2 * i + 1]), float(p[-1]), *list(asdict(fits[n]).values())[4:])
    return float(p[-1]), fits


def bootstrap_beta(df: pd.DataFrame, n_boot: int = 300, seed: int = 0) -> np.ndarray:
    """Day-block bootstrap of β (hours within a day are autocorrelated)."""
    df = df.dropna(subset=["lcs", "kk", "rh"])
    days = df.index.normalize()
    groups = {d: g for d, g in df.groupby(days)}
    keys = list(groups)
    rng = np.random.default_rng(seed)
    out = []
    for _ in range(n_boot):
        x = pd.concat([groups[keys[i]] for i in rng.integers(0, len(keys), len(keys))])
        out.append(_fit_arrays(x.lcs.to_numpy(float), x.kk.to_numpy(float), h_of(x.rh))[2])
    return np.asarray(out)


def correct_stations(ls: pd.DataFrame, beta: float, fits: dict[str, PairFit], in_group: np.ndarray) -> pd.DataFrame:
    """Corrected `pm25_corr` for LCS station-hours `ls` (columns station, pm25, rh) + flags."""
    out = ls.copy()
    h = h_of(out.rh.to_numpy(float))
    rh_missing = out.rh.isna().to_numpy()
    h[rh_missing] = 0.0
    factor = np.where(in_group, np.exp(beta * h), 1.0)
    y = out.pm25.to_numpy(float) / factor
    for name, f in fits.items():
        m = (out.station == name).to_numpy()
        y[m] = (out.pm25.to_numpy(float)[m] - f.b) / (f.a * np.exp(f.beta * h[m]))
    out["pm25_corr"] = np.maximum(y, 0.0)
    out["corr_applied"] = in_group & ~rh_missing
    out["rh_missing"] = rh_missing
    out["rh_capped"] = (out.rh > RH_CAP).to_numpy()
    return out
