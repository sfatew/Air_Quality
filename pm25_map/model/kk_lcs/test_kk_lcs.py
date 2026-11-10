#!/usr/bin/env python3
"""Checks for the KK/LCS experiment (plain python, no pytest in Airqua_env).

    python test_kk_lcs.py
Covers: label mapping vs the production frame, fold integrity (cells never split, KK/LCS of a cell share
a fold), test rows are held-out KK only, D's correction never sees the held-out cells / post-cutoff data,
correction maths on synthetic data, identifiability guard.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import kk_lcs_data as KD  # noqa: E402
import rh_correction as RC  # noqa: E402
import run_kk_lcs as R  # noqa: E402

FAILS: list[str] = []


def check(cond: bool, msg: str) -> None:
    print(("PASS  " if cond else "FAIL  ") + msg)
    if not cond:
        FAILS.append(msg)


def test_mapping(data):
    long = data["long"]
    pure = long.groupby("cell_id").cls.nunique() == 1
    p = long[long.cell_id.isin(pure[pure].index)]
    check(np.allclose(p.y_raw, p.pm25_cell), "pure-class cells: y_raw == frame pm25 (map_aqi_grid rule)")
    mixed = long[~long.cell_id.isin(pure[pure].index)]
    check(mixed.cell_id.nunique() == 4, f"4 mixed KK+LCS cells (found {mixed.cell_id.nunique()})")
    both = mixed.groupby(["cell_id", "time_lt"]).filter(lambda g: len(g) == 2)
    if len(both):
        g = both.groupby(["cell_id", "time_lt"])
        w = g.n_stations_cls.transform("sum")
        rec = (both.y_raw * both.n_stations_cls).groupby([both.cell_id, both.time_lt]).sum() / g.n_stations_cls.sum()
        cell = g.pm25_cell.first()
        check(np.allclose(rec.values, cell.values), "mixed cells: station-weighted class means reproduce frame pm25")
    check(not long.duplicated(["cell_id", "time_lt", "cls"]).any(), "one row per (cell, hour, class)")
    check(long[R.FEATURES].notna().all().all() or long[R.FEATURES].isna().sum().max() == long.aod.isna().sum(),
          "predictors complete (only AOD may be NaN, as in the frame)")
    check(set(long.cls) == {"KK", "LCS"}, "OTHER station dropped")


def test_folds(data):
    long = data["long"]
    check((long.groupby("cell_id").outer_fold.nunique() == 1).all(), "each cell (KK and LCS rows) in one outer fold")
    ref = pd.read_csv(KD.D.FOLDS_CSV).set_index("cell_id").outer_fold
    check((long.outer_fold == long.cell_id.map(ref)).all(), "outer folds identical to _data cv_folds_hourly.csv")
    for k in range(5):
        for use_lcs in (True, False):
            tr, te, _ = R.split_rows(long, "site", k, use_lcs)
            check(not set(long.cell_id.iloc[tr]) & set(long.cell_id.iloc[te]), f"site fold {k} lcs={use_lcs}: no cell in both")
            check((long.cls.iloc[te] == "KK").all(), f"site fold {k}: test rows are KK only")
            if not use_lcs:
                check((long.cls.iloc[tr] == "KK").all(), f"site fold {k}: B trains on KK only")
    tr, te, cut = R.split_rows(long, "temporal", 0, True)
    check((long.time_lt.iloc[tr] < cut).all() and (long.time_lt.iloc[te] >= cut).all(), "temporal: train < cutoff ≤ test")
    check((long.cls.iloc[te] == "KK").all(), "temporal: test rows KK only")


def test_correction_leakage(data):
    long = data["long"]
    lab = R.Labels(data, R.VARIANTS["D"])
    pair = next(iter(data["colloc"].values()))
    fold_pair = int(long.loc[long.cell_id == pair["kk_cell"], "outer_fold"].iloc[0])
    for k in range(5):
        tr, _, cut = R.split_rows(long, "site", k, True)
        cells = set(long.cell_id.iloc[tr])
        pairs = lab.pairs(cells, cut)
        if k == fold_pair:
            check(not pairs, f"fold {k} (holds the collocated cell): no pair available → D must skip")
            _, info = lab.y(cells, cut)
            check(info["correction"] == "not_identifiable", f"fold {k}: correction reported not identifiable")
        else:
            check(len(pairs) == 1, f"fold {k}: the collocated pair (training cell) is available")
    tr, te, cut = R.split_rows(long, "temporal", 0, True)
    pairs = lab.pairs(set(long.cell_id.iloc[tr]), cut)
    check(all((d.index < cut).all() for d in pairs.values()), "temporal: pair data restricted to < cutoff")
    y, info = lab.y(set(long.cell_id.iloc[tr]), cut)
    kk = (long.cls == "KK").to_numpy()
    check(np.array_equal(y[kk], long.y_raw.to_numpy()[kk]), "D never alters KK labels")
    ls = data["lcs_station"]
    hn_cells = set(ls.loc[ls.province == "Hà Nội", "cell_id"])
    other = ((long.cls == "LCS") & ~long.cell_id.isin(hn_cells)).to_numpy()
    check(np.allclose(y[other], long.y_raw.to_numpy()[other]), "D: LCS cells without Hà Nội stations unchanged")
    hn = ((long.cls == "LCS") & long.cell_id.isin(hn_cells)).to_numpy()
    check(np.nanmean(y[hn]) < np.nanmean(long.y_raw.to_numpy()[hn]), "D: Hà Nội LCS labels reduced on average (β>0)")
    check(info["correction"] == "fitted" and info["pairs"], f"temporal: correction fitted (β={info.get('beta', np.nan):.4f})")


def test_correction_maths():
    rng = np.random.default_rng(1)
    n = 1500
    idx = pd.date_range("2025-01-01", periods=n, freq="h")
    rh = rng.uniform(40, 99, n)
    kk = rng.gamma(3, 12, n)
    lcs = -4 + 1.1 * kk * np.exp(0.08 * RC.h_of(rh)) + rng.normal(0, 3, n)
    f = RC.fit_pair("synthetic", pd.DataFrame({"lcs": lcs, "kk": kk, "rh": rh}, index=idx))
    check(abs(f.beta - 0.08) < 0.01 and abs(f.a - 1.1) < 0.05 and abs(f.b + 4) < 1.5,
          f"synthetic recovery b={f.b:.2f} a={f.a:.3f} β={f.beta:.4f} (true −4, 1.1, 0.08)")
    check(np.allclose(RC.h_of([50, 60, 70, 95, 120]), [0, 0, 1, 3.5, 3.5]), "h(RH): 0 below 60, linear, capped at 95")
    ls = pd.DataFrame({"station": ["s1", "s2"], "pm25": [50.0, 50.0], "rh": [80.0, np.nan]})
    c = RC.correct_stations(ls, 0.1, {}, np.array([True, True]))
    check(np.isclose(c.pm25_corr.iloc[0], 50 / np.exp(0.2)) and c.pm25_corr.iloc[1] == 50 and c.rh_missing.iloc[1],
          "correct_stations: divides by exp(βh); missing RH left uncorrected and flagged")
    narrow = pd.DataFrame({"lcs": lcs[:300], "kk": kk[:300], "rh": np.full(300, 90.0)}, index=idx[:300])
    try:
        RC.fit_pair("narrow", narrow)
        check(False, "identifiability guard rejects a pair without RH≤60 hours")
    except RC.NotIdentifiable:
        check(True, "identifiability guard rejects a pair without RH≤60 hours")


if __name__ == "__main__":
    data = KD.build(verbose=False)
    test_mapping(data)
    test_folds(data)
    test_correction_leakage(data)
    test_correction_maths()
    print(f"\n{'ALL PASSED' if not FAILS else str(len(FAILS)) + ' FAILED'}")
    sys.exit(1 if FAILS else 0)
