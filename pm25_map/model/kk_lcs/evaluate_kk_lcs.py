#!/usr/bin/env python3
"""Compare KK/LCS variants on held-out KK rows → kk_lcs/output/report/.

Every comparison uses the rows ALL compared variants predicted (D/D_all skip the fold that holds the only
collocated pair, so A–D are compared on D's folds; A/B/C also on all 5 folds).
Reported: N, cells, KK stations, R², RMSE, MAE, bias, NMB, slope (est on obs); by on-site KK RH
(≤60, 60–75, 75–85, >85), ERA5 RH, season, region, PM level; equal-cell RMSE ratio vs A with a paired
cell bootstrap; the D acceptance checks; β per fold.

    python evaluate_kk_lcs.py [--root output]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import kk_lcs_data as KD  # noqa: E402

D = KD.D
VARIANTS = ["A", "B", "C", "D", "D_all", "E"]
RH_BINS, RH_LABELS = [-np.inf, 60, 75, 85, np.inf], ["≤60", "60–75", "75–85", ">85"]
PM_BINS, PM_LABELS = [-np.inf, 25, 50, np.inf], ["<25", "25–50", "≥50"]
KEY = ["cell_id", "time_lt"]


def md(t: pd.DataFrame, index: bool = True) -> str:
    try:
        return t.to_markdown(index=index)
    except ImportError:                       # tabulate is not installed in Airqua_env
        return "```\n" + t.to_string(index=index) + "\n```"


def load(root: Path, cv: str) -> dict[str, pd.DataFrame]:
    out = {}
    for v in VARIANTS:
        p = root / v / cv / "oof.pkl"
        if p.exists():
            out[v] = pd.read_pickle(p)
    return out


def metrics(g: pd.DataFrame, n_stations: dict) -> pd.Series:
    m = D.metrics(g.y, g.pred)
    cells = g.cell_id.unique()
    return pd.Series(dict(N=m["N"], cells=len(cells), kk_stations=int(sum(n_stations.get(c, 0) for c in cells)),
                          R2=m["R2"], RMSE=m["RMSE"], MAE=m["MAE"], bias=m["bias"],
                          NMB_pct=100 * (g.pred - g.y).sum() / g.y.sum(), slope=m["slope"]))


def common(oofs: dict, names: list[str]) -> pd.DataFrame:
    base = oofs[names[0]][KEY + ["region", "season", "rh_site", "RH", "y", "fold"]]
    for v in names:
        base = base.merge(oofs[v][KEY + ["pred"]].rename(columns={"pred": f"pred_{v}"}), on=KEY, how="inner")
    rh = pd.cut(base.rh_site, RH_BINS, labels=RH_LABELS).astype(object).fillna("missing")
    base["rh_bin"] = pd.Categorical(rh, categories=RH_LABELS + ["missing"], ordered=True)
    base["rh_era5_bin"] = pd.cut(base.RH, RH_BINS, labels=RH_LABELS)
    base["pm_bin"] = pd.cut(base.y, PM_BINS, labels=PM_LABELS)
    base["season"] = pd.Categorical(base.season, categories=["DJF", "MAM", "JJA", "SON"], ordered=True)
    base["pm_tertile"] = pd.qcut(base.y, 3, labels=["T1 low", "T2 mid", "T3 high"])
    return base


def long_form(c: pd.DataFrame, names: list[str]) -> pd.DataFrame:
    return pd.concat([c.assign(variant=v, pred=c[f"pred_{v}"]) for v in names], ignore_index=True)


def table(c: pd.DataFrame, names: list[str], by: str | None, n_st: dict) -> pd.DataFrame:
    lf = long_form(c, names)
    keys = ["variant"] + ([by] if by else [])
    t = lf.groupby(keys, observed=True).apply(lambda g: metrics(g, n_st), include_groups=False)
    return t.round(3)


def cell_bootstrap(c: pd.DataFrame, a: str, b: str, n: int = 5000, seed: int = 0) -> dict:
    e = c.assign(ea=(c[f"pred_{a}"] - c.y) ** 2, eb=(c[f"pred_{b}"] - c.y) ** 2).groupby("cell_id")[["ea", "eb"]].mean()
    idx = np.random.default_rng(seed).integers(0, len(e), (n, len(e)))
    r = np.sqrt(e.eb.values[idx].mean(1)) / np.sqrt(e.ea.values[idx].mean(1)) - 1
    return dict(pct=100 * (np.sqrt(e.eb.mean()) / np.sqrt(e.ea.mean()) - 1), ci_lo=100 * np.percentile(r, 2.5),
                ci_hi=100 * np.percentile(r, 97.5), cells_better=int((e.eb < e.ea).sum()), cells=len(e))


def betas(root: Path) -> pd.DataFrame:
    rows = []
    for v in ("C", "D", "D_all", "E"):
        for cv in ("site", "temporal"):
            for f in sorted((root / v / cv / "folds").glob("fold_*.json")):
                j = json.loads(f.read_text())
                c = j.get("correction", {})
                p = next(iter(c.get("pairs", {}).values()), {})
                rows.append(dict(variant=v, cv=cv, fold=f.stem, status=j.get("status"),
                                 lcs_weight=j.get("lcs_weight"), beta=c.get("beta"),
                                 a=p.get("a"), b=p.get("b"), pair_hours=p.get("n_hours"),
                                 inner=",".join(j.get("inner_corrections", [])), reason=j.get("reason")))
    return pd.DataFrame(rows)


def d_checks(c: pd.DataFrame, ref: str, cand: str) -> dict:
    def rm(g, v):
        return float(np.sqrt(((g[f"pred_{v}"] - g.y) ** 2).mean()))

    def ma(g, v):
        return float((g[f"pred_{v}"] - g.y).abs().mean())
    hi, lo = c[c.rh_bin == ">85"], c[c.rh_bin == "≤60"]
    bias_hi = {v: float((hi[f"pred_{v}"] - hi.y).mean()) for v in (ref, cand)}
    return {"rmse_better": rm(c, cand) < rm(c, ref), "mae_better": ma(c, cand) < ma(c, ref),
            "abs_bias_RH>85_reduced": abs(bias_hi[cand]) < abs(bias_hi[ref]),
            "RH≤60_rmse_not_worse(+1%)": rm(lo, cand) <= 1.01 * rm(lo, ref),
            "rmse": {ref: rm(c, ref), cand: rm(c, cand)}, "mae": {ref: ma(c, ref), cand: ma(c, cand)},
            "bias_RH>85": bias_hi, "rmse_RH≤60": {ref: rm(lo, ref), cand: rm(lo, cand)}}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--root", type=Path, default=HERE / "output")
    a = ap.parse_args()
    rep = a.root / "report"
    rep.mkdir(parents=True, exist_ok=True)
    st = pd.read_csv(KD.STATIONS_CSV)
    n_st = st[st.cls == "KK"].groupby("cell_id").size().to_dict()
    lines = ["# KK/LCS experiment report", ""]
    summary = {}
    for cv in ("site", "temporal"):
        oofs = load(a.root, cv)
        if not oofs:
            continue
        sets = []
        main_names = [v for v in VARIANTS if v in oofs]
        sets.append(("all_available_common_rows", main_names))
        abc = [v for v in ("A", "B", "C") if v in oofs]
        if len(abc) > 1 and abc != main_names:
            sets.append(("ABC_all_folds", abc))
        for tag, names in sets:
            c = common(oofs, names)
            folds = sorted(c.fold.unique().tolist())
            lines += [f"## {cv} · {tag} · folds {folds} · {c.cell_id.nunique()} KK cells · {len(c):,} rows", ""]
            for by in (None, "rh_bin", "rh_era5_bin", "season", "region", "pm_bin", "pm_tertile"):
                t = table(c, names, by, n_st)
                t.to_csv(rep / f"{cv}_{tag}_{by or 'overall'}.csv")
                lines += [f"### by {by or 'overall'}", "", md(t), ""]
            if "A" in names:
                bs = {v: cell_bootstrap(c, "A", v) for v in names if v != "A"}
                lines += ["### equal-cell RMSE vs A (paired cell bootstrap, 95% CI)", "",
                          md(pd.DataFrame(bs).T.round(2)), ""]
                summary[f"{cv}/{tag}/bootstrap_vs_A"] = bs
            if "C" in names and "E" in names:
                bs = cell_bootstrap(c, "C", "E")
                summary[f"{cv}/{tag}/E_vs_C_bootstrap"] = bs
                lines += ["### E vs C (correction effect at tuned LCS weight; paired cell bootstrap)", "",
                          md(pd.DataFrame({"E": bs}).T.round(2)), ""]
            for ref, cand in (("A", "D"), ("A", "D_all"), ("C", "E")):
                if cand in names and ref in names:
                    chk = d_checks(c, ref, cand)
                    summary[f"{cv}/{tag}/{cand}_vs_{ref}_checks"] = chk
                    lines += [f"### {cand} vs {ref} acceptance checks", "", "```",
                              json.dumps(chk, indent=2, ensure_ascii=False), "```", ""]
    b = betas(a.root)
    if len(b):
        b.to_csv(rep / "beta_per_fold.csv", index=False)
        lines += ["## chosen LCS weight and β per fold (C, D, D_all, E)", "", md(b, index=False), "",
                  "Note: all D training folds contain the same single collocated pair, so equal β across folds is "
                  "not evidence of stability; see the day-block bootstrap of β in the report notes.", ""]
    (rep / "report.md").write_text("\n".join(lines))
    (rep / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False, default=float))
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
