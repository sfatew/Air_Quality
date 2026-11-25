#!/usr/bin/env python3
"""Summarise TFL runs: mean ± sd over repeats, and paired (run, cell) bootstrap of equal-cell RMSE vs references.

    python report.py --tag pilot2020
"""
import argparse
import json

import numpy as np
import pandas as pd

import common as M

MODELS = ["T", "S", "ST", "NNW", "DR", "T[LDF]", "ST[LDF]", "NNW[LDF]"]


def boot_vs(preds: list[pd.DataFrame], model: str, ref: str, n_boot: int = 2000, seed: int = 0) -> dict:
    """% change of equal-cell RMSE of `model` vs `ref`; resampling units = (run, test cell)."""
    units = []
    for r, p in enumerate(preds):
        g = p.groupby("cell_id")
        units.append(pd.DataFrame({"m": g.apply(lambda d: np.mean((d[model] - d.obs) ** 2), include_groups=False),
                                   "r": g.apply(lambda d: np.mean((d[ref] - d.obs) ** 2), include_groups=False)}))
    u = pd.concat(units)
    m, r = u.m.to_numpy(), u.r.to_numpy()
    idx = np.random.default_rng(seed).integers(0, len(u), (n_boot, len(u)))
    d = 100 * (np.sqrt(m[idx].mean(1)) / np.sqrt(r[idx].mean(1)) - 1)
    return dict(pct=100 * (np.sqrt(m.mean()) / np.sqrt(r.mean()) - 1), lo=np.percentile(d, 2.5),
                hi=np.percentile(d, 97.5), better=f"{int((m < r).sum())}/{len(u)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="pilot2020")
    a = ap.parse_args()
    d = M.RESULTS / a.tag
    runs = []
    for f in sorted(d.glob("*.json")):
        j = json.loads(f.read_text())
        for model, mt in j["metrics"].items():
            runs.append({**{k: j["info"][k] for k in ("source", "n_train", "rep")}, "model": model, **mt,
                         "ldf_corr_test": abs(j["info"]["ldf_corr_test"]), "w_nnw_ess": j["info"]["w_nnw_ess"],
                         "w_dr_ess": j["info"]["w_dr_ess"]})
    if not runs:
        print("no runs yet")
        return
    R = pd.DataFrame(runs)
    lines = [f"# TFL China→China — {a.tag}\n",
             f"Runs: {R.groupby(['source', 'n_train']).rep.nunique().to_dict()}\n"]

    for metric in ("R2", "RMSE_eq_cell"):
        g = R.groupby(["source", "n_train", "model"])[metric]
        mean, sd = g.mean().unstack().reindex(columns=MODELS), g.std().unstack().reindex(columns=MODELS)
        txt = mean.round(3).astype(str) + " ± " + sd.round(3).astype(str)
        lines.append(f"## {metric} on held-out South-China cells (mean ± sd over repeats)\n\n```\n{txt.to_string()}\n```\n")

    rows = []
    for (source, n), g in R.groupby(["source", "n_train"]):
        preds = [pd.read_pickle(d / f"{source}_n{n:02d}_rep{rep}.preds.pkl") for rep in sorted(g.rep.unique())]
        for ref in ("T", "ST"):
            for model in MODELS:
                if model != ref:
                    b = boot_vs(preds, model, ref)
                    rows.append(dict(source=source, n_train=n, ref=ref, model=model, **b))
    B = pd.DataFrame(rows)
    for ref in ("T", "ST"):
        t = B[B.ref == ref].pivot_table(index=["source", "n_train"], columns="model", values="pct").reindex(
            columns=[m for m in MODELS if m != ref]).round(1)
        lines.append(f"## % change of equal-cell RMSE vs {ref} (negative = better; pooled over repeats)\n\n"
                     f"```\n{t.to_string()}\n```\n")
    sig = B[(B.hi < 0) | (B.lo > 0)].round(2)
    lines.append("## Changes whose 95% bootstrap CI (units = run × cell) excludes 0\n\n```\n"
                 + sig[["source", "n_train", "ref", "model", "pct", "lo", "hi", "better"]].to_string(index=False) + "\n```\n")
    diag = R.groupby(["source", "n_train"])[["ldf_corr_test", "w_nnw_ess", "w_dr_ess"]].mean().round(3)
    lines.append("## Diagnostics (mean over repeats): |LDF corr| with log PM on test (latent sign is arbitrary), ESS share of NNW / DR weights\n\n"
                 f"```\n{diag.to_string()}\n```\n")
    text = "\n".join(lines)
    (d / "report.md").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
