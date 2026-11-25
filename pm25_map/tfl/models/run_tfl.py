#!/usr/bin/env python3
"""China→China spatial transfer learning for hourly 0.05° PM2.5 (Gupta et al. 2024 design).

For each source region (central, north), each number of labelled target cells N ∈ N_TRAIN and each repeat,
N South-China cells are labelled for training and every other eligible South-China cell is the test set.
Models (all GBR = LightGBM with the same fixed parameters, common.GBR):
    T          target cells only                                   (paper: ML baselines)
    S          source only                                         (pure transfer, no target labels)
    ST         source + target, unweighted                         (naive pooling)
    NNW        source reweighted by nearest-neighbour weighting + target   (paper: best ITL model)
    DR         source reweighted by classifier density ratio + target      (scalable KLIEP/KMM stand-in)
    T[LDF], ST[LDF], NNW[LDF]   the same with the Latent Dependency Factor added as a feature
Metrics on the held-out target cells; per-run JSON + test predictions (for paired bootstrap) in results/.

    python run_tfl.py                                  # everything, resumable per run
    python run_tfl.py --sources central --ns 5 --reps 1 --tag smoke
"""
import argparse
import json
import time
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", message="X does not have valid feature names")

import common as M  # noqa: E402
import itl  # noqa: E402
from ldf import LDF  # noqa: E402

# Instance weights compare CONDITIONS, not places: source and target are disjoint regions, so with x, y in the
# weighting space the domain classifier separates them perfectly (all DR weights collapse to one value) and NNW
# only rewards the source cells nearest the border. The regressors still see every feature.
DR_FEATS = [f for f in M.FEATS if f not in ("x", "y")]
NNW_FEATS = [f for f in DR_FEATS if f != "aod"]       # + no mostly-missing AOD in the distance space


def log(msg: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S}  {msg}", flush=True)


def X(d: pd.DataFrame, feats: list) -> np.ndarray:
    return d[feats].to_numpy(np.float32)


def run_one(df: pd.DataFrame, source: str, n: int, rep: int, device: str, n_jobs: int) -> tuple[dict, pd.DataFrame]:
    src, ttr, tte = M.split(df, source, n, rep)
    y_te, cell_te = tte.pm25.to_numpy(float), tte.cell_id.to_numpy()
    res, preds = {}, {}
    info = dict(source=source, n_train=n, rep=rep, src_rows=len(src), ttr_rows=len(ttr), tte_rows=len(tte),
                ttr_cells=sorted(map(int, ttr.cell_id.unique())), tte_cells=int(tte.cell_id.nunique()))

    def evaluate(name, X_tr, y_tr, w, feats):
        t0 = time.time()
        p = M.fit(X_tr, y_tr, w, n_jobs).predict(X(tte, feats))
        res[name] = M.metrics(y_te, p, cell_te)
        preds[name] = p.astype(np.float32)
        log(f"  {name:9s} R²={res[name]['R2']:.3f} RMSE={res[name]['RMSE']:.2f} "
            f"eq-cell RMSE={res[name]['RMSE_eq_cell']:.2f} ({time.time() - t0:.0f}s)")

    F = M.FEATS
    st = pd.concat([src, ttr])
    ones_t = np.ones(len(ttr))
    evaluate("T", X(ttr, F), ttr.pm25, None, F)
    evaluate("S", X(src, F), src.pm25, None, F)
    evaluate("ST", X(st, F), st.pm25, None, F)
    w_nnw = itl.nnw_weights(X(src, NNW_FEATS), X(ttr, NNW_FEATS))
    evaluate("NNW", X(st, F), st.pm25, np.r_[w_nnw, ones_t], F)
    w_dr = itl.dr_weights(X(src, DR_FEATS), X(ttr, DR_FEATS), seed=rep)
    evaluate("DR", X(st, F), st.pm25, np.r_[w_dr, ones_t], F)
    info["w_nnw_ess"] = float(w_nnw.sum() ** 2 / (w_nnw ** 2).sum() / len(w_nnw))
    info["w_dr_ess"] = float(w_dr.sum() ** 2 / (w_dr ** 2).sum() / len(w_dr))

    t0 = time.time()
    allr = pd.concat([src, ttr, tte], ignore_index=True)
    labelled = np.r_[np.ones(len(src) + len(ttr), bool), np.zeros(len(tte), bool)]
    ldf = LDF(allr, labelled, device=device, seed=rep).fit(log=log).transform()
    src, ttr, tte = (d.assign(ldf=v) for d, v in zip((src, ttr, tte), np.split(ldf, [len(src), len(src) + len(ttr)])))
    info["ldf_seconds"] = time.time() - t0
    info["ldf_missing_test"] = float(np.isnan(tte.ldf).mean())
    ok = np.isfinite(tte.ldf.to_numpy())
    info["ldf_corr_test"] = float(np.corrcoef(tte.ldf[ok], np.log1p(tte.pm25[ok]))[0, 1])
    log(f"  LDF: corr with log PM on test {info['ldf_corr_test']:.3f} · missing {info['ldf_missing_test']:.1%} "
        f"({info['ldf_seconds']:.0f}s)")

    F2 = F + ["ldf"]
    st = pd.concat([src, ttr])
    evaluate("T[LDF]", X(ttr, F2), ttr.pm25, None, F2)
    evaluate("ST[LDF]", X(st, F2), st.pm25, None, F2)
    w2 = itl.nnw_weights(X(src, NNW_FEATS + ["ldf"]), X(ttr, NNW_FEATS + ["ldf"]))
    evaluate("NNW[LDF]", X(st, F2), st.pm25, np.r_[w2, ones_t], F2)

    out = pd.DataFrame({"cell_id": cell_te, "time_utc": tte.time_utc.to_numpy(), "obs": y_te, **preds})
    return dict(info=info, metrics=res), out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", nargs="+", default=M.SOURCES)
    ap.add_argument("--ns", nargs="+", type=int, default=M.N_TRAIN)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--n-jobs", type=int, default=8)
    ap.add_argument("--tag", default="pilot2020")
    a = ap.parse_args()
    out_dir = M.RESULTS / a.tag
    out_dir.mkdir(parents=True, exist_ok=True)
    df = M.B.load()
    log(f"frame {len(df):,} cell-hours · target cells eligible {len(M.target_cells(df))} · tag {a.tag}")
    for source in a.sources:
        for n in a.ns:
            for rep in range(a.reps):
                stem = out_dir / f"{source}_n{n:02d}_rep{rep}"
                if stem.with_suffix(".json").exists():
                    log(f"[{stem.name}] done already")
                    continue
                log(f"[{stem.name}] start")
                t0 = time.time()
                result, preds = run_one(df, source, n, rep, a.device, a.n_jobs)
                result["info"]["seconds"] = time.time() - t0
                preds.to_pickle(stem.with_suffix(".preds.pkl"))
                stem.with_suffix(".json").write_text(json.dumps(result, indent=2))
                log(f"[{stem.name}] done ({time.time() - t0:.0f}s)")
    log("all done")


if __name__ == "__main__":
    main()
