#!/usr/bin/env python3
"""KK/LCS experiment — how should low-cost-sensor (LCS) labels enter the hourly PM2.5 model?

Variants (all: features `_data.X_VARS + _data.ST_VARS` (= "-st", 24), target hourly PM2.5 per
(cell, hour, class), LightGBM tuned by the train_global Optuna protocol, scored ONLY on held-out KK rows):
    A      KK + raw LCS, every row weight 1                                 (current practice, per class)
    B      KK only
    C      KK + raw LCS, LCS weight w ∈ {0.1, 0.25, 0.5} chosen INSIDE each outer fold by inner CV on KK
    D      KK + RH-corrected LCS (rh_correction.py), weight 1 as in A — main comparison D vs A.
           β_g from collocated pairs of the training data only; applied to Hà Nội LCS stations (the
           group the only collocated pair belongs to); a_s, b_s only for the collocated station itself.
    D_all  sensitivity: same β applied to every LCS station (other provinces' hardware is unverified)
    E      supplementary: D's correction + LCS weight tuned as in C → A/C/D/E = {raw, corrected} × {w=1, w tuned}

CV (`--cv`):
    site      the 5 cell folds of `_data.make_folds` (cv_folds_hourly.csv) — all rows of a cell, KK and LCS,
              share a fold. D/D_all skip a fold whose training cells contain no collocated pair (reported).
    temporal  train time_lt < 2026-01-01 (all cells), test KK rows 2026-01-01 … end (Jan–Apr 2026).
Hyper-parameter protocol per outer split: Optuna TPE `n_trials` (seed RANDOM_STATE + k), each trial scored
by mean R² on the KK rows of GroupKFold(inner_k)-by-cell inner validation; for D the correction is refitted
on the inner-training cells of every inner split (raw LCS for an inner split without a collocated pair).

Output (kk_lcs/output/<variant>/<cv>/): config.json, folds/fold_<k>.{json,npz} (resumable), oof.pkl (held-out
KK rows), metrics.json.

    python run_kk_lcs.py --variant A --cv site temporal --n-jobs 3
    python run_kk_lcs.py --variant D --cv site --smoke          # tiny run into output_smoke/
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore", message="X does not have valid feature names", category=UserWarning)

import lightgbm as lgb  # noqa: E402
import numpy as np  # noqa: E402
import optuna  # noqa: E402
import pandas as pd  # noqa: E402
from sklearn.metrics import r2_score  # noqa: E402
from sklearn.model_selection import GroupKFold  # noqa: E402

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import kk_lcs_data as KD  # noqa: E402
import rh_correction as RC  # noqa: E402

D = KD.D
sys.path.insert(0, str(KD.MODEL / "global_model"))
import train_global as TG  # noqa: E402  — same Optuna search space and BASE_PARAMS as the baselines

optuna.logging.set_verbosity(optuna.logging.WARNING)

FEATURES = D.X_VARS + D.ST_VARS
VARIANTS = {
    "A": dict(use_lcs=True, label="raw", weight=1.0),
    "B": dict(use_lcs=False),
    "C": dict(use_lcs=True, label="raw", weight="tune", weight_grid=[0.1, 0.25, 0.5]),
    "D": dict(use_lcs=True, label="rh_corr", weight=1.0, group="Hà Nội"),
    "D_all": dict(use_lcs=True, label="rh_corr", weight=1.0, group="all"),
    "E": dict(use_lcs=True, label="rh_corr", weight="tune", weight_grid=[0.1, 0.25, 0.5], group="Hà Nội"),
}
CV_MODES = ("site", "temporal")
TEMPORAL_CUTOFF = pd.Timestamp("2026-01-01")
OUT, OUT_SMOKE = HERE / "output", HERE / "output_smoke"
OOF_COLS = ["cell_id", "time_lt", "time_utc", "region", "province", "season", "month", "year", "hour_lt",
            "rh_site", "RH", "n_stations_cls", "outer_fold"]


def log(msg: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S}  {msg}", flush=True)


class Labels:
    """Training labels for one variant; for D the LCS labels depend on which collocations are allowed."""

    def __init__(self, data: dict, spec: dict):
        self.spec, self.long, self.colloc = spec, data["long"], data["colloc"]
        self.ls = data["lcs_station"]
        self.raw = self.long.y_raw.to_numpy(float)
        self.is_lcs = (self.long.cls == "LCS").to_numpy()
        self._cache: dict = {}
        if spec.get("label") == "rh_corr":
            key = self.long.loc[self.is_lcs, ["cell_id", "time_lt"]]
            self.lcs_rows = np.where(self.is_lcs)[0]
            self.lcs_key = pd.MultiIndex.from_frame(key)
            g = spec["group"]
            self.in_group = np.ones(len(self.ls), bool) if g == "all" else (self.ls.province == g).to_numpy()

    def pairs(self, cells: set, cutoff: pd.Timestamp | None) -> dict:
        g = self.spec["group"]
        out = {}
        for name, p in self.colloc.items():
            if p["lcs_cell"] in cells and p["kk_cell"] in cells and (g == "all" or p["province"] == g):
                df = p["df"] if cutoff is None else p["df"][p["df"].index < cutoff]
                out[name] = df
        return out

    def y(self, cells: set, cutoff: pd.Timestamp | None) -> tuple[np.ndarray, dict]:
        """Label vector over all long rows + info about the correction used (fitted from `cells` only)."""
        if self.spec.get("label") != "rh_corr":
            return self.raw, {"correction": "none"}
        pairs = self.pairs(cells, cutoff)
        key = tuple(sorted((n, len(d)) for n, d in pairs.items()))
        if key not in self._cache:
            try:
                beta, fits = RC.fit_group(pairs)
            except RC.NotIdentifiable as e:
                self._cache[key] = (self.raw, {"correction": "not_identifiable", "reason": str(e)})
            else:
                c = RC.correct_stations(self.ls, beta, fits, self.in_group)
                cell_mean = c.groupby(["cell_id", "time_lt"]).pm25_corr.mean()
                y = self.raw.copy()
                y[self.lcs_rows] = cell_mean.reindex(self.lcs_key).to_numpy()
                info = {"correction": "fitted", "beta": beta, "factor_per_10RH": float(np.exp(beta)),
                        "pairs": {n: vars(f) for n, f in fits.items()},
                        "lcs_station_hours_corrected": int(c.corr_applied.sum()),
                        "lcs_station_hours_rh_missing": int(c.rh_missing.sum()),
                        "lcs_station_hours_rh_capped": int(c.rh_capped.sum())}
                self._cache[key] = (y, info)
        return self._cache[key]


def split_rows(long: pd.DataFrame, cv: str, k: int, use_lcs: bool) -> tuple[np.ndarray, np.ndarray, pd.Timestamp | None]:
    kk = (long.cls == "KK").to_numpy()
    cls_ok = np.ones(len(long), bool) if use_lcs else kk
    if cv == "site":
        tr = (long.outer_fold != k).to_numpy() & cls_ok
        te = (long.outer_fold == k).to_numpy() & kk
        return np.where(tr)[0], np.where(te)[0], None
    before = (long.time_lt < TEMPORAL_CUTOFF).to_numpy()
    return np.where(before & cls_ok)[0], np.where(~before & kk)[0], TEMPORAL_CUTOFF


def weights(spec: dict, is_lcs: np.ndarray, w: float | None) -> np.ndarray:
    wt = np.ones(len(is_lcs))
    if spec.get("use_lcs"):
        wt[is_lcs] = spec["weight"] if w is None else w
    return wt


def run_split(spec: dict, labels: Labels, X: np.ndarray, long: pd.DataFrame, tr: np.ndarray, te: np.ndarray,
              cutoff, *, n_trials: int, inner_k: int, n_jobs: int, seed: int, base: dict) -> dict:
    cells = long.cell_id.to_numpy()
    is_kk = (long.cls == "KK").to_numpy()
    groups = cells[tr]
    splits = list(GroupKFold(n_splits=min(inner_k, len(np.unique(groups)))).split(tr, groups=groups))
    inner_info = []
    for itr, iva in splits:
        y_in, info = labels.y(set(cells[tr[itr]]), cutoff)
        inner_info.append((y_in, info))

    def objective(trial):
        params = {**TG.suggest_params(trial, n_jobs), **base}
        w = trial.suggest_categorical("lcs_weight", spec["weight_grid"]) if spec.get("weight") == "tune" else None
        scores = []
        for (itr, iva), (y_in, _) in zip(splits, inner_info):
            a, v = tr[itr], tr[iva]
            v = v[is_kk[v]]
            if not len(v):
                continue
            m = lgb.LGBMRegressor(**params).fit(X[a], y_in[a], sample_weight=weights(spec, labels.is_lcs[a], w))
            scores.append(r2_score(y_in[v], m.predict(X[v])))
        return float(np.mean(scores))

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=seed))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    best = dict(study.best_params)
    w = best.pop("lcs_weight", None)
    params = {**TG.BASE_PARAMS, "n_jobs": n_jobs, "bagging_freq": 5, **best, **base}
    y_out, info_out = labels.y(set(cells[tr]), cutoff)
    m = lgb.LGBMRegressor(**params).fit(X[tr], y_out[tr], sample_weight=weights(spec, labels.is_lcs[tr], w))
    pred = m.predict(X[te])
    return dict(pred=pred, best_params=best, lcs_weight=(w if w is not None else spec.get("weight")),
                inner_r2=float(study.best_value), correction=info_out,
                inner_corrections=[i["correction"] for _, i in inner_info],
                importance=TG.gain_pct(m, X.shape[1]))


def git_rev() -> str:
    try:
        return subprocess.run(["git", "-C", str(KD.MODEL), "rev-parse", "--short", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        return "unknown"


def run(variant: str, cv: str, data: dict, *, n_trials: int, inner_k: int, n_jobs: int, folds: list[int],
        out_root: Path, smoke: bool) -> None:
    spec = VARIANTS[variant]
    long = data["long"]
    out = out_root / variant / cv
    (out / "folds").mkdir(parents=True, exist_ok=True)
    base = dict(n_estimators=30) if smoke else {}
    cfg = dict(variant=variant, spec=spec, cv=cv, features=FEATURES, n_trials=n_trials, inner_k=inner_k,
               n_jobs=n_jobs, random_state=D.RANDOM_STATE, temporal_cutoff=str(TEMPORAL_CUTOFF),
               base_params={**TG.BASE_PARAMS, **base}, folds_csv=str(D.FOLDS_CSV), frame_pkl=str(D.FRAME_PKL),
               model_dir=str(KD.MODEL), model_git=git_rev(), command=" ".join(sys.argv), smoke=smoke,
               rh_correction=dict(rh_ref=RC.RH_REF, rh_step=RC.RH_STEP, rh_cap=RC.RH_CAP, f_scale=RC.F_SCALE,
                                  min_hours=[RC.MIN_HOURS, RC.MIN_HOURS_LOW, RC.MIN_HOURS_HIGH]),
               data=dict(rows=len(long), long_mtime=datetime.fromtimestamp(KD.LONG_PKL.stat().st_mtime).isoformat()),
               created=datetime.now().isoformat(timespec="seconds"))
    (out / "config.json").write_text(json.dumps(cfg, indent=2, ensure_ascii=False, default=str))

    labels = Labels(data, spec)
    X = long[FEATURES].to_numpy(np.float32)
    ks = folds if cv == "site" else [0]
    per_fold, parts = {}, []
    for k in ks:
        fj, fz = out / "folds" / f"fold_{k}.json", out / "folds" / f"fold_{k}.npz"
        tr, te, cutoff = split_rows(long, cv, k, spec["use_lcs"])
        if smoke:
            rng = np.random.default_rng(k)
            tr, te = np.sort(rng.choice(tr, min(len(tr), 6000), replace=False)), np.sort(rng.choice(te, min(len(te), 2000), replace=False))
        tag = f"[{variant}/{cv}/fold {k}]"
        if fj.exists():
            info = json.loads(fj.read_text())
            per_fold[k] = info
            if info.get("status") == "done" and fz.exists():
                z = np.load(fz)
                parts.append(long.iloc[z["te"]][OOF_COLS].assign(y=long.y_raw.to_numpy()[z["te"]], pred=z["pred"], fold=k))
            log(f"{tag} already on disk ({info.get('status')})")
            continue
        t0 = time.time()
        if spec.get("label") == "rh_corr":
            _, info0 = labels.y(set(long.cell_id.to_numpy()[tr]), cutoff)
            if info0["correction"] != "fitted":
                info = dict(status="skipped_not_identifiable", reason=info0.get("reason"),
                            n_test_rows=int(len(te)), n_test_cells=int(long.cell_id.iloc[te].nunique()))
                fj.write_text(json.dumps(info, indent=2, ensure_ascii=False))
                per_fold[k] = info
                log(f"{tag} SKIPPED — correction not identifiable from training collocations: {info['reason']}")
                continue
        n_kk_tr = int((long.cls.to_numpy()[tr] == "KK").sum())
        log(f"{tag} train {len(tr):,} rows ({n_kk_tr:,} KK, {len(tr) - n_kk_tr:,} LCS) · "
            f"{long.cell_id.iloc[tr].nunique()} cells · test {len(te):,} KK rows · {long.cell_id.iloc[te].nunique()} cells")
        r = run_split(spec, labels, X, long, tr, te, cutoff, n_trials=n_trials, inner_k=inner_k, n_jobs=n_jobs,
                      seed=D.RANDOM_STATE + k, base=base)
        y_te = long.y_raw.to_numpy()[te]
        info = dict(status="done", **D.metrics(y_te, r["pred"]), inner_r2=r["inner_r2"], best_params=r["best_params"],
                    lcs_weight=r["lcs_weight"], correction=r["correction"], inner_corrections=r["inner_corrections"],
                    n_train_rows=int(len(tr)), n_train_kk=n_kk_tr, n_train_cells=int(long.cell_id.iloc[tr].nunique()),
                    n_test_rows=int(len(te)), n_test_cells=int(long.cell_id.iloc[te].nunique()),
                    seconds=round(time.time() - t0, 1))
        np.savez(fz, te=te, pred=r["pred"], imp=r["importance"])
        fj.write_text(json.dumps(info, indent=2, ensure_ascii=False, default=float))
        per_fold[k] = info
        parts.append(long.iloc[te][OOF_COLS].assign(y=y_te, pred=r["pred"], fold=k))
        corr = info["correction"]
        log(f"{tag} R²={info['R2']:.3f} RMSE={info['RMSE']:.2f} MAE={info['MAE']:.2f} bias={info['bias']:+.2f} "
            f"inner-R²={info['inner_r2']:.3f} w_LCS={info['lcs_weight']}"
            + (f" β={corr['beta']:.4f}" if corr.get("beta") is not None else "") + f" ({info['seconds']:.0f}s)")

    if parts:
        oof = pd.concat(parts, ignore_index=True)
        oof.to_pickle(out / "oof.pkl")
        pooled = D.metrics(oof.y, oof.pred)
        (out / "metrics.json").write_text(json.dumps(dict(variant=variant, cv=cv, pooled=pooled, per_fold=per_fold,
                                                          folds_done=sorted(int(f) for f in oof.fold.unique())),
                                                     indent=2, ensure_ascii=False, default=float))
        log(f"[{variant}/{cv}] POOLED over folds {sorted(oof.fold.unique().tolist())}: R²={pooled['R2']:.3f} "
            f"RMSE={pooled['RMSE']:.2f} MAE={pooled['MAE']:.2f} bias={pooled['bias']:+.2f} slope={pooled['slope']:.3f}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--variant", nargs="+", required=True, choices=list(VARIANTS))
    ap.add_argument("--cv", nargs="+", default=["site", "temporal"], choices=CV_MODES)
    ap.add_argument("--folds", nargs="+", type=int, default=list(range(D.N_OUTER)))
    ap.add_argument("--n-trials", type=int, default=30)
    ap.add_argument("--inner-k", type=int, default=5)
    ap.add_argument("--n-jobs", type=int, default=3)
    ap.add_argument("--smoke", action="store_true", help="2 trials, inner-k 2, 30 trees, subsampled rows → output_smoke/")
    a = ap.parse_args()
    if a.smoke:
        a.n_trials, a.inner_k = 2, 2
    data = KD.build(verbose=False)
    log(f"kk_lcs: variants {a.variant} · cv {a.cv} · folds {a.folds} · {a.n_trials} trials · inner-k {a.inner_k} · "
        f"{a.n_jobs} threads · {len(data['long']):,} rows · model dir {KD.MODEL}")
    for v in a.variant:
        for cv in a.cv:
            run(v, cv, data, n_trials=a.n_trials, inner_k=a.inner_k, n_jobs=a.n_jobs, folds=a.folds,
                out_root=OUT_SMOKE if a.smoke else OUT, smoke=a.smoke)
    log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
