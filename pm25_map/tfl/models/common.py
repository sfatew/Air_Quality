"""Shared pieces of the TFL experiments: data split, regressor, metrics."""
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "data"))
import build as B  # noqa: E402
import config as C  # noqa: E402

FEATS = B.FEATS
SIM_FEATS = B.SIM_FEATS
RESULTS = HERE.parent / "results"

SOURCES = ["central", "north"]
N_TRAIN = [5, 10, 20, 40]          # target cells with labels (Gupta et al. use 5/7/9/11 of 128 US sensors)
MIN_CELL_HOURS = 1000              # target cells with fewer labelled hours are not used for train or test
SOURCE_MAX_ROWS = 400_000          # random source cell-hours per run (paper: ~249k US samples)

GBR = dict(n_estimators=600, learning_rate=0.05, num_leaves=63, min_child_samples=50, subsample=0.8,
           subsample_freq=1, colsample_bytree=0.8, reg_lambda=1.0, verbosity=-1, random_state=0)


def target_cells(df: pd.DataFrame) -> np.ndarray:
    n = df[df.region == "target"].groupby("cell_id").size()
    return np.sort(n[n >= MIN_CELL_HOURS].index.to_numpy())


def split(df: pd.DataFrame, source: str, n_train: int, rep: int):
    """(source rows, target-train rows, target-test rows). Target train cells depend only on (n_train, rep),
    so both sources are evaluated on the same cells — paired comparison."""
    cells = target_cells(df)
    rng = np.random.default_rng(1000 * n_train + rep)
    tr = rng.choice(cells, n_train, replace=False)
    tgt = df[df.cell_id.isin(cells)]
    src = df[df.region == source]
    if len(src) > SOURCE_MAX_ROWS:
        src = src.sample(SOURCE_MAX_ROWS, random_state=rep)
    return src, tgt[tgt.cell_id.isin(tr)], tgt[~tgt.cell_id.isin(tr)]


def fit(X: np.ndarray, y: np.ndarray, w: np.ndarray | None = None, n_jobs: int = 8) -> lgb.LGBMRegressor:
    return lgb.LGBMRegressor(**GBR, n_jobs=n_jobs).fit(X, y, sample_weight=w)


def metrics(obs: np.ndarray, pred: np.ndarray, cell: np.ndarray) -> dict:
    e = pred - obs
    per_cell = pd.Series(e ** 2).groupby(cell).mean()
    return dict(N=int(len(obs)), cells=int(per_cell.size),
                R2=float(1 - np.sum(e ** 2) / np.sum((obs - obs.mean()) ** 2)),
                RMSE=float(np.sqrt(np.mean(e ** 2))), MAE=float(np.mean(np.abs(e))), bias=float(e.mean()),
                slope=float(np.polyfit(obs, pred, 1)[0]), RMSE_eq_cell=float(np.sqrt(per_cell.mean())))
