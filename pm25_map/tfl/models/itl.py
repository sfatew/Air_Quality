"""Instance transfer learning weights for the source domain (Gupta et al. 2024, §4.3 / §5.2).

    NNW   nearest-neighbour weighting (Loog 2012): a source sample's weight is the number of target samples
          whose nearest source sample it is (Voronoi count), +1 so no source sample is discarded.
    DR    density ratio p_T(x)/p_S(x) from a probabilistic domain classifier — the scalable estimate of the
          quantity KLIEP/KMM target (both are O(n²) and infeasible for ~10⁶ cell-hours).
Weights are rescaled to mean 1 over the source rows; target rows keep weight 1.
"""
import lightgbm as lgb
import numpy as np
import torch


def _standardize(a: np.ndarray, b: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    both = np.vstack([a, b])
    mu, sd = np.nanmean(both, 0), np.nanstd(both, 0) + 1e-6
    return np.nan_to_num((a - mu) / sd), np.nan_to_num((b - mu) / sd)


def nearest(query: np.ndarray, ref: np.ndarray, device: str = "cuda", q_chunk: int = 2048,
            r_chunk: int = 200_000) -> np.ndarray:
    """Index of the nearest `ref` row for every `query` row (exact, chunked on the GPU)."""
    ref_t = torch.as_tensor(ref, dtype=torch.float32, device=device)
    out = np.empty(len(query), np.int64)
    for i in range(0, len(query), q_chunk):
        q = torch.as_tensor(query[i:i + q_chunk], dtype=torch.float32, device=device)
        best_d = torch.full((len(q),), float("inf"), device=device)
        best_i = torch.zeros(len(q), dtype=torch.long, device=device)
        for j in range(0, len(ref_t), r_chunk):
            d = torch.cdist(q, ref_t[j:j + r_chunk])
            v, k = d.min(1)
            upd = v < best_d
            best_d[upd], best_i[upd] = v[upd], k[upd] + j
        out[i:i + q_chunk] = best_i.cpu().numpy()
    return out


def nnw_weights(Xs: np.ndarray, Xt: np.ndarray) -> np.ndarray:
    s, t = _standardize(Xs, Xt)
    counts = np.bincount(nearest(t, s), minlength=len(s)).astype(float) + 1.0
    return counts / counts.mean()


def dr_weights(Xs: np.ndarray, Xt: np.ndarray, seed: int = 0, clip_q: float = 0.99) -> np.ndarray:
    """Cross-fitted (2-fold) classifier; w ∝ p/(1−p) = exp(log-odds), clipped at the 99th percentile.

    Log-odds come from the raw margin, not predict_proba: the regions are nearly separable, probabilities
    saturate and clipping them made every weight identical (ESS = 1)."""
    X = np.vstack([Xs, Xt])
    d = np.r_[np.zeros(len(Xs)), np.ones(len(Xt))]
    fold = np.random.default_rng(seed).integers(0, 2, len(X))
    margin = np.empty(len(X))
    for k in (0, 1):
        clf = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=31, verbosity=-1,
                                 is_unbalance=True, random_state=seed, n_jobs=8)
        clf.fit(X[fold != k], d[fold != k])
        margin[fold == k] = clf.predict(X[fold == k], raw_score=True)
    m = margin[:len(Xs)]
    w = np.exp(m - m.max())
    w = np.minimum(w, np.quantile(w, clip_q))
    return w / w.mean()
