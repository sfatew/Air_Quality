"""Latent Dependency Factor (Gupta et al. 2024, §4.1–4.2) for hourly cell data.

Neighbourhood cloud: for each cell-hour (c, t), the K most similar LABELLED cell-hours of the same hour t
(source cells + target training cells; never cell c itself, never a target test cell), similarity = L2 on
standardized SIM_FEATS (geographical x, y; topographical elev; meteorological ERA5).
Input matrix (K+1) × (p+1): row 0 = objective (its p features, PM set to 0); rows 1..K = neighbours sorted by
similarity, each feature multiplied by its similarity to the objective 1/(1+|Δ|) and the neighbour PM by the
mean of those weights — our reading of "we first calculate the weight for each feature ... similarity (inverse
distance) between the feature of the objective location and neighboring sensors" (§4.2). PM enters as the
z-score of log1p(PM).

Two-stage autoencoder (Fig. 2b): encoder = three 1D-CNN layers over the K+1 rows (kernels 1, 1, 3) and an FNN
to one latent value (the LDF); decoder mirrors it to reconstruct the matrix (encoder–decoder stage);
estimator = one linear unit LDF → objective PM (encoder–estimator stage). The stages alternate every epoch.
"""
import numpy as np
import pandas as pd
import torch
import torch.nn as nn

from common import SIM_FEATS

K_NEIGH = 12                     # paper's choice (§5.3)
EPOCHS, BATCH, LR = 6, 1024, 1e-3
MAX_TRAIN = 300_000


class TwoStageAE(nn.Module):
    def __init__(self, rows: int, cols: int):
        super().__init__()
        self.rows = rows
        self.enc_conv = nn.Sequential(nn.Conv1d(cols, 16, 1), nn.ReLU(), nn.Conv1d(16, 8, 1), nn.ReLU(),
                                      nn.Conv1d(8, 4, 3, padding=1), nn.ReLU())
        self.enc_fc = nn.Sequential(nn.Flatten(), nn.Linear(4 * rows, 16), nn.ReLU(), nn.Linear(16, 1))
        self.dec_fc = nn.Sequential(nn.Linear(1, 16), nn.ReLU(), nn.Linear(16, 4 * rows), nn.ReLU())
        self.dec_conv = nn.Sequential(nn.Conv1d(4, 8, 3, padding=1), nn.ReLU(), nn.Conv1d(8, 16, 1), nn.ReLU(),
                                      nn.Conv1d(16, cols, 1))
        self.est = nn.Linear(1, 1)

    def encode(self, x):                                   # x (B, rows, cols) → (B, 1)
        return self.enc_fc(self.enc_conv(x.transpose(1, 2)))

    def decode(self, z):
        return self.dec_conv(self.dec_fc(z).view(-1, 4, self.rows)).transpose(1, 2)


class LDF:
    def __init__(self, rows: pd.DataFrame, labelled: np.ndarray, device: str = "cuda", seed: int = 0):
        """`rows`: every cell-hour that needs an LDF (source, target train, target test), `labelled`: bool mask
        of rows whose PM may serve as a neighbour / training label."""
        self.dev, self.seed = device, seed
        self.labelled = labelled.astype(bool)
        S = rows[SIM_FEATS].to_numpy(np.float64)
        mu, sd = S[self.labelled].mean(0), S[self.labelled].std(0) + 1e-6
        self.S = torch.as_tensor((S - mu) / sd, dtype=torch.float32, device=device)
        pm = np.log1p(rows.pm25.to_numpy(np.float64))
        pmu, psd = pm[self.labelled].mean(), pm[self.labelled].std()
        pmz = np.where(self.labelled, (pm - pmu) / psd, 0.0)
        self.pmz = torch.as_tensor(pmz, dtype=torch.float32, device=device)
        self.nb = self._neighbours(rows.time_utc.to_numpy(), rows.cell_id.to_numpy())
        self.valid = (self.nb >= 0).all(1)

    def _neighbours(self, t: np.ndarray, cell: np.ndarray) -> torch.Tensor:
        nb = torch.full((len(t), K_NEIGH), -1, dtype=torch.long, device=self.dev)
        codes = np.unique(t)
        order = np.argsort(t, kind="stable")
        bounds = np.r_[np.searchsorted(t[order], codes), len(t)]
        lab_all = torch.as_tensor(self.labelled, device=self.dev)
        cell_t = torch.as_tensor(cell, device=self.dev)
        order_t = torch.as_tensor(order, device=self.dev)
        for g in range(len(codes)):
            q = order_t[bounds[g]:bounds[g + 1]]
            lab = q[lab_all[q]]
            if len(lab) < K_NEIGH + 1:
                continue
            d = torch.cdist(self.S[q], self.S[lab])
            d[cell_t[q][:, None] == cell_t[lab][None, :]] = float("inf")
            v, k = d.topk(K_NEIGH, largest=False)
            ok = torch.isfinite(v).all(1)
            nb[q[ok]] = lab[k[ok]]
        return nb

    def _matrix(self, idx: torch.Tensor) -> torch.Tensor:
        nb = self.nb[idx]
        so, sn = self.S[idx], self.S[nb]                                   # (B,p), (B,K,p)
        w = 1.0 / (1.0 + (sn - so[:, None, :]).abs())
        obj = torch.cat([so, torch.zeros(len(idx), 1, device=self.dev)], 1)[:, None, :]
        nbr = torch.cat([sn * w, (self.pmz[nb] * w.mean(-1))[..., None]], 2)
        return torch.cat([obj, nbr], 1)                                    # (B, K+1, p+1)

    def fit(self, log=print) -> "LDF":
        torch.manual_seed(self.seed)
        g = np.random.default_rng(self.seed)
        train = torch.nonzero(self.valid & torch.as_tensor(self.labelled, device=self.dev)).squeeze(1)
        if len(train) > MAX_TRAIN:
            train = train[torch.as_tensor(g.choice(len(train), MAX_TRAIN, replace=False), device=self.dev)]
        self.net = TwoStageAE(K_NEIGH + 1, self.S.shape[1] + 1).to(self.dev)
        enc = list(self.net.enc_conv.parameters()) + list(self.net.enc_fc.parameters())
        opt_rec = torch.optim.Adam(enc + list(self.net.dec_fc.parameters()) + list(self.net.dec_conv.parameters()), LR)
        opt_est = torch.optim.Adam(enc + list(self.net.est.parameters()), LR)
        mse = nn.MSELoss()
        for ep in range(EPOCHS):
            tot = {"rec": 0.0, "est": 0.0}
            for stage, opt in (("rec", opt_rec), ("est", opt_est)):
                perm = train[torch.randperm(len(train), device=self.dev)]
                for i in range(0, len(perm), BATCH):
                    b = perm[i:i + BATCH]
                    x = self._matrix(b)
                    z = self.net.encode(x)
                    loss = mse(self.net.decode(z), x) if stage == "rec" else mse(self.net.est(z).squeeze(1), self.pmz[b])
                    opt.zero_grad()
                    loss.backward()
                    opt.step()
                    tot[stage] += loss.item() * len(b)
            log(f"    LDF epoch {ep + 1}/{EPOCHS}: recon {tot['rec'] / len(train):.4f} · est {tot['est'] / len(train):.4f}")
        return self

    @torch.no_grad()
    def transform(self) -> np.ndarray:
        out = np.full(len(self.nb), np.nan, np.float32)
        idx = torch.nonzero(self.valid).squeeze(1)
        for i in range(0, len(idx), 16384):
            b = idx[i:i + 16384]
            out[b.cpu().numpy()] = self.net.encode(self._matrix(b)).squeeze(1).cpu().numpy()
        return out
