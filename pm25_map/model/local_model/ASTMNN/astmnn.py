"""ASTMNN — Adaptive Spatio-Temporal Multiscale Neural Network (Wang et al. 2024, Int. J. Appl. Earth
Obs. Geoinf. 134:104145; Fig. 2, Table 2, Text S4), viết bằng PyTorch.

Ba nhánh và hàm gộp thích nghi:

    SaM  (không gian) patch w×w × C:
         3 × [Conv 3×3 → ReLU → BN] với 128, 256, 256 kênh → Dropout → [Conv 3×3 → ReLU → BN] 512 kênh
         → spatial attention kiểu CBAM (Woo et al. 2018, Eq. 1 của paper):
               A = σ(Conv7×7([AvgPool_kênh(F); MaxPool_kênh(F)])),   F' = F ⊙ A
         → TB trên toàn patch → 512.
         Khác paper: dùng padding "same" để cùng một mạng chạy được với w = 5, 7, 9. Bảng 2 của paper
         dùng tích chập "valid" cho w = 9 (9 → 7 → 5 → 3), cách đó không áp được cho w = 5.
    TaM  (thời gian) chuỗi t bước × C: 3 lớp BiLSTM (512, 512, 256 đơn vị mỗi chiều)
         → attention Luong (Text S4): score(h_t, h_s) = h_tᵀ W h_s, α = softmax, c_t = Σ α_s h_s,
           h̃ = tanh(W_c [c_t; h_t]) → 256.
    GM   (toàn cục) MLP 128 → 256 → Dropout → 512 trên véc-tơ biến của ô đích + mã hoá vị trí
         (x, y Descartes) và thời gian.
    Gộp  mỗi nhánh qua FC về cùng số chiều D, rồi F = σ(λ1·Fs + λ2·Ft + λ3·Fg) → FC → PM2.5 (z).
         λ = softmax(θ), θ học cùng mạng. Paper: λ1, λ2 ∈ [0, 1], λ3 = 1 − λ1 − λ2; softmax giữ đúng
         ràng buộc đó (cả ba ≥ 0, tổng = 1) mà không cần cắt giá trị.
"""
from __future__ import annotations

import torch
import torch.nn as nn


class ConvBlock(nn.Sequential):
    def __init__(self, c_in: int, c_out: int):
        super().__init__(nn.Conv2d(c_in, c_out, 3, padding=1), nn.ReLU(inplace=True), nn.BatchNorm2d(c_out))


class SpatialAttention(nn.Module):
    def __init__(self, k: int = 7):
        super().__init__()
        self.conv = nn.Conv2d(2, 1, k, padding=k // 2)

    def forward(self, f):
        a = torch.sigmoid(self.conv(torch.cat([f.mean(1, keepdim=True), f.amax(1, keepdim=True)], dim=1)))
        return f * a, a


class SaM(nn.Module):
    def __init__(self, c_in: int, p_drop: float):
        super().__init__()
        self.body = nn.Sequential(ConvBlock(c_in, 128), ConvBlock(128, 256), ConvBlock(256, 256),
                                  nn.Dropout(p_drop), ConvBlock(256, 512))
        self.sa = SpatialAttention(7)

    def forward(self, x):
        f, a = self.sa(self.body(x))
        return f.mean(dim=(2, 3)), a


class LuongAttention(nn.Module):
    def __init__(self, d: int, d_out: int):
        super().__init__()
        self.W = nn.Linear(d, d, bias=False)
        self.Wc = nn.Linear(2 * d, d_out, bias=False)

    def forward(self, H):                       # H: (B, T, d), trạng thái đích h_t = bước cuối
        ht = H[:, -1]
        alpha = torch.softmax(torch.einsum("btd,bd->bt", self.W(H), ht), dim=1)   # h_tᵀ W h_s
        c = torch.einsum("bt,btd->bd", alpha, H)
        return torch.tanh(self.Wc(torch.cat([c, ht], dim=-1))), alpha


class TaM(nn.Module):
    def __init__(self, c_in: int):
        super().__init__()
        self.l1 = nn.LSTM(c_in, 512, batch_first=True, bidirectional=True)
        self.l2 = nn.LSTM(1024, 512, batch_first=True, bidirectional=True)
        self.l3 = nn.LSTM(1024, 256, batch_first=True, bidirectional=True)
        self.att = LuongAttention(512, 256)

    def forward(self, s):
        h, _ = self.l1(s)
        h, _ = self.l2(h)
        h, _ = self.l3(h)
        return self.att(h)


class GM(nn.Module):
    def __init__(self, d_in: int, p_drop: float):
        super().__init__()
        self.net = nn.Sequential(nn.Linear(d_in, 128), nn.ReLU(inplace=True), nn.Linear(128, 256),
                                 nn.ReLU(inplace=True), nn.Dropout(p_drop), nn.Linear(256, 512),
                                 nn.ReLU(inplace=True))

    def forward(self, g):
        return self.net(g)


class ASTMNN(nn.Module):
    def __init__(self, c_in: int, g_in: int, d: int = 256, p_drop: float = 0.3):
        super().__init__()
        self.sam, self.tam, self.gm = SaM(c_in, p_drop), TaM(c_in), GM(g_in, p_drop)
        self.fs, self.ft, self.fg = nn.Linear(512, d), nn.Linear(256, d), nn.Linear(512, d)
        self.theta = nn.Parameter(torch.zeros(3))          # λ = softmax(θ) = 1/3 mỗi nhánh lúc đầu
        self.drop = nn.Dropout(p_drop)
        self.head = nn.Linear(d, 1)

    def branch_weights(self) -> torch.Tensor:
        return torch.softmax(self.theta, dim=0)

    def forward(self, patch, seq, glob):
        fs, _ = self.sam(patch)
        ft, _ = self.tam(seq)
        lam = self.branch_weights()
        f = lam[0] * self.fs(fs) + lam[1] * self.ft(ft) + lam[2] * self.fg(self.gm(glob))
        return self.head(self.drop(torch.relu(f))).squeeze(-1)
