"""Lọc và chuẩn hoá CSV AQI thô của Envisoft: raw → process.

Đọc từng file trạm trong `/home/slow_data/Air_Quality/AQI/raw` (do
`Data_Collection/AQI/crawl.py` ghi ra), làm sạch, rồi ghi sang
`/home/slow_data/Air_Quality/AQI/process`.

Các bộ lọc, theo đúng thứ tự — mỗi bộ lọc giải quyết vấn đề gì
------------------------------------------------------------------
  (1) Timestamp không parse được → bỏ dòng.
      Vấn đề: dòng hỏng định dạng không gán được giờ/ngày.
  (2) Timestamp trùng trong cùng trạm → giữ bản ghi đầu.
      Vấn đề: crawl chồng lấn ghi một giờ hai lần → đếm giờ và trung bình ngày bị lệch.
  (3) PM2.5 rỗng → bỏ dòng.
      Vấn đề: giờ không đo PM2.5 (vd. trạm Lăng Bác chỉ đo khí) không được tính là giờ có số liệu.
  (4) TRẠM-THÁNG hỏng: tháng có > BAD_MONTH_FRAC (50%) số giờ PM2.5 < 3 → bỏ CẢ THÁNG.
      Vấn đề: cảm biến hỏng báo về ~0 thành GIAI ĐOẠN dài (Tây Ninh Phường 3: 100% giờ < 3
      từ 2022Q4). Nếu chỉ cắt giờ < 3 như bước (6), vài giờ 3–6 µg/m³ của chính cảm biến
      hỏng vẫn lọt qua → ngày "thiếu giờ" có trung bình sát sàn (trung vị 4,1 µg/m³, ~6 giờ/ngày,
      so với 11,2 µg/m³ ở tháng bình thường của cùng các trạm). Lần chạy 22/10/2026: 513 trạm-tháng,
      38 trạm; 2 trạm (Gia Lai Diên Phú, Thái Nguyên Quang Sơn) mất toàn bộ.
  (4b) TRẠM-NGÀY hỏng một phần: ngày có > BAD_DAY_FRAC (20%) số giờ PM2.5 < 3 VÀ trung bình
      các giờ còn lại < BAD_DAY_REST_MEAN (10 µg/m³) → bỏ CẢ NGÀY. Nếu các giờ còn lại có mức
      bình thường thì chỉ bỏ các giờ < 3 (bước 6), giữ phần còn lại.
      Vấn đề: trạm trôi về 0 giữa ngày trong tháng chưa đủ hỏng để bị (4) bắt. 68% ngày
      "thiếu giờ" thật ra có ≥ 18 giờ trong raw — giờ < 3 bị cắt, giờ còn lại cũng thấp
      (0,41–0,79 lần trạm lân cận 100 km cùng ngày; ngày không có giờ < 3: 1,02).
      Vì sao có điều kiện "mức giờ còn lại": quy tắc chỉ-tỉ-lệ (bản trước) xoá oan ~880 ngày
      mà phần còn lại khớp lân cận như ngày tốt (TB ≥ 10: 0,92 lần lân cận, 11% < 0,5 lần —
      đối chứng ngày sạch 1,02 / 14%); ngày TB còn lại < 6 thì 69% < 0,5 lần lân cận (hỏng thật).
      Điều kiện không dùng trạm lân cận nên phép kiểm bằng lân cận vẫn độc lập.
  (5) Đoạn KẸT: ≥ STUCK_MIN_HOURS (6) bản ghi liên tiếp, cách nhau ≤ 1 giờ, mỗi bước đổi
      ≤ STUCK_TOL (0,05 µg/m³) VÀ biên độ toàn đoạn ≤ STUCK_RANGE_MAX (0,25) → bỏ đoạn.
      Vấn đề: cảm biến treo một giá trị (Quảng Ninh Cái Lân 16/11/2022: 267 giờ chỉ 9 giá trị,
      phần lớn 8,318). Số liệu có 4 chữ số lẻ nên không phải do làm tròn; hai trạm tốt nhất
      Hà Nội có 0 đoạn như vậy trên > 74.000 giờ. Rào biên độ 0,25 (> p99 = 0,21 của các đoạn
      kẹt thật) chặn chuỗi trôi chậm nhiều bước nhỏ; ngưỡng chặt hơn (0,05) sẽ bỏ sót ~850 đoạn.
  (6) PM2.5 ngoài [PM25_MIN_VALID, PM25_MAX_VALID] = [3, 350] µg/m³ → bỏ giờ.
      Vấn đề: < 3 = trôi điểm không của cảm biến; > 350 = bão hoà / lỗi thiết bị.
  (7) GIAI ĐOẠN không bám trạm lân cận (dò điểm gãy, chạy sau khi mọi trạm qua 1–6):
      chuỗi ngày r = log(trạm / trung vị trạm lân cận ≤ 100 km, ≥ 2 trạm có ngày đủ 18 giờ),
      tách cứng ở khoảng trống > 60 ngày, rồi tách điểm gãy trung bình (binary segmentation,
      đoạn ≥ 30 ngày, phạt 3·σ²·ln n). Bỏ cả đoạn (≥ 10 ngày) nếu:
        cp_low  — TB < 0,5 lần lân cận, biến thiên < 0,6 lần lân cận, TB trạm < 8 µg/m³;
        cp_flat — biến thiên < 0,25 lần lân cận và tương quan log < 0,3.
      Vấn đề: lỗi mà bước 1–6 không thấy vì giá trị không < 3 và không kẹt tuyệt đối —
      trạm "bám sàn" ngay trên 3 (Trà Vinh Tp. đứng ~4,3 µg/m³ khi lân cận 8–21) và trạm "đường
      phẳng có nhiễu" (Khe Chàm 2022: 26,8–27,1 µg/m³ cả ngày, cao gấp 2–3 lần lân cận).
      Mốc dựng LẶP: đoạn đã gắn cờ của mọi trạm bị bỏ khỏi dữ liệu làm mốc rồi dò lại tới khi
      cờ không đổi (trạm lân cận hỏng không kéo lệch mốc; chạy một phần = chạy toàn bộ).
      Cần HAI điều kiện (lệch mức VÀ không bám lân cận) vì mốc lân cận đổi thành phần theo thời
      gian: chỉ dùng tỉ lệ thì trạm tốt Hà Nội 556 cũng dao động 0,5–3,4 lần lân cận.
      Kiểm chứng độc lập bằng dữ liệu giờ (không dùng trong quy tắc): đoạn cp_low có 41% giờ
      < 4 µg/m³ và CV trong ngày 0,12 (đoạn khác: 1,1% và 0,35); đoạn cp_flat có CV ngày 0,011.
  Đoạn bị bỏ ở (4), (4b), (5), (7) ghi vào QC_DIR/removed_segments.csv (cột kind = bad_month /
  bad_day / stuck / cp_low / cp_flat; n_kept_if_no_qc = số giờ ≥ 3 mà bộ lọc chỉ-ngưỡng sẽ để lọt).
  Kiểm chứng: sau QC, ngày đủ 24 giờ vẫn bằng 1,02 lần lân cận (không làm méo dữ liệu tốt);
  trạm tốt (Hà Nội 556 Nguyễn Văn Cừ) mất < 1% số giờ.

Schema đầu ra
-------------
    datetime, pm2.5, CO, NOx, O3, SO2

Chỉ PM2.5 bị bỏ khi rỗng; NaN của CO/NOx/O3/SO2 được giữ nguyên để EDA
báo cáo được độ phủ từng chất.

Cách dùng
---------
    python filter_aqi.py
    python filter_aqi.py --overwrite       # xử lý lại cả trạm đã có file
    python filter_aqi.py --station "Hà Nội 556 Nguyễn Văn Cừ (KK)"
    python filter_aqi.py --out-dir /tmp/thu_nghiem   # ghi ra chỗ khác để kiểm tra
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- paths
AQI_DIR     = Path("/home/slow_data/Air_Quality/AQI/raw")
OUT_DIR     = Path("/home/slow_data/Air_Quality/AQI/process")
STATION_MAP = Path("/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv")

# --------------------------------------------------------------------------- config
# Các chất giữ lại bên cạnh PM2.5. KHÔNG bỏ dòng khi chúng rỗng — để nguyên
# cho EDA hạ nguồn báo cáo độ phủ.
EXTRA_POLLUTANTS = ["CO", "NOx", "O3", "SO2"]

# Khoảng hợp lý của PM2.5 (µg/m³).
PM25_MIN_VALID = 3.0
PM25_MAX_VALID = 350.0

# Trạm-tháng hỏng: tỉ lệ giờ < PM25_MIN_VALID vượt ngưỡng này thì bỏ cả tháng.
BAD_MONTH_FRAC = 0.5
# Trạm-ngày hỏng một phần: ngày có > BAD_DAY_FRAC số giờ < PM25_MIN_VALID VÀ trung bình các giờ
# còn lại (trong [PM25_MIN_VALID, PM25_MAX_VALID]) < BAD_DAY_REST_MEAN thì bỏ cả ngày;
# nếu các giờ còn lại có mức bình thường thì chỉ bỏ các giờ < 3 (bước 6).
BAD_DAY_FRAC = 0.2
BAD_DAY_REST_MEAN = 10.0
# Cảm biến kẹt: từng bước đổi ≤ STUCK_TOL VÀ biên độ toàn đoạn ≤ STUCK_RANGE_MAX.
STUCK_MIN_HOURS = 6
STUCK_TOL = 0.05
STUCK_RANGE_MAX = 0.25
# Dò điểm gãy (bước 7, chạy sau khi mọi trạm đã qua bước 1–6): chuỗi ngày
# r = log(trạm / trung vị trạm lân cận) tách đoạn bằng binary segmentation (gãy trung bình).
CP_RADIUS_KM = 100          # bán kính trạm lân cận
CP_MIN_NB = 2               # số trạm lân cận tối thiểu có ngày đủ giờ để có mốc
CP_NB_HOURS = 18            # ngày của trạm lân cận phải có ≥ ngần này giờ
CP_ST_HOURS = 8             # ngày của trạm đang xét phải có ≥ ngần này giờ
CP_MIN_SEG = 30             # đoạn tối thiểu (số ngày có mốc) khi tách
CP_GAP_DAYS = 60            # khoảng trống dữ liệu dài hơn → tách cứng (có thể đã thay thiết bị)
CP_PENALTY = 3.0            # β = CP_PENALTY · σ² · ln(n), σ ước từ MAD của sai phân r
CP_MIN_FLAG_DAYS = 10       # đoạn ngắn hơn không đủ tin để gắn cờ
CP_MAX_ITER = 5             # số vòng lặp dựng lại mốc tối đa
# cp_low  — thấp, bị nén, sát sàn: tỉ lệ < 0,5 · biến thiên < 0,6 lần lân cận · TB trạm < 8 µg/m³
CP_LOW_RATIO, CP_LOW_SD, CP_LOW_MEAN = 0.5, 0.6, 8.0
# cp_flat — phẳng, không bám lân cận: biến thiên < 0,25 lần lân cận · tương quan log < 0,3
CP_FLAT_SD, CP_FLAT_R = 0.25, 0.3
QC_DIR = Path("/home/slow_data/Air_Quality/AQI/qc")
QC_LOG: list[dict] = []

# Trạm loại hẳn sau rà soát từng trạm (23/10/2026, mức 1 trong AQI/qc/station_scorecard.csv).
# Không ghi ra process/ và không làm mốc lân cận cho bước 7.
EXCLUDED_STATIONS = {
    "Quảng Ninh: Văn phòng C.ty than Khe Chàm - Tp Cẩm Phả (KK)": "97% giờ < 4 µg/m³, 64% ngày phẳng — bám sàn cả năm 2025",
    "Quảng Ninh: phường Cao Xanh - Hạ Long (KK)": "61% ngày phẳng (CV < 0,03), r = 0,08 với lân cận",
    "Quảng Ninh: Phường Cẩm Thịnh - Cẩm Phả (KK)": "24% ngày phẳng, r = −0,01 với lân cận, bước nhảy mức năm ×2,9",
    "Trà Vinh: xã Dân Thành, TX Duyên Hải (KK)": "60% ngày phẳng, r ≈ 0 với mọi trạm miền Nam",
    "Sóc Trăng: Sở TNMT - 16 Hùng Vương, P.6, Tp. Sóc Trăng (KK)": "r = −0,10–0,34 với mọi trạm miền Nam",
    "Thái Bình: xã Thái Thọ, huyện Thái Thụy (KK)": "r = 0,06–0,22 với mọi trạm quanh (các trạm đó với nhau 0,77–0,91)",
    "Tây Ninh: Phường 3 - TP. Tây Ninh (KK)": "trạm hỏng nặng: QC loại 98%, phần còn lại 29 ngày",
    "Vĩnh Long: UBND tỉnh, đường Hoàng Thái Hiếu (KK)": "trạm hỏng nặng: QC loại 96%, phần còn lại 39 ngày",
    "Nam Định: Số 2 Trần Thánh Tông - P. Quang Trung (VT1) (KK)": "trạm hỏng nặng: QC loại 57%, phần còn lại 44 ngày, 0,36× lân cận",
}


# --------------------------------------------------------------------------- station lookup
def _normalize(name: str) -> str:
    """Master dùng 'Tỉnh: địa chỉ'; tên file thô bỏ dấu hai chấm.
    Gộp luôn các khoảng trắng liên tiếp — vài tên file có hai dấu cách."""
    return " ".join(name.replace(":", "").split()).strip().lower()


def load_known_stations(map_csv: Path = STATION_MAP) -> set[str]:
    """Tập khoá chuẩn hoá của các trạm có trong master."""
    df = pd.read_csv(map_csv)
    return {_normalize(n) for n in df["stationName"]}


# --------------------------------------------------------------------------- QC giai đoạn
def bad_months(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """(mask dòng thuộc trạm-tháng hỏng, tỉ lệ giờ < PM25_MIN_VALID theo tháng)."""
    month = df["datetime"].dt.to_period("M")
    frac = (df["pm2.5"] < PM25_MIN_VALID).groupby(month).mean()
    return month.map(frac).gt(BAD_MONTH_FRAC), frac


def stuck_rows(df: pd.DataFrame) -> np.ndarray:
    """Mask các dòng thuộc đoạn kẹt (≥ STUCK_MIN_HOURS bản ghi liên tiếp gần như không đổi)."""
    v = df["pm2.5"].to_numpy()
    if len(v) < STUCK_MIN_HOURS:
        return np.zeros(len(v), bool)
    gap = df["datetime"].diff().dt.total_seconds().to_numpy()
    same = np.r_[False, (np.abs(np.diff(v)) <= STUCK_TOL) & (gap[1:] <= 3600)]
    gid = np.cumsum(~same)
    run = np.bincount(gid)[gid]
    rng = pd.Series(v).groupby(gid).transform(lambda x: x.max() - x.min()).to_numpy()
    return (run >= STUCK_MIN_HOURS) & (rng <= STUCK_RANGE_MAX) & (v >= PM25_MIN_VALID)


def _binseg(y: np.ndarray, beta: float, min_seg: int) -> list[int]:
    """Điểm gãy trung bình bằng binary segmentation (chi phí bình phương). Trả chỉ số đầu các đoạn sau."""
    out: list[int] = []

    def rec(a: int, b: int) -> None:
        n = b - a
        if n < 2 * min_seg:
            return
        cs, cs2 = np.cumsum(y[a:b]), np.cumsum(y[a:b] ** 2)
        k = np.arange(min_seg, n - min_seg + 1)
        left = cs2[k - 1] - cs[k - 1] ** 2 / k
        right = (cs2[-1] - cs2[k - 1]) - (cs[-1] - cs[k - 1]) ** 2 / (n - k)
        gain = (cs2[-1] - cs[-1] ** 2 / n) - (left + right)
        j = int(np.argmax(gain))
        if gain[j] > beta:
            out.append(a + int(k[j])); rec(a, a + int(k[j])); rec(a + int(k[j]), b)
    rec(0, len(y))
    return sorted(out)


def changepoint_qc(out_dir: Path, targets: set[str], map_csv: Path = STATION_MAP) -> None:
    """Bước 7: gắn cờ và bỏ các GIAI ĐOẠN trạm không bám theo trạm lân cận (cp_low / cp_flat).

    Chạy sau bước 1–6 của mọi trạm vì cần mốc lân cận; mọi file trong out_dir làm mốc, nhưng
    chỉ trạm trong `targets` (vừa được ghi lại ở lần chạy này) bị cắt — chạy lại không cắt chồng.
    Mốc = trung vị trạm lân cận, dựng LẶP: các đoạn đã gắn cờ của mọi trạm được bỏ khỏi dữ liệu
    làm mốc rồi dò lại, tới khi cờ không đổi — để trạm lân cận hỏng (vd. đường phẳng ở mức cao)
    không kéo lệch mốc của trạm khác, và chạy một phần cho cùng kết quả với chạy toàn bộ.
    """
    m = pd.read_csv(map_csv)
    m["k"] = m["stationName"].map(_normalize)
    m = m.drop_duplicates("k").set_index("k")
    files = {p.stem: p for p in out_dir.glob("*.csv") if _normalize(p.stem) in m.index}
    daily, nh = {}, {}
    for s, p in files.items():
        d = pd.read_csv(p, usecols=["datetime", "pm2.5"], parse_dates=["datetime"])
        g = d.groupby(d["datetime"].dt.normalize())["pm2.5"].agg(["mean", "size"])
        daily[s], nh[s] = g["mean"], g["size"]
    D, N = pd.DataFrame(daily), pd.DataFrame(nh).fillna(0)
    full = D.where(N >= CP_NB_HOURS)
    st = list(D.columns)
    lat = np.array([m.loc[_normalize(s), "latitude"] for s in st])
    lon = np.array([m.loc[_normalize(s), "longitude"] for s in st])
    xy = np.c_[lon * np.cos(np.radians(lat.mean())), lat] * 111.0
    from scipy.spatial import cKDTree
    tree = cKDTree(xy)
    nbs = {s: [st[k] for k in tree.query_ball_point(xy[i], CP_RADIUS_KM) if k != i] for i, s in enumerate(st)}

    def detect(s: str, ref_data: pd.DataFrame) -> list[tuple]:
        nb = nbs[s]
        if not nb:
            return []
        ref = ref_data[nb].median(axis=1).where(ref_data[nb].notna().sum(axis=1) >= CP_MIN_NB)
        x = D[s].where(N[s] >= CP_ST_HOURS)
        ok = x.notna() & ref.notna() & (x > 0) & (ref > 0)
        if ok.sum() < CP_MIN_FLAG_DAYS:
            return []
        t = x.index[ok]
        lx, lr = np.log(x[ok].to_numpy()), np.log(ref[ok].to_numpy())
        r = lx - lr
        dr = np.diff(r)
        sig = 1.4826 * np.median(np.abs(dr - np.median(dr))) / np.sqrt(2) if len(dr) else 0.0
        beta = CP_PENALTY * sig ** 2 * np.log(len(r))
        cuts = [0] + [j for j in range(1, len(t)) if (t[j] - t[j - 1]).days > CP_GAP_DAYS] + [len(t)]
        flagged = []
        for a, b in zip(cuts[:-1], cuts[1:]):
            bps = [a] + [a + q for q in _binseg(r[a:b], beta, CP_MIN_SEG)] + [b]
            for u, v in zip(bps[:-1], bps[1:]):
                if v - u < CP_MIN_FLAG_DAYS:
                    continue
                ratio = float(np.exp(np.median(r[u:v])))
                sd_ratio = float(np.std(lx[u:v]) / np.std(lr[u:v])) if np.std(lr[u:v]) > 0 else np.nan
                corr = float(np.corrcoef(lx[u:v], lr[u:v])[0, 1]) if np.std(lx[u:v]) > 0 else 0.0
                mean_st = float(np.exp(lx[u:v]).mean())
                kind = ("cp_low" if ratio < CP_LOW_RATIO and sd_ratio < CP_LOW_SD and mean_st < CP_LOW_MEAN
                        else "cp_flat" if sd_ratio < CP_FLAT_SD and corr < CP_FLAT_R else None)
                if kind:
                    flagged.append((kind, t[u], t[v - 1], v - u, ratio, sd_ratio, corr, mean_st))
        return flagged

    flags: dict[str, list[tuple]] = {}
    for it in range(1, CP_MAX_ITER + 1):
        ref_data = full.copy()
        for s, fl in flags.items():
            for f in fl:
                ref_data.loc[f[1]:f[2], s] = np.nan
        new = {s: fl for s in st if s in targets and (fl := detect(s, ref_data))}
        if new == flags:
            break
        flags = new
    print(f"Bước 7 (điểm gãy): {it} vòng lặp{'' if new == flags else ' — CHƯA hội tụ, dùng vòng cuối'}")

    n_seg = 0
    for s, flagged in flags.items():
        df = pd.read_csv(files[s], parse_dates=["datetime"])
        drop = np.zeros(len(df), bool)
        for kind, t0, t1, nd, ratio, sd_ratio, corr, mean_st in flagged:
            sel = (df["datetime"] >= t0) & (df["datetime"] < t1 + pd.Timedelta(days=1))
            QC_LOG.append({"station": s, "kind": kind, "start": t0, "end": t1 + pd.Timedelta(hours=23),
                           "n_rows": int(sel.sum()), "n_kept_if_no_qc": int(sel.sum()),
                           "value": round(mean_st, 3), "cp_ratio": round(ratio, 3),
                           "cp_sd_ratio": round(sd_ratio, 3), "cp_corr": round(corr, 3), "cp_days": nd})
            drop |= sel.to_numpy()
            n_seg += 1
        df = df[~drop]
        if len(df):
            df.to_csv(files[s], index=False)
        else:
            files[s].unlink()
    print(f"Bước 7 (điểm gãy): {n_seg} đoạn bị bỏ")


# --------------------------------------------------------------------------- per-station
def process_station(src: Path, out_dir: Path, known: set[str],
                    *, overwrite: bool = False) -> str:
    """Chạy 4 bước làm sạch cho một trạm.

    Trả về một dòng trạng thái, luôn kèm bộ đếm
    ``n_in → n_dedup → n_pm_na → n_pm_range`` để nhìn log là biết dữ liệu
    rụng ở bước nào, chứ không chỉ biết tổng số dòng cuối.
    """
    out_path = out_dir / src.name
    if _normalize(src.stem) in {_normalize(k) for k in EXCLUDED_STATIONS}:
        if out_path.exists():
            out_path.unlink()
        return "excluded (loại sau rà soát trạm — EXCLUDED_STATIONS)"
    if out_path.exists() and not overwrite:
        return "skip (đã có file)"

    if _normalize(src.stem) not in known:
        return "skip (không có trong master station map)"

    # Dò header trước để chỉ yêu cầu các cột thật sự tồn tại; chất nào trạm
    # không đo thì thành cột toàn NaN, giữ schema đầu ra ổn định.
    try:
        header = pd.read_csv(src, nrows=0).columns.tolist()
    except Exception:
        return "skip (không đọc được CSV)"
    if "Timestamp" not in header or "PM2.5" not in header:
        return "skip (thiếu cột Timestamp/PM2.5)"

    wanted    = ["Timestamp", "PM2.5", *EXTRA_POLLUTANTS]
    available = [c for c in wanted if c in header]
    df = pd.read_csv(src, usecols=available, low_memory=False)
    for c in EXTRA_POLLUTANTS:
        if c not in df.columns:
            df[c] = np.nan

    df = df.rename(columns={"Timestamp": "datetime", "PM2.5": "pm2.5"})
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["pm2.5"]    = pd.to_numeric(df["pm2.5"],    errors="coerce")
    for c in EXTRA_POLLUTANTS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # ---- Bước 1: bỏ dòng có timestamp không parse được
    df = df.dropna(subset=["datetime"]).sort_values("datetime")
    n_in = len(df)
    if n_in == 0:
        return "skip (rỗng sau khi parse timestamp)  n_in=0"

    # ---- Bước 2: bỏ timestamp trùng trong cùng trạm (giữ bản ghi đầu)
    df = df.drop_duplicates(subset=["datetime"], keep="first")
    n_dedup = len(df)

    # ---- Bước 3: bỏ PM2.5 rỗng
    df = df.dropna(subset=["pm2.5"])
    n_pm_na = len(df)

    # ---- Bước 4: bỏ trạm-tháng hỏng
    bad, frac = bad_months(df)
    for m, f in frac[frac > BAD_MONTH_FRAC].items():
        sel = df["datetime"].dt.to_period("M") == m
        QC_LOG.append({"station": src.stem, "kind": "bad_month", "start": df.loc[sel, "datetime"].min(),
                       "end": df.loc[sel, "datetime"].max(), "n_rows": int(sel.sum()),
                       "n_kept_if_no_qc": int((sel & df["pm2.5"].between(PM25_MIN_VALID, PM25_MAX_VALID)).sum()),
                       "frac_lt3": round(float(f), 3)})
    df = df[~bad]
    n_month = len(df)

    # ---- Bước 4b: bỏ trạm-ngày hỏng một phần
    day = df["datetime"].dt.normalize()
    in_range = df["pm2.5"].between(PM25_MIN_VALID, PM25_MAX_VALID)
    dfrac = (df["pm2.5"] < PM25_MIN_VALID).groupby(day).mean()
    rest = df["pm2.5"].where(in_range).groupby(day).mean()          # NaN nếu không còn giờ nào
    bad_day = (dfrac > BAD_DAY_FRAC) & ~(rest >= BAD_DAY_REST_MEAN)
    for dd in bad_day[bad_day].index:
        sel = day == dd
        QC_LOG.append({"station": src.stem, "kind": "bad_day", "start": df.loc[sel, "datetime"].min(),
                       "end": df.loc[sel, "datetime"].max(), "n_rows": int(sel.sum()),
                       "n_kept_if_no_qc": int((sel & in_range).sum()),
                       "frac_lt3": round(float(dfrac[dd]), 3), "value": round(float(rest[dd]), 3)})
    df = df[~day.map(bad_day).astype(bool)]
    n_day = len(df)

    # ---- Bước 5: bỏ đoạn kẹt
    st = stuck_rows(df)
    if st.any():
        gid = np.cumsum(np.r_[True, st[1:] != st[:-1]])
        for g in np.unique(gid[st]):
            seg = df[gid == g]
            QC_LOG.append({"station": src.stem, "kind": "stuck", "start": seg["datetime"].min(),
                           "end": seg["datetime"].max(), "n_rows": len(seg),
                           "n_kept_if_no_qc": len(seg), "value": round(float(seg["pm2.5"].median()), 3)})
    df = df[~st]
    n_stuck = len(df)

    # ---- Bước 6: bỏ PM2.5 ngoài khoảng hợp lý
    df = df[df["pm2.5"].between(PM25_MIN_VALID, PM25_MAX_VALID)]
    n_pm_range = len(df)

    counters = f"n: {n_in:,}→{n_dedup:,}→{n_pm_na:,}→tháng {n_month:,}→ngày {n_day:,}→kẹt {n_stuck:,}→{n_pm_range:,}"

    # Không còn dòng nào thì không ghi file rỗng (đây là tránh file trống,
    # không phải ngưỡng số mẫu — trạm còn 1 dòng hợp lệ vẫn được ghi).
    if n_pm_range == 0:
        if overwrite and out_path.exists():
            out_path.unlink()
            return f"drop (0 dòng sau lọc, đã xoá file cũ)  {counters}"
        return f"skip (0 dòng sau lọc)  {counters}"

    df = df[["datetime", "pm2.5", *EXTRA_POLLUTANTS]]
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return f"ok (ghi {len(df):,} dòng)  {counters}"


# --------------------------------------------------------------------------- CLI
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--aqi-dir",     type=Path, default=AQI_DIR)
    ap.add_argument("--out-dir",     type=Path, default=OUT_DIR)
    ap.add_argument("--station-map", type=Path, default=STATION_MAP)
    ap.add_argument("--overwrite",   action="store_true",
                    help="xử lý lại cả trạm đã có file đầu ra")
    ap.add_argument("--station",     action="append", default=None,
                    help="chỉ xử lý trạm có tên file này (lặp lại được)")
    args = ap.parse_args(argv)

    known = load_known_stations(args.station_map)
    files = sorted(p for p in args.aqi_dir.glob("*.csv") if p.is_file())
    if args.station:
        wanted = {s.lower() for s in args.station}
        files = [f for f in files if f.stem.lower() in wanted]

    if not files:
        print("Không tìm thấy CSV đầu vào.", file=sys.stderr)
        return 1

    print(f"Xử lý {len(files)} trạm  "
          f"(pm2.5 trong [{PM25_MIN_VALID:g}, {PM25_MAX_VALID:g}], "
          f"không có ngưỡng số dòng)  ->  {args.out_dir}")

    counts: dict[str, int] = {}
    written: set[str] = set()
    for fp in files:
        status = process_station(fp, args.out_dir, known, overwrite=args.overwrite)
        tag = status.split(" ", 1)[0]
        counts[tag] = counts.get(tag, 0) + 1
        if tag == "ok":
            written.add(fp.stem)
        print(f"  [{status:60s}] {fp.name}")

    summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    print(f"Xong bước 1–6. {summary}")
    if written:
        changepoint_qc(args.out_dir, written, args.station_map)
    if QC_LOG:
        QC_DIR.mkdir(parents=True, exist_ok=True)
        log = pd.DataFrame(QC_LOG)
        old = QC_DIR / "removed_segments.csv"
        if args.station and old.exists():        # chạy một phần: chỉ thay dòng của trạm vừa chạy lại
            prev = pd.read_csv(old)
            log = pd.concat([prev[~prev["station"].isin(written)], log], ignore_index=True)
        log.to_csv(old, index=False)
        print(f"Đoạn bị bỏ: {log.groupby('kind').n_rows.agg(['size', 'sum']).to_dict('index')}"
              f"  ->  {QC_DIR / 'removed_segments.csv'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
