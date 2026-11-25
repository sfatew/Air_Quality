"""Gộp PM2.5 của các trạm về từng ô lưới 0,05°, theo ngày hoặc theo giờ (`--hourly`).

    "Measurements from multiple monitoring sites located in one grid cell
     are averaged."  — Li et al. 2020, ISPRS JPRS 167:178-188, §2.4

Luồng gộp hai tầng:

    PM2.5 theo giờ (AQI/process/<trạm>.csv, giờ địa phương)
      → theo (trạm, ngày)  : trung bình các giờ có số liệu
      → theo (ô, ngày)     : trung bình các TRẠM nằm trong ô

Bản `--hourly` bỏ tầng ngày: (trạm, giờ) → (ô, giờ) = trung bình các trạm có số liệu
giờ đó. Nhãn giờ giữ nguyên như file Envisoft (giờ địa phương) và là giờ KẾT THÚC:
dòng 10:00 = TB [09:00, 10:00) — xác nhận 29/10/2026.

Trung bình tầng hai không trọng số, tức mỗi trạm trong ô đóng góp như nhau
bất kể hôm đó nó đo được bao nhiêu giờ — đúng cách đọc thông thường của câu
trên. Cột `pm25_hour_weighted` giữ phương án còn lại (gộp thẳng mọi giờ của
mọi trạm trong ô) để đối chiếu độ nhạy; hai số chỉ lệch nhau khi các trạm
cùng ô có độ phủ giờ khác nhau.

Lưới lấy từ `divide_grid.py` — cùng một hộp bao và bước lưới với Stage A,
nên `cell_id` ở đây ghép thẳng được với lưới AOD mà không cần nội suy.

Không có ngưỡng số giờ tối thiểu cho một ngày-trạm: `--min-hours` mặc định 0.
Mỗi dòng mang theo `n_hours_total` và `n_hours_min` để lọc ở phía dùng dữ
liệu, thay vì chôn ngưỡng vào bước sinh dữ liệu.

Đầu ra
------
    output/aqi_pm25_grid_daily.csv   một dòng = (ô, ngày)
    output/aqi_pm25_grid_cells.csv   bảng tra ô → danh sách trạm
    output/aqi_pm25_grid_hourly.pkl  một dòng = (ô, giờ)  — chỉ với --hourly

Cách dùng
---------
    python map_aqi_grid.py
    python map_aqi_grid.py --min-hours 12
    python map_aqi_grid.py --start 2024-01-01 --end 2025-12-31
    python map_aqi_grid.py --hourly
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import divide_grid as DG  # noqa: E402  — nguồn duy nhất định nghĩa lưới

REPO = DG.find_repo()
AQI_DIR = Path("/home/slow_data/Air_Quality/AQI/process")
STATION_MAP = REPO / "Masterdata" / "envisoft_station_map.csv"
GPKG = REPO / "pm25_map" / "output" / "vn_grid_005deg.gpkg"

# Đầu ra nằm cạnh dữ liệu nguồn trên slow_data, không nằm trong repo: bảng này
# là DỮ LIỆU (vài MB, sinh lại được), không phải mã nguồn.
# Lưu ý: cùng thư mục với AQI_DIR, nơi script quét CSV từng trạm. Không va nhau
# vì việc quét khớp theo tên trạm trong master, mà tên file đầu ra thì không
# trùng tên trạm nào — xem station_cells().
OUT_DIR = Path("/home/slow_data/Air_Quality/AQI/process")

# Tên trong master có dấu hai chấm, tên file thì không; chuẩn hoá về chữ và số.
_KEY_RE = re.compile(r"(?:[^\w]|_)+", flags=re.UNICODE)


def _norm(name: str) -> str:
    return _KEY_RE.sub("", str(name).lower())


def sensor_class(name: str) -> str:
    """Hậu tố tên trạm cho biết loại thiết bị: (KK) trạm chuẩn, (LCS) cảm biến giá rẻ.

    Phân biệt cái này quan trọng khi gộp: ô trộn hai loại có độ lệch giữa các
    trạm cao gấp đôi ô thuần một loại, tức một phần "biến thiên không gian"
    trong ô thật ra là sai khác giữa thiết bị. Cột `sensor_mix` ở bảng tra ô
    để lọc được chỗ đó về sau.
    """
    m = re.search(r"\((KK|LCS)\)\s*$", str(name))
    return m.group(1) if m else "OTHER"


# --------------------------------------------------------------------------- ô của trạm
def station_cells(spec: dict) -> pd.DataFrame:
    """Master station map + chỉ số ô, chỉ giữ trạm có file trong process/.

    Công thức row/col trùng `AOD_map/stage_a/gridder._lat_to_row/_lon_to_col`,
    nên trạm rơi vào đúng ô mà Stage A bin dữ liệu vệ tinh vào.
    """
    df = pd.read_csv(STATION_MAP)
    df["_key"] = df["stationName"].map(_norm)

    files = {_norm(p.stem): p for p in AQI_DIR.glob("*.csv") if p.is_file()}
    df["path"] = df["_key"].map(files)
    missing = int(df["path"].isna().sum())
    df = df.dropna(subset=["path"]).copy()

    r = spec["res"]
    df["row"] = np.floor((spec["lat_max"] - df["latitude"]) / r).astype(int)
    df["col"] = np.floor((df["longitude"] - spec["lon_min"]) / r).astype(int)
    inside = (df["row"].between(0, spec["nlat"] - 1)
              & df["col"].between(0, spec["nlon"] - 1))
    outside = int((~inside).sum())
    df = df[inside].copy()
    df["cell_id"] = df["row"] * spec["nlon"] + df["col"]
    df["cell_lat"] = spec["lat_max"] - (df["row"] + 0.5) * r
    df["cell_lon"] = spec["lon_min"] + (df["col"] + 0.5) * r

    # Tự kiểm: toạ độ trạm phải nằm trong đúng hộp của ô vừa tính.
    assert (np.abs(df["latitude"] - df["cell_lat"]) <= r / 2 + 1e-9).all(), \
        "trạm rơi ngoài ô theo vĩ độ"
    assert (np.abs(df["longitude"] - df["cell_lon"]) <= r / 2 + 1e-9).all(), \
        "trạm rơi ngoài ô theo kinh độ"

    print(f"[cell] {len(df)} trạm có dữ liệu → {df['cell_id'].nunique()} ô "
          f"{r}°  (bỏ {missing} trạm không có file process/, "
          f"{outside} trạm ngoài hộp bao)")
    return df


# --------------------------------------------------------------------------- giờ / ngày của trạm
def _read_hours(path: Path) -> pd.DataFrame:
    """CSV một trạm → (t, pm25) từng giờ có số liệu, t = nhãn giờ địa phương."""
    d = pd.read_csv(path, usecols=["datetime", "pm2.5"], low_memory=False)
    t = pd.to_datetime(d["datetime"], errors="coerce")
    v = pd.to_numeric(d["pm2.5"], errors="coerce")
    ok = t.notna() & v.notna()
    d = pd.DataFrame({"t": t[ok].dt.floor("h"), "pm25": v[ok]})
    # filter_aqi đã khử trùng lặp, nhưng giữ lại cho chắc nếu đọc nguồn khác.
    return d.drop_duplicates("t")


def station_daily(path: Path, start: pd.Timestamp, end: pd.Timestamp,
                  hours: tuple[int, int] | None = None) -> pd.DataFrame:
    """CSV một trạm → trung bình PM2.5 theo ngày + số giờ có số liệu.

    `hours = (h0, h1)` giới hạn các giờ địa phương đưa vào trung bình, hai đầu
    đều tính (ví dụ `(8, 17)` giữ 10 giờ 08:00–17:00). Để None thì lấy cả 24
    giờ — đúng định nghĩa trung bình ngày của Li et al. §2.1 và của QCVN/WHO.
    """
    d = _read_hours(path)
    if d.empty:
        return pd.DataFrame(columns=["date", "pm25", "n_hours"])
    d["date"] = d["t"].dt.normalize()
    d = d[(d["date"] >= start) & (d["date"] <= end)]
    if hours is not None:
        h0, h1 = hours
        d = d[d["t"].dt.hour.between(h0, h1)]
    if d.empty:
        return pd.DataFrame(columns=["date", "pm25", "n_hours"])
    return (d.groupby("date", as_index=False)
              .agg(pm25=("pm25", "mean"), n_hours=("pm25", "size")))


# --------------------------------------------------------------------------- gộp về ô
def build(start: str, end: str, min_hours: int = 0,
          hours: tuple[int, int] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    spec = DG.grid_spec()
    print(f"[grid] {spec['nlat']}×{spec['nlon']} ô · bước {spec['res']}° · "
          f"{spec['lon_min']}–{spec['lon_max']}°E, {spec['lat_min']}–{spec['lat_max']}°N")
    stations = station_cells(spec)
    lo, hi = pd.Timestamp(start), pd.Timestamp(end)

    parts = []
    for s in stations.itertuples():
        d = station_daily(Path(s.path), lo, hi, hours)
        if min_hours > 0:
            d = d[d["n_hours"] >= min_hours]
        if d.empty:
            continue
        d["cell_id"] = s.cell_id
        d["stationName"] = s.stationName
        parts.append(d)
    if not parts:
        raise RuntimeError("không trạm nào cho ra ngày hợp lệ")

    per_station = pd.concat(parts, ignore_index=True)
    if hours is not None:
        print(f"[daily] chỉ lấy giờ {hours[0]:02d}:00–{hours[1]:02d}:00 "
              f"({hours[1]-hours[0]+1} giờ/ngày tối đa)")
    print(f"[daily] {len(per_station):,} (trạm, ngày) từ "
          f"{per_station['stationName'].nunique()} trạm")

    # Tầng hai: trung bình các trạm trong cùng một ô.
    cell = (per_station.groupby(["cell_id", "date"], as_index=False)
            .agg(pm25=("pm25", "mean"),
                 pm25_sd_stations=("pm25", lambda x: float(np.std(x, ddof=0))
                                   if len(x) > 1 else 0.0),
                 n_stations=("stationName", "nunique"),
                 n_hours_total=("n_hours", "sum"),
                 n_hours_min=("n_hours", "min")))

    # Phương án đối chiếu: trung bình có trọng số theo số giờ mỗi trạm góp vào.
    per_station["_w"] = per_station["pm25"] * per_station["n_hours"]
    w = (per_station.groupby(["cell_id", "date"], as_index=False)
         .agg(_sw=("_w", "sum"), _sh=("n_hours", "sum")))
    w["pm25_hour_weighted"] = w["_sw"] / w["_sh"]
    cell = cell.merge(w[["cell_id", "date", "pm25_hour_weighted"]],
                      on=["cell_id", "date"], how="left")

    meta = _cell_meta(stations)
    cell = cell.merge(meta[["cell_id", "row", "col", "cell_lat", "cell_lon"]],
                      on="cell_id", how="left")
    front = ["cell_id", "row", "col", "cell_lat", "cell_lon", "date", "pm25"]
    cell = cell[[*front, *[c for c in cell.columns if c not in front]]]
    return cell.sort_values(["cell_id", "date"]).reset_index(drop=True), meta


def build_hourly(start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(ô, giờ): TB không trọng số các trạm trong ô có số liệu giờ đó. `time_lt` = nhãn giờ địa phương."""
    spec = DG.grid_spec()
    stations = station_cells(spec)
    lo, hi = pd.Timestamp(start), pd.Timestamp(end) + pd.Timedelta(days=1)

    parts = []
    for s in stations.itertuples():
        d = _read_hours(Path(s.path))
        d = d[(d["t"] >= lo) & (d["t"] < hi)]
        if len(d):
            parts.append(d.assign(cell_id=s.cell_id))
    if not parts:
        raise RuntimeError("không trạm nào có giờ hợp lệ")
    per_station = pd.concat(parts, ignore_index=True).rename(columns={"t": "time_lt"})
    print(f"[hourly] {len(per_station):,} (trạm, giờ) từ {len(parts)} trạm")

    # Mỗi trạm có tối đa một dòng mỗi giờ (đã khử trùng lặp), nên số dòng = số trạm.
    g = per_station.groupby(["cell_id", "time_lt"])["pm25"]
    cell = g.agg(pm25="mean", n_stations="size").reset_index()
    cell["pm25_sd_stations"] = g.std(ddof=0).to_numpy()

    meta = _cell_meta(stations)
    cell = cell.merge(meta[["cell_id", "row", "col", "cell_lat", "cell_lon"]], on="cell_id", how="left")
    front = ["cell_id", "row", "col", "cell_lat", "cell_lon", "time_lt", "pm25"]
    cell = cell[[*front, *[c for c in cell.columns if c not in front]]]
    return cell.sort_values(["cell_id", "time_lt"]).reset_index(drop=True), meta


def _cell_meta(stations: pd.DataFrame) -> pd.DataFrame:
    """Thuộc tính ô: toạ độ tâm luôn có; tỉnh/vùng lấy từ GeoPackage nếu đã dựng."""
    meta = (stations.groupby("cell_id", as_index=False)
            .agg(row=("row", "first"), col=("col", "first"),
                 cell_lat=("cell_lat", "first"), cell_lon=("cell_lon", "first"),
                 n_stations_total=("stationName", "nunique"),
                 sensor_mix=("stationName",
                             lambda x: "+".join(sorted({sensor_class(n) for n in x}))),
                 station_names=("stationName", lambda x: " | ".join(sorted(set(x))))))
    if GPKG.exists():
        import geopandas as gpd
        gg = gpd.read_file(GPKG, layer="grid")[
            ["cell_id", "province", "region", "in_vietnam"]]
        meta = meta.merge(gg, on="cell_id", how="left")
    else:
        print(f"[meta] chưa có {GPKG.name} — bỏ qua cột province/region "
              f"(chạy divide_grid.py để có)")
    return meta


def report(cell: pd.DataFrame, meta: pd.DataFrame) -> None:
    print(f"\n[out] {len(cell):,} dòng (ô, ngày) · {cell['cell_id'].nunique()} ô · "
          f"{cell['date'].min().date()} → {cell['date'].max().date()}")
    multi = meta[meta["n_stations_total"] > 1]
    print(f"[out] ô có nhiều hơn 1 trạm: {len(multi)}/{len(meta)} "
          f"(gộp {int(multi['n_stations_total'].sum())} trạm)")
    vc = cell["n_stations"].value_counts().sort_index()
    print("[out] số trạm góp vào mỗi dòng:", vc.to_dict())
    if len(multi):
        print("[out] thành phần thiết bị của ô gộp:",
              multi["sensor_mix"].value_counts().to_dict())
    gop = cell[cell["n_stations"] > 1]
    if len(gop):
        print(f"[out] trong {len(gop):,} dòng có gộp: độ lệch chuẩn giữa các trạm "
              f"trung vị {gop['pm25_sd_stations'].median():.2f} µg/m³, "
              f"p95 {gop['pm25_sd_stations'].quantile(0.95):.2f}")
        dd = (gop["pm25"] - gop["pm25_hour_weighted"]).abs()
        print(f"[out] lệch giữa 2 cách trung bình: trung vị {dd.median():.3f}, "
              f"tối đa {dd.max():.2f} µg/m³")
    print(f"[out] pm2.5: trung bình {cell['pm25'].mean():.1f}, "
          f"trung vị {cell['pm25'].median():.1f}, p95 {cell['pm25'].quantile(.95):.1f} µg/m³")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--start", default="2020-01-01")
    ap.add_argument("--end", default="2026-12-31")
    ap.add_argument("--hours", nargs=2, type=int, default=None,
                    metavar=("H0", "H1"),
                    help="chỉ dùng giờ địa phương H0..H1 (cả hai đầu), "
                         "ví dụ --hours 8 17 cho cửa sổ vệ tinh bay qua; "
                         "bỏ trống = cả 24 giờ (mặc định, đúng paper)")
    ap.add_argument("--min-hours", type=int, default=0,
                    help="bỏ ngày-trạm có ít hơn ngần này giờ (mặc định 0 = không lọc)")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    ap.add_argument("--name", default="aqi_pm25_grid")
    ap.add_argument("--hourly", action="store_true",
                    help="ghi bảng (ô, giờ) {name}_hourly.pkl thay cho bảng ngày; "
                         "bỏ qua --hours / --min-hours, không ghi đè bảng ngày và bảng ô")
    args = ap.parse_args(argv)

    if args.hourly:
        cell, meta = build_hourly(args.start, args.end)
        print(f"\n[out] {len(cell):,} dòng (ô, giờ) · {cell['cell_id'].nunique()} ô · "
              f"{cell['time_lt'].min()} → {cell['time_lt'].max()}")
        print("[out] số trạm góp vào mỗi dòng:", cell["n_stations"].value_counts().sort_index().to_dict())
        print(f"[out] pm2.5: trung bình {cell['pm25'].mean():.1f}, "
              f"trung vị {cell['pm25'].median():.1f}, p95 {cell['pm25'].quantile(.95):.1f} µg/m³")
        args.out_dir.mkdir(parents=True, exist_ok=True)
        f_hourly = args.out_dir / f"{args.name}_hourly.pkl"
        cell.to_pickle(f_hourly)
        print(f"[out] → {f_hourly}  ({f_hourly.stat().st_size/1e6:.1f} MB)")
        return 0

    hours = tuple(args.hours) if args.hours else None
    if hours and not (0 <= hours[0] <= hours[1] <= 23):
        ap.error(f"--hours không hợp lệ: {hours}")
    if hours and args.min_hours > hours[1] - hours[0] + 1:
        ap.error(f"--min-hours {args.min_hours} lớn hơn số giờ tối đa "
                 f"({hours[1]-hours[0]+1}) của cửa sổ {hours} — không ngày nào qua được")
    cell, meta = build(args.start, args.end, args.min_hours, hours)
    report(cell, meta)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    f_daily = args.out_dir / f"{args.name}_daily.csv"
    f_cells = args.out_dir / f"{args.name}_cells.csv"
    cell.to_csv(f_daily, index=False)
    cell.to_pickle(args.out_dir / f"{args.name}_daily.pkl")
    meta.to_csv(f_cells, index=False)
    print(f"\n[out] → {f_daily}  ({f_daily.stat().st_size/1e6:.1f} MB)")
    print(f"[out] → {f_cells}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
