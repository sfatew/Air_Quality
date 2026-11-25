"""Chia lãnh thổ Việt Nam thành lưới đều 0.05° × 0.05° trên WGS-84 (EPSG:4326).

Lưới KHÔNG được định nghĩa lại ở đây: nó đọc thẳng hằng số từ
``AOD_map/stage_a/config.py`` (LAT_MIN/LAT_MAX/LON_MIN/LON_MAX/GRID_RES) để
mỗi ô trùng khít từng chỉ số (row, col) với lưới AOD của Stage A/B. Nhờ vậy
ghép PM2.5 với AOD về sau chỉ là phép join theo ``cell_id``, không cần nội suy.
Nếu Stage A đổi domain, chạy lại file này là lưới tự khớp theo — không có hằng
số nào bị chép tay hai nơi.

Quy ước chỉ số (giống ``AOD_map/stage_a/gridder.py``):
    row = 0  là hàng CỰC BẮC   (lat giảm dần theo row)
    col = 0  là cột CỰC TÂY    (lon tăng dần theo col)
    cell_id = row * NLON + col          -> trùng thứ tự flatten của raster

Mọi ô trong hộp bao đều được giữ (310 × 160 = 49 600 ô, hình chữ nhật đầy đủ),
kèm các cột phân loại:

    in_vietnam       tâm ô nằm trong một tỉnh của GADM  (dùng để lọc/thống kê)
    touches_vietnam  ô có giao với lãnh thổ             (ôm trọn bờ biển + đảo)
    province/region  tên tỉnh và mã vùng của dự án

Lưu ý về dữ liệu ranh giới: ``gadm41_VNM_0/1.shp`` chỉ phủ tới 109.47°E, tức
KHÔNG bao gồm Hoàng Sa và Trường Sa. Mọi ô phía đông kinh tuyến đó đều mang
``in_vietnam = False`` — đó là giới hạn của file ranh giới, không phải kết luận
về chủ quyền. Muốn có hai quần đảo thì phải bổ sung shapefile khác.

Chạy:
    python devide_grid.py                  # -> output/vn_grid_005deg.gpkg
    python devide_grid.py --self-check     # chỉ đối chiếu lưới với Stage A
    python devide_grid.py --res 0.1 --out output/vn_grid_01deg.gpkg
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import box

def find_repo(start: Path | None = None) -> Path:
    """Đi ngược lên cho tới thư mục gốc của dự án.

    Nhận ra gốc bằng việc nó chứa cả `Masterdata/` và `AOD_map/`. Dùng cách
    này thay cho `parent.parent` vì các script trong dự án hay bị chuyển chỗ;
    đếm số cấp thư mục sẽ hỏng lặng lẽ mỗi lần như vậy, còn mốc nhận dạng thì
    không.
    """
    here = (start or Path(__file__)).resolve()
    for d in here.parents:
        if (d / "Masterdata").is_dir() and (d / "AOD_map").is_dir():
            return d
    raise RuntimeError(f"không tìm thấy gốc dự án tính từ {here}")


REPO = find_repo()

STAGE_A_CONFIG = REPO / "AOD_map" / "stage_a" / "config.py"
STAGE_A_GRIDDER = REPO / "AOD_map" / "stage_a" / "gridder.py"
GADM_L1 = REPO / "Data_Collection" / "GADM_Vietnam" / "gadm41_VNM_1.shp"
REGION_SRC = REPO / "Masterdata" / "src" / "add_region.py"

DEFAULT_OUT = REPO / "pm25_map" / "output" / "vn_grid_005deg.gpkg"
DEFAULT_LAYER = "grid"

# 1° vĩ độ trên cầu bán kính 6371.0088 km (cùng bán kính các module khác dùng).
KM_PER_DEG = 2.0 * np.pi * 6371.0088 / 360.0


def _load_module(path: Path, name: str):
    """Nạp một file .py rời làm module (Stage A không phải package cài đặt được)."""
    if not path.exists():
        raise FileNotFoundError(f"không tìm thấy {path}")
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_stage_a_gridder(cfg):
    """Nạp ``stage_a/gridder.py``, vốn viết ``from config import ...``.

    Nó mong thư mục stage_a nằm trên sys.path và module tên đúng là ``config``,
    nên ta tạm gắn alias rồi trả nguyên trạng — tránh để một module tên chung
    như ``config`` nằm lại trong sys.modules của tiến trình gọi.
    """
    d = str(STAGE_A_GRIDDER.parent)
    added = d not in sys.path
    if added:
        sys.path.insert(0, d)
    prev = sys.modules.get("config")
    sys.modules["config"] = cfg
    try:
        return _load_module(STAGE_A_GRIDDER, "_stage_a_gridder")
    finally:
        if prev is not None:
            sys.modules["config"] = prev
        else:
            sys.modules.pop("config", None)
        if added:
            sys.path.remove(d)


def grid_spec(res: float | None = None) -> dict:
    """Hộp bao + bước lưới, lấy từ Stage A; `res` chỉ để ghi đè khi thử nghiệm."""
    cfg = _load_module(STAGE_A_CONFIG, "_stage_a_config")
    r = float(cfg.GRID_RES if res is None else res)
    lat_min, lat_max = float(cfg.LAT_MIN), float(cfg.LAT_MAX)
    lon_min, lon_max = float(cfg.LON_MIN), float(cfg.LON_MAX)
    nlat = int(round((lat_max - lat_min) / r))
    nlon = int(round((lon_max - lon_min) / r))
    return {"res": r, "lat_min": lat_min, "lat_max": lat_max,
            "lon_min": lon_min, "lon_max": lon_max, "nlat": nlat, "nlon": nlon}


def build_grid(spec: dict) -> gpd.GeoDataFrame:
    """Dựng toàn bộ ô của hộp bao thành polygon EPSG:4326.

    Cạnh ô tính từ chỉ số (``lon_min + col*res``) chứ không từ tâm ± res/2, để
    sai số dấu phẩy động không tích luỹ dọc theo 160 cột.
    """
    r, nlat, nlon = spec["res"], spec["nlat"], spec["nlon"]
    rows, cols = np.meshgrid(np.arange(nlat), np.arange(nlon), indexing="ij")
    rows, cols = rows.ravel(), cols.ravel()

    lon_w = spec["lon_min"] + cols * r
    lon_e = lon_w + r
    lat_n = spec["lat_max"] - rows * r
    lat_s = lat_n - r
    lat_c = lat_n - r / 2.0
    lon_c = lon_w + r / 2.0

    gdf = gpd.GeoDataFrame(
        {
            "cell_id": (rows * nlon + cols).astype(np.int32),
            "row": rows.astype(np.int16),
            "col": cols.astype(np.int16),
            "lon": lon_c.astype(np.float64),
            "lat": lat_c.astype(np.float64),
            "lon_min": lon_w, "lat_min": lat_s, "lon_max": lon_e, "lat_max": lat_n,
            # Ô 0.05° đủ nhỏ để dùng công thức hình chữ nhật trên mặt cầu;
            # diện tích co lại theo cos(vĩ độ), từ ~30.7 km² ở 8°N còn ~28.4 km² ở 23.5°N.
            "area_km2": (r * KM_PER_DEG) * (r * KM_PER_DEG * np.cos(np.radians(lat_c))),
        },
        geometry=box(lon_w, lat_s, lon_e, lat_n),
        crs="EPSG:4326",
    )
    return gdf


def attach_vietnam(gdf: gpd.GeoDataFrame, verbose: bool = True) -> gpd.GeoDataFrame:
    """Gắn cờ lãnh thổ + tỉnh + vùng từ GADM level-1."""
    prov = gpd.read_file(GADM_L1)[["NAME_1", "geometry"]].to_crs("EPSG:4326")

    # (1) tâm ô nằm trong tỉnh nào -> in_vietnam + province
    centres = gpd.GeoDataFrame(
        {"cell_id": gdf["cell_id"]},
        geometry=gpd.points_from_xy(gdf["lon"], gdf["lat"]),
        crs="EPSG:4326",
    )
    hit = gpd.sjoin(centres, prov, how="left", predicate="within")
    # Tâm rơi đúng biên hai tỉnh có thể khớp nhiều polygon; giữ bản ghi đầu.
    hit = hit[~hit.index.duplicated(keep="first")]
    gdf["province"] = hit["NAME_1"].to_numpy()
    gdf["in_vietnam"] = gdf["province"].notna()

    # (2) ô có giao với lãnh thổ (rộng hơn (1): gồm cả ô ven bờ lệch tâm ra biển)
    touch = gpd.sjoin(gdf[["cell_id", "geometry"]], prov,
                      how="inner", predicate="intersects")
    gdf["touches_vietnam"] = gdf["cell_id"].isin(touch["cell_id"].unique())

    # (3) ô chỉ "chạm" thì gán tỉnh theo phần giao lớn nhất, để dải ven biển
    #     vẫn có nhãn hành chính khi vẽ bản đồ.
    only_touch = gdf.loc[gdf["touches_vietnam"] & ~gdf["in_vietnam"],
                         ["cell_id", "geometry"]]
    if len(only_touch):
        inter = gpd.overlay(only_touch, prov, how="intersection")
        if len(inter):
            # Diện tích tính bằng độ² — ở đây chỉ dùng để SO SÁNH tương đối giữa
            # các tỉnh cắt cùng một ô 0.05°, nên méo hình theo vĩ độ không đổi
            # thứ hạng. Không dùng con số này làm diện tích thật (đã có area_km2).
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message=".*geographic CRS.*")
                inter["_a"] = inter.geometry.area
            best = inter.loc[inter.groupby("cell_id")["_a"].idxmax(),
                             ["cell_id", "NAME_1"]]
            fill = gdf["cell_id"].map(best.set_index("cell_id")["NAME_1"])
            gdf["province"] = gdf["province"].fillna(fill)

    # (4) tỉnh -> vùng, dùng đúng bảng đã gán vùng cho các trạm quan trắc
    reg = _load_module(REGION_SRC, "_add_region")
    gdf["region"] = gdf["province"].map(
        lambda n: reg.PROVINCE_TO_REGION.get(reg._ascii_lower(str(n)), None)
        if pd.notna(n) else None
    )

    unmapped = sorted(gdf.loc[gdf["province"].notna() & gdf["region"].isna(),
                              "province"].unique())
    if verbose and unmapped:
        print(f"[grid] CẢNH BÁO: {len(unmapped)} tỉnh chưa có trong "
              f"PROVINCE_TO_REGION -> region = None: {unmapped}")
    return gdf


def self_check(gdf: gpd.GeoDataFrame, spec: dict) -> None:
    """Đối chiếu lưới vừa dựng với định nghĩa gốc của Stage A."""
    cfg = _load_module(STAGE_A_CONFIG, "_stage_a_config")
    if abs(spec["res"] - float(cfg.GRID_RES)) > 1e-12:
        print(f"[check] BỎ QUA đối chiếu: đang chạy res={spec['res']}, "
              f"Stage A dùng {cfg.GRID_RES}")
        return

    assert spec["nlat"] == cfg.NLAT, f"NLAT {spec['nlat']} != {cfg.NLAT}"
    assert spec["nlon"] == cfg.NLON, f"NLON {spec['nlon']} != {cfg.NLON}"

    first = gdf.sort_values("cell_id").drop_duplicates("row")
    assert np.allclose(first.sort_values("row")["lat"].to_numpy(), cfg.LATS), \
        "tâm ô theo vĩ độ lệch so với config.LATS"
    firstc = gdf.sort_values("cell_id").drop_duplicates("col")
    assert np.allclose(firstc.sort_values("col")["lon"].to_numpy(), cfg.LONS), \
        "tâm ô theo kinh độ lệch so với config.LONS"

    # Chỉ số (row, col) phải khớp hàm ánh xạ mà Stage A dùng khi bin dữ liệu.
    gridder = _load_stage_a_gridder(cfg)
    sample = gdf.sample(min(2000, len(gdf)), random_state=0)
    r_back = gridder._lat_to_row(sample["lat"].to_numpy())
    c_back = gridder._lon_to_col(sample["lon"].to_numpy())
    assert (r_back == sample["row"].to_numpy()).all(), "row không khớp _lat_to_row"
    assert (c_back == sample["col"].to_numpy()).all(), "col không khớp _lon_to_col"
    print(f"[check] OK — {spec['nlat']}×{spec['nlon']} ô trùng khít lưới Stage A "
          f"(tâm ô, chỉ số row/col, bước {spec['res']}°)")


def report(gdf: gpd.GeoDataFrame) -> None:
    n = len(gdf)
    n_in = int(gdf["in_vietnam"].sum())
    n_touch = int(gdf["touches_vietnam"].sum())
    print(f"\n[grid] {n:,} ô | trong lãnh thổ {n_in:,} ({100*n_in/n:.1f}%) | "
          f"giao lãnh thổ {n_touch:,} ({100*n_touch/n:.1f}%)")
    print(f"[grid] diện tích ô: {gdf['area_km2'].min():.2f} – "
          f"{gdf['area_km2'].max():.2f} km²  "
          f"(tổng phần in_vietnam ≈ {gdf.loc[gdf['in_vietnam'],'area_km2'].sum():,.0f} km²)")
    vc = gdf.loc[gdf["in_vietnam"], "region"].value_counts(dropna=False)
    print("[grid] số ô theo vùng:", vc.to_dict())
    no_reg = int((gdf["in_vietnam"] & gdf["region"].isna()).sum())
    if no_reg:
        print(f"[grid] {no_reg:,} ô trong lãnh thổ nhưng chưa gán được vùng")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--res", type=float, default=None,
                    help="ghi đè bước lưới (mặc định: lấy GRID_RES của Stage A)")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--layer", default=DEFAULT_LAYER)
    ap.add_argument("--self-check", action="store_true",
                    help="chỉ đối chiếu lưới với Stage A rồi thoát, không ghi file")
    args = ap.parse_args(argv)

    spec = grid_spec(args.res)
    print(f"[grid] hộp bao: {spec['lon_min']}–{spec['lon_max']}°E, "
          f"{spec['lat_min']}–{spec['lat_max']}°N | bước {spec['res']}° | "
          f"{spec['nlat']}×{spec['nlon']} ô")

    gdf = build_grid(spec)
    self_check(gdf, spec)
    if args.self_check:
        return 0

    gdf = attach_vietnam(gdf)
    report(gdf)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out, layer=args.layer, driver="GPKG")
    print(f"\n[grid] → {out}  (layer '{args.layer}', {out.stat().st_size/1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
