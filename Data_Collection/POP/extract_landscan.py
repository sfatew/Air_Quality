"""
Extract LandScan Global yearly population tại các trạm quan trắc.

Source : projects/sat-io/open-datasets/ORNL/LANDSCAN_GLOBAL (GEE Community Catalog)
Avail  : 2000-2024 (LandScan 2025/2026 chưa release tính tới 06/2026)
Res    : ~1km (30 arc-sec). Đơn vị: người/pixel (ambient, 24h average)

Usage:
    pip install earthengine-api pandas
    earthengine authenticate
    python extract_landscan_pop.py
"""
import argparse
import time
from pathlib import Path

import ee
import pandas as pd

# ===== Config =====
STATION_CSV = "/home/work1/projects/Air_Quality/Masterdata/envisoft_station_map.csv"
OUT_CSV     = "/home/work1/projects/Air_Quality/Masterdata/station_pop_landscan.csv"
YEARS       = list(range(2022, 2027))   # 2022..2026
LATEST_YEAR = 2024                      # năm mới nhất có data; > năm này -> fallback
BUFFER_M    = 0                         # 0 = pixel tại điểm; >0 = sum trong buffer (m)
BATCH_SIZE  = 500                       # số trạm/batch để tránh getInfo timeout
GEE_PROJECT = "your-gee-project"        # đổi thành project GEE của bạn


def build_fc(df: pd.DataFrame) -> ee.FeatureCollection:
    feats = [
        ee.Feature(
            ee.Geometry.Point([float(r.longitude), float(r.latitude)]),
            {"stationId": str(r.stationId)},
        )
        for r in df.itertuples()
    ]
    return ee.FeatureCollection(feats)


def get_image(year: int, ic: ee.ImageCollection) -> ee.Image:
    """Lấy ảnh LandScan theo năm. year > LATEST_YEAR -> fallback LATEST_YEAR."""
    y = min(year, LATEST_YEAR)
    img = ic.filter(ee.Filter.date(f"{y}-01-01", f"{y}-12-31")).first()
    return ee.Image(img).rename("pop")


def sample_batch(img: ee.Image, fc: ee.FeatureCollection, buffer_m: int):
    if buffer_m > 0:
        sampled = img.reduceRegions(
            collection=fc.map(lambda f: f.buffer(buffer_m)),
            reducer=ee.Reducer.sum(),
            scale=1000,
        )
        key = "sum"
    else:
        sampled = img.sampleRegions(collection=fc, scale=1000, geometries=False)
        key = "pop"
    return sampled.getInfo()["features"], key


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stations", default=STATION_CSV)
    parser.add_argument("--out", default=OUT_CSV)
    parser.add_argument("--buffer", type=int, default=BUFFER_M,
                        help="Buffer radius in meters (0 = point sample)")
    parser.add_argument("--project", default=GEE_PROJECT)
    args = parser.parse_args()

    ee.Initialize(project=args.project)

    stations = pd.read_csv(args.stations, dtype={"stationId": str})
    stations = stations.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
    print(f"Loaded {len(stations)} stations from {args.stations}")

    landscan = ee.ImageCollection("projects/sat-io/open-datasets/ORNL/LANDSCAN_GLOBAL")

    records = []
    for year in YEARS:
        img = get_image(year, landscan)
        is_extrap = year > LATEST_YEAR
        src_year = min(year, LATEST_YEAR)

        # Batch để tránh getInfo timeout khi nhiều trạm
        t0 = time.time()
        for i in range(0, len(stations), BATCH_SIZE):
            batch = stations.iloc[i:i + BATCH_SIZE]
            fc = build_fc(batch)
            feats, key = sample_batch(img, fc, args.buffer)
            for f in feats:
                p = f["properties"]
                records.append({
                    "stationId": p["stationId"],
                    "year": year,
                    "population": p.get(key),
                    "source_year": src_year,
                    "is_extrapolated": is_extrap,
                })
        print(f"  year {year} (source {src_year}, extrap={is_extrap}) "
              f"-> {len(stations)} pts in {time.time()-t0:.1f}s")

    df = pd.DataFrame(records).merge(
        stations[["stationId", "stationName", "latitude", "longitude"]],
        on="stationId", how="left",
    )
    df = df[["stationId", "stationName", "latitude", "longitude",
             "year", "population", "source_year", "is_extrapolated"]]

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Saved {len(df)} rows -> {args.out}")


if __name__ == "__main__":
    main()