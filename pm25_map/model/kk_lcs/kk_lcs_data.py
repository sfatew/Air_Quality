"""KK/LCS experiment — hourly PM2.5 labels split by sensor class.

Station → cell, QC'd hourly series and hour labels come unchanged from
`features/build_grid/map_aqi_grid.py` (station_cells, _read_hours, sensor_class); predictors are the
unchanged rows of `_data.build_frame()` and folds are `_data.make_folds()` (cells → 5 folds).
One row per (cell, local hour, sensor class): y_raw = unweighted mean of that class's stations in the
cell-hour (map_aqi_grid's rule applied per class). In pure KK or pure LCS cells y_raw equals the
frame's `pm25`; each of the 4 mixed cells gets one KK row and one LCS row with identical predictors.
The single unsuffixed station (Vũng Tàu, class OTHER) is dropped. On-site RH = hourly mean of
`Humidity` in the raw tedp.vn records (`AQI/raw/<same file>`).

Cache (kk_lcs/data/):
    long.pkl         class rows + frame predictors, only cell-hours present in frame_hourly.pkl
    lcs_station.pkl  LCS station-hours on those cell-hours (+ on-site RH) — D corrects per station
    colloc.pkl       full overlapping series of collocated LCS–KK pairs (all hours), for fitting D
    stations.csv     stations with class, cell, province, collocated partner

    python kk_lcs_data.py            # build if missing, print summary
    python kk_lcs_data.py --refresh
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent


def _model_dir() -> Path:
    env = os.environ.get("KK_LCS_MODEL_DIR")
    if env:
        return Path(env)
    if (HERE.parent / "_data.py").exists():
        return HERE.parent
    # worktree without the (uncommitted) model code: use the main checkout read-only
    return Path("/home/work1/projects/Air_Quality/pm25_map/model")


MODEL = _model_dir()
sys.path.insert(0, str(MODEL))
sys.path.insert(0, str(MODEL.parent / "features" / "build_grid"))
import _data as D  # noqa: E402
import divide_grid as DG  # noqa: E402
import map_aqi_grid as MG  # noqa: E402

DATA = HERE / "data"
LONG_PKL, LCS_PKL, COLLOC_PKL, STATIONS_CSV = (DATA / "long.pkl", DATA / "lcs_station.pkl",
                                               DATA / "colloc.pkl", DATA / "stations.csv")
CLASSES = ("KK", "LCS")
COLLOC_KM = 0.5          # "same site": the only pair in the network is 0.11 km apart; the next is 1.14 km


def province(name: str) -> str:
    return str(name).split(":")[0].strip()


def stations() -> pd.DataFrame:
    st = MG.station_cells(DG.grid_spec())
    st["cls"] = st.stationName.map(MG.sensor_class)
    st = st[st.cls.isin(CLASSES)].copy()
    st["province"] = st.stationName.map(province)
    st["raw_path"] = st.path.map(lambda p: Path(str(p).replace("/AQI/process/", "/AQI/raw/")))
    return st.reset_index(drop=True)


def station_rh(raw_path: Path) -> pd.Series:
    """Hourly mean on-site relative humidity (%) from the raw records; empty if unavailable."""
    if not Path(raw_path).exists():
        return pd.Series(dtype=float)
    r = pd.read_csv(raw_path, usecols=lambda c: c in ("Timestamp", "Humidity"), low_memory=False)
    if "Humidity" not in r:
        return pd.Series(dtype=float)
    t = pd.to_datetime(r.Timestamp.astype(str).str.replace("Z", "", regex=False), errors="coerce").dt.floor("h")
    v = pd.to_numeric(r.Humidity, errors="coerce")
    ok = t.notna() & v.between(0, 100)
    return v[ok].groupby(t[ok]).mean()


def station_series(s) -> pd.DataFrame:
    """All QC'd hours of one station: index time_lt, columns pm25, rh."""
    d = MG._read_hours(Path(s.path)).set_index("t").rename_axis("time_lt")
    d["rh"] = station_rh(s.raw_path).reindex(d.index)
    return d


def collocated_pairs(st: pd.DataFrame, max_km: float = COLLOC_KM) -> pd.DataFrame:
    kk, lcs = st[st.cls == "KK"], st[st.cls == "LCS"]
    d = D.haversine_km(lcs.latitude.values[:, None], lcs.longitude.values[:, None],
                       kk.latitude.values[None], kk.longitude.values[None])
    i, j = np.where(d <= max_km)
    return pd.DataFrame({"lcs": lcs.stationName.values[i], "kk": kk.stationName.values[j], "km": d[i, j],
                         "lcs_cell": lcs.cell_id.values[i], "kk_cell": kk.cell_id.values[j]})


def build(refresh: bool = False, verbose: bool = True) -> dict:
    if not refresh and all(p.exists() for p in (LONG_PKL, LCS_PKL, COLLOC_PKL, STATIONS_CSV)):
        return load()
    frame = D.build_frame(verbose=False)
    folds = D.make_folds(frame).set_index("cell_id").outer_fold
    st = stations()
    series = {s.stationName: station_series(s) for s in st.itertuples()}

    keys = frame[["cell_id", "time_lt"]].drop_duplicates()
    parts = [series[s.stationName].reset_index().assign(station=s.stationName, cls=s.cls, cell_id=s.cell_id,
                                                         province=s.province)
             for s in st.itertuples()]
    sh = pd.concat(parts, ignore_index=True).merge(keys, on=["cell_id", "time_lt"], how="inner")

    cls_rows = (sh.groupby(["cell_id", "time_lt", "cls"])
                  .agg(y_raw=("pm25", "mean"), n_stations_cls=("station", "nunique"), rh_site=("rh", "mean"))
                  .reset_index())
    feats = frame.rename(columns={D.PM: "pm25_cell", "n_stations": "n_stations_cell"})
    long = cls_rows.merge(feats, on=["cell_id", "time_lt"], how="left", validate="many_to_one")
    long["outer_fold"] = long.cell_id.map(folds).astype(int)
    long = long.sort_values(["time_lt", "cell_id", "cls"]).reset_index(drop=True)

    pairs = collocated_pairs(st)
    colloc = {}
    for p in pairs.itertuples():
        j = series[p.lcs].join(series[p.kk], how="inner", lsuffix="_lcs", rsuffix="_kk")
        j = j.dropna(subset=["pm25_lcs", "pm25_kk"]).rename(columns={"pm25_lcs": "lcs", "pm25_kk": "kk",
                                                                     "rh_lcs": "rh"})
        colloc[p.lcs] = dict(kk=p.kk, km=float(p.km), lcs_cell=int(p.lcs_cell), kk_cell=int(p.kk_cell),
                             province=province(p.lcs), df=j[["lcs", "kk", "rh", "rh_kk"]])
    st_out = (st.drop(columns=["path", "raw_path", "_key"], errors="ignore")
                .merge(pairs[["lcs", "kk", "km"]].rename(columns={"lcs": "stationName", "kk": "colloc_kk",
                                                                  "km": "colloc_km"}),
                       on="stationName", how="left"))
    st_out["outer_fold"] = st_out.cell_id.map(folds)

    DATA.mkdir(parents=True, exist_ok=True)
    long.to_pickle(LONG_PKL)
    sh[sh.cls == "LCS"][["station", "province", "cell_id", "time_lt", "pm25", "rh"]] \
        .reset_index(drop=True).to_pickle(LCS_PKL)
    pd.to_pickle(colloc, COLLOC_PKL)
    st_out.to_csv(STATIONS_CSV, index=False)
    out = load()
    if verbose:
        summary(out)
    return out


def load() -> dict:
    return dict(long=pd.read_pickle(LONG_PKL), lcs_station=pd.read_pickle(LCS_PKL),
                colloc=pd.read_pickle(COLLOC_PKL), stations=pd.read_csv(STATIONS_CSV))


def summary(data: dict) -> None:
    long, colloc, ls = data["long"], data["colloc"], data["lcs_station"]
    c = long.groupby("cls").agg(rows=("y_raw", "size"), cells=("cell_id", "nunique"),
                                rh_site_coverage=("rh_site", lambda x: x.notna().mean()))
    print(c.round(3).to_string())
    print(f"time {long.time_lt.min()} → {long.time_lt.max()} · rows per fold "
          f"{long.outer_fold.value_counts().sort_index().to_dict()}")
    print(f"LCS station-hours {len(ls):,} · on-site RH coverage {ls.rh.notna().mean():.3f} · "
          f"RH>95 share {(ls.rh > 95).mean():.3f}")
    for name, p in colloc.items():
        print(f"collocated: {name} ↔ {p['kk']} ({p['km']:.2f} km, cell {p['lcs_cell']}) · "
              f"{len(p['df'])} h {p['df'].index.min()} → {p['df'].index.max()}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--refresh", action="store_true")
    out = build(refresh=ap.parse_args().refresh, verbose=False)
    summary(out)
