"""CNEMC hourly PM2.5 → QC → 0.05° cell-hour labels.

Following the reference papers:
    * drop null values; keep 08:00–17:00 Beijing time (Wang et al. 2024, ASTMNN)
    * average all stations falling in one grid cell (Li et al. 2020, 2024; Wang et al. 2024)
Added here because the papers give no outlier rules and raw CNEMC contains clear errors
(max 200,000 µg/m³, stuck analysers):
    1. range      outside [PM_RANGE] µg/m³
    2. spike      ≥ SPIKE_RATIO × and ≥ SPIKE_MIN_JUMP above BOTH the previous and next hour of the same station
    3. stuck      ≥ CONST_RUN_HOURS consecutive hours with the identical value > CONST_RUN_MIN_VALUE
QC runs on each station's full series (all 24 h) before the daytime window is applied, so spikes and
runs at the window edges are judged with their real neighbours.

Time: `time_bjt` is the hour-ending Beijing time stamp; time_utc = time_bjt − 8 h.
"""
import numpy as np
import pandas as pd

import cells as K
import config as C


def load(stations: pd.DataFrame) -> pd.DataFrame:
    years = range(pd.Timestamp(C.START).year, pd.Timestamp(C.END).year + 1)
    keep = set(stations.station_code)
    parts = []
    for y in years:
        d = pd.read_csv(C.PM25_DIR / f"china_pm25_{y}.csv.gz", dtype={"station_code": str, "pm25": "float32"})
        parts.append(d[d.station_code.isin(keep)])
    d = pd.concat(parts, ignore_index=True)
    d["time_bjt"] = pd.to_datetime(d.time_bjt, format="%Y-%m-%d %H:%M:%S")
    return d.dropna(subset=["pm25"]).drop_duplicates(["station_code", "time_bjt"])


def qc(d: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    d = d.sort_values(["station_code", "time_bjt"]).reset_index(drop=True)
    n0 = len(d)
    bad_range = ~d.pm25.between(*C.PM_RANGE)

    st = d.station_code.to_numpy()
    t = d.time_bjt.to_numpy()
    v = d.pm25.to_numpy(float)
    same_prev = np.r_[False, (st[1:] == st[:-1]) & (t[1:] - t[:-1] == np.timedelta64(1, "h"))]
    same_next = np.r_[same_prev[1:], False]
    prev = np.where(same_prev, np.r_[np.nan, v[:-1]], np.nan)
    nxt = np.where(same_next, np.r_[v[1:], np.nan], np.nan)
    ref = np.fmax(prev, nxt)
    spike = same_prev & same_next & (v >= C.SPIKE_RATIO * ref) & (v - ref >= C.SPIKE_MIN_JUMP)

    # run id increments whenever station, hourly continuity or value changes
    new_run = ~(same_prev & (v == np.r_[np.nan, v[:-1]]))
    run_id = np.cumsum(new_run)
    run_len = pd.Series(run_id).map(pd.Series(run_id).value_counts()).to_numpy()
    stuck = (run_len >= C.CONST_RUN_HOURS) & (v > C.CONST_RUN_MIN_VALUE)

    bad = bad_range.to_numpy() | spike | stuck
    stats = dict(rows=n0, range=int(bad_range.sum()), spike=int(spike.sum()), stuck=int(stuck.sum()),
                 removed=int(bad.sum()), removed_pct=round(100 * bad.mean(), 3))
    return d[~bad].reset_index(drop=True), stats


def to_cells(d: pd.DataFrame, stations: pd.DataFrame) -> pd.DataFrame:
    """Cell-hour mean over stations; only the UTC hours of the pilot window."""
    d = d.merge(stations[["station_code", "cell_id"]], on="station_code")
    d["time_utc"] = d.time_bjt - pd.Timedelta(hours=8)
    d = d[d.time_utc.dt.hour.isin(list(C.UTC_HOURS))
          & d.time_utc.between(pd.Timestamp(C.START), pd.Timestamp(C.END) + pd.Timedelta(hours=23))]
    g = d.groupby(["cell_id", "time_utc"]).pm25
    out = g.agg(pm25="mean", n_stations="size").reset_index()
    out["pm25_sd"] = g.std(ddof=0).to_numpy()
    out["pm25"] = out.pm25.astype(np.float32)
    return out


if __name__ == "__main__":
    st = K.stations()
    st = st[st.region.notna()]
    raw = load(st)
    clean, stats = qc(raw)
    print(f"[pm25] QC on {stats['rows']:,} station-hours: range {stats['range']:,} · spike {stats['spike']:,} · "
          f"stuck {stats['stuck']:,} → removed {stats['removed']:,} ({stats['removed_pct']}%)")
    cell = to_cells(clean, st)
    C.OUT.mkdir(parents=True, exist_ok=True)
    cell.to_pickle(C.OUT / "pm25_cells.pkl")
    print(f"[pm25] {len(cell):,} cell-hours · {cell.cell_id.nunique()} cells · "
          f"{cell.time_utc.min()} → {cell.time_utc.max()} UTC")
