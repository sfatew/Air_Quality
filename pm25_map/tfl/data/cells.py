"""CNEMC stations → 0.05° cells and transfer regions."""
import numpy as np
import pandas as pd

import config as C


def cell_of(lat, lon) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(row, col, cell_id) of points on the China 0.05° grid."""
    row = np.floor((C.LAT_MAX - np.asarray(lat, float)) / C.RES).astype(int)
    col = np.floor((np.asarray(lon, float) - C.LON_MIN) / C.RES).astype(int)
    return row, col, row * C.NCOL + col


def cell_centre(cell_id) -> tuple[np.ndarray, np.ndarray]:
    cell_id = np.asarray(cell_id)
    row, col = cell_id // C.NCOL, cell_id % C.NCOL
    return C.LAT_MAX - (row + 0.5) * C.RES, C.LON_MIN + (col + 0.5) * C.RES


def region_of(lat, lon) -> np.ndarray:
    lat, lon = np.asarray(lat, float), np.asarray(lon, float)
    out = np.full(lat.shape, None, dtype=object)
    for name, r in C.REGIONS.items():
        m = (lat >= r["lat"][0]) & (lat < r["lat"][1]) & (lon >= r["lon"][0]) & (lon < r["lon"][1])
        out[m] = name
    return out


def stations() -> pd.DataFrame:
    """Trusted stations: coordinates exist and the two coordinate sources agree within MAX_SHIFT_KM
    (codes whose PM-correlation placement or relocation is uncertain are dropped, as agreed earlier)."""
    st = pd.read_csv(C.STATIONS)
    ok = st.lat.notna() & (st.shift_km.isna() | (st.shift_km <= C.MAX_SHIFT_KM))
    st = st[ok].copy()
    st["row"], st["col"], st["cell_id"] = cell_of(st.lat, st.lon)
    st["region"] = region_of(st.lat, st.lon)
    return st[["station_code", "lat", "lon", "elevation", "coord_source", "city_en", "row", "col", "cell_id", "region"]]


def cells(st: pd.DataFrame | None = None) -> pd.DataFrame:
    """One row per station cell in a transfer region: centre, region, number of stations."""
    st = stations() if st is None else st
    st = st[st.region.notna()]
    c = st.groupby("cell_id").agg(region=("region", "first"), n_stations=("station_code", "size")).reset_index()
    c["lat"], c["lon"] = cell_centre(c.cell_id)
    return c
