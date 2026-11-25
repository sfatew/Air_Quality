"""Join PM2.5, ERA5, MAIAC AOD and DEM into the cell-hour frame used by the TFL experiments.

Rows: station cells of the three regions × UTC hours 00–09 with a QC'd PM2.5 label and ERA5.
Features (FEATS):
    ERA5 ×11, elev, elev_std, aod (NaN when no clear MAIAC retrieval — LightGBM handles missing natively),
    x, y        cell centre on the sphere in km (Wang et al. 2024 turn lon/lat into Cartesian x, y; DEM gives z)
    hour_sin/cos, doy_sin/doy_cos   cyclic time of day (Wang et al. 2024 use sin 2π·hour/24) and season
SIM_FEATS (for the LDF neighbourhood cloud — "geographical, topographical and meteorological", Gupta et al.):
    x, y, elev, ERA5 ×11
"""
import numpy as np
import pandas as pd

import cells as K
import config as C

R_KM = 6371.0
AOD_FEATS = ["aod"]
GEO_FEATS = ["x", "y"]
TIME_FEATS = ["hour_sin", "hour_cos", "doy_sin", "doy_cos"]
FEATS = C.ERA5_VARS + C.DEM_VARS + AOD_FEATS + GEO_FEATS + TIME_FEATS
SIM_FEATS = GEO_FEATS + ["elev"] + C.ERA5_VARS
FRAME = C.OUT / "frame.pkl"


def build() -> pd.DataFrame:
    pm = pd.read_pickle(C.OUT / "pm25_cells.pkl")
    era = pd.read_pickle(C.OUT / "era5_cells.pkl")
    aod = pd.read_pickle(C.OUT / "aod_maiac_cells.pkl")
    dem = pd.read_pickle(C.OUT / "dem_cells.pkl")
    cells = K.cells()[["cell_id", "region", "lat", "lon"]]

    df = pm.merge(era, on=["cell_id", "time_utc"], how="inner")
    df = df.merge(aod, on=["cell_id", "time_utc"], how="left").merge(dem, on="cell_id").merge(cells, on="cell_id")
    df["aod_npix"] = df.aod_npix.fillna(0).astype(np.int32)
    df = df.dropna(subset=C.ERA5_VARS)

    lat, lon = np.radians(df.lat.to_numpy()), np.radians(df.lon.to_numpy())
    df["x"] = (R_KM * np.cos(lat) * np.cos(lon)).astype(np.float32)
    df["y"] = (R_KM * np.cos(lat) * np.sin(lon)).astype(np.float32)
    h = df.time_utc.dt.hour.to_numpy()
    doy = df.time_utc.dt.dayofyear.to_numpy()
    df["hour_sin"], df["hour_cos"] = np.sin(2 * np.pi * h / 24), np.cos(2 * np.pi * h / 24)
    df["doy_sin"], df["doy_cos"] = np.sin(2 * np.pi * doy / 365.25), np.cos(2 * np.pi * doy / 365.25)
    df[FEATS] = df[FEATS].astype(np.float32)
    cols = ["cell_id", "time_utc", "region", "lat", "lon", "pm25", "n_stations", "pm25_sd", "aod_npix", *FEATS]
    return df[cols].sort_values(["time_utc", "cell_id"]).reset_index(drop=True)


def load() -> pd.DataFrame:
    return pd.read_pickle(FRAME)


if __name__ == "__main__":
    df = build()
    df.to_pickle(FRAME)
    print(f"[frame] {len(df):,} cell-hours · {df.cell_id.nunique()} cells · AOD present {df.aod.notna().mean():.1%}")
    print(df.groupby("region").agg(cells=("cell_id", "nunique"), rows=("pm25", "size"), pm_mean=("pm25", "mean"),
                                   aod_share=("aod", lambda s: s.notna().mean())).round(3).to_string())
