"""ERA5 (0.25°, hourly UTC) → station-cell centres, bilinear, at the pilot's UTC hours.

Values at HH:00 UTC are instantaneous; SolarRad and Precip are 1-h accumulations over [HH−1, HH)
(unit-converted by Data_Collection/ERA5/fetch_era5_bbox.py), i.e. the same window as the PM label ending at HH.
Bilinear interpolation to the cell centre follows the Vietnam frame (pm25_map/model/_data.py) and Li et al.
"""
import numpy as np
import pandas as pd
import xarray as xr

import cells as K
import config as C


def month_files() -> list:
    months = pd.period_range(C.START, C.END, freq="M")
    files = [C.ERA5_DIR / f"era5_{m.strftime('%Y%m')}.nc" for m in months]
    missing = [f.name for f in files if not f.exists()]
    if missing:
        raise FileNotFoundError(f"ERA5-China months missing: {missing}")
    return files


def extract(cells: pd.DataFrame) -> pd.DataFrame:
    """Long table: cell_id, time_utc, ERA5_VARS."""
    lat = xr.DataArray(cells.lat.to_numpy(), dims="cell")
    lon = xr.DataArray(cells.lon.to_numpy(), dims="cell")
    parts = []
    for f in month_files():
        with xr.open_dataset(f) as ds:
            ds = ds[C.ERA5_VARS].sortby("latitude")
            ds = ds.sel(time=ds.time.dt.hour.isin(list(C.UTC_HOURS)))
            ds = ds.sel(time=slice(C.START, f"{C.END} 23:00"))
            v = ds.load().interp(latitude=lat, longitude=lon, method="linear")
        df = v.to_dataframe().reset_index()
        df["cell_id"] = cells.cell_id.to_numpy()[df.cell.to_numpy()]
        parts.append(df.rename(columns={"time": "time_utc"})[["cell_id", "time_utc", *C.ERA5_VARS]])
        print(f"[era5] {f.name}: {len(df):,} cell-hours", flush=True)
    out = pd.concat(parts, ignore_index=True)
    out[C.ERA5_VARS] = out[C.ERA5_VARS].astype(np.float32)
    return out


if __name__ == "__main__":
    c = K.cells()
    C.OUT.mkdir(parents=True, exist_ok=True)
    e = extract(c)
    e.to_pickle(C.OUT / "era5_cells.pkl")
    print(f"[era5] {len(e):,} rows · {e.cell_id.nunique()} cells · NaN share {e[C.ERA5_VARS].isna().mean().max():.4f}")
