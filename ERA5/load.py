"""
Lazy loader for the ERA5 monthly checkpoint files.

Use this instead of opening a single merged .nc file — the monthly files are
already self-contained, and xr.open_mfdataset stitches them into one virtual
Dataset with a continuous time axis.  Time-series ops (.sel/.resample/.rolling/
.groupby) work transparently across the file boundaries.

Example
───────
    from ERA5.load import load_era5_bbox

    ds = load_era5_bbox()
    ds_april = ds.sel(time=slice("2024-04-01", "2024-04-30"))
    t2m_daily_mean = ds["T2m"].resample(time="1D").mean()
"""

import glob
import os

import xarray as xr

MONTHLY_DIR = "/home/slow_data/Air_Quality/ERA5/_monthly_raw"
MONTHLY_GLOB = os.path.join(MONTHLY_DIR, "era5_??????.nc")


def load_era5_bbox(
    monthly_dir: str = MONTHLY_DIR,
    chunks: dict | None = None,
) -> xr.Dataset:
    """
    Open all monthly ERA5 checkpoints as a single lazy Dataset.

    Parameters
    ──────────
    monthly_dir : str
        Directory containing era5_YYYYMM.nc files.
    chunks : dict | None
        Optional dask chunking, e.g. {"time": 720}.  Passing None opens
        without dask (pure lazy xarray); pass a dict to enable dask for
        out-of-core computations.

    Returns
    ───────
    xr.Dataset with a continuous hourly time axis spanning every month
    found on disk.
    """
    files = sorted(glob.glob(os.path.join(monthly_dir, "era5_??????.nc")))
    if not files:
        raise FileNotFoundError(f"No ERA5 monthly files under {monthly_dir}")

    return xr.open_mfdataset(
        files,
        engine="netcdf4",
        combine="by_coords",
        parallel=False,
        chunks=chunks,
        compat="no_conflicts",
        join="outer",
    )


if __name__ == "__main__":
    ds = load_era5_bbox()
    print(ds)
    print(f"\ntime: {ds.sizes['time']} steps  "
          f"({ds.time.values[0]} → {ds.time.values[-1]})")
