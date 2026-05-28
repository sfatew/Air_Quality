"""
Download ERA5 single-level data for the Vietnam bounding box via CDS API.

Strategy
────────
  Two CDS requests per month (instant + accumulated variables submitted
  separately → smaller jobs, faster CDS queue scheduling).
  Checkpoint .nc file per month → merge at the end.
  Restarting the script skips months already on disk.

Requires
────────
  pip install cdsapi xarray netCDF4 scipy
  ~/.cdsapirc  (or CDSAPI_URL / CDSAPI_KEY env vars) with valid credentials.
  CDS account: https://cds.climate.copernicus.eu

Output variables (all saved in the final NetCDF)
────────────────────────────────────────────────
  T2m          2-m temperature                        [K → °C]
  Td2m         2-m dewpoint temperature               [K → °C]
  RH           2-m relative humidity (derived)        [%]
  Psfc         surface pressure                       [Pa → hPa]
  U10          10-m U wind component (eastward)       [m/s]
  V10          10-m V wind component (northward)      [m/s]
  WS10m        10-m wind speed (derived)              [m/s]
  WD10m        10-m wind direction met. (derived)     [°]
  PBLH         boundary layer height                  [m]
  CloudCover   total cloud cover                      [0–1 → %]
  SolarRad     surface solar radiation downwelling    [J/m² → W/m²]  ← deaccumulated
  Precip       total precipitation                    [m → mm]       ← deaccumulated
  Albedo       forecast surface albedo                [0–1 → %]

Notes on deaccumulation
────────────────────────
  ERA5 accumulated fields (ssrd, tp) reset at each 12-h forecast run
  initialised at 00Z and 12Z UTC.  Step 1 of each run (01Z and 13Z UTC)
  already represents one hour of accumulation, so no diff is needed there.
  IMPORTANT: deaccumulation is performed in UTC BEFORE the UTC+7 shift.
  Do not reorder these two operations.
"""

import calendar
import glob
import os
import tempfile
import zipfile

import cdsapi
import numpy as np
import pandas as pd
import xarray as xr

# ── Configuration ──────────────────────────────────────────────────────────────
BBOX         = [23.5, 102.0, 8.0, 110.0]   # [N, W, S, E] — CDS convention
START        = (2022, 9)
END          = (2026, 4)

OUTPUT_DIR   = "/home/slow_data/Air_Quality/ERA5"
OUTPUT_FILE  = os.path.join(OUTPUT_DIR, "Vietnam_ERA5_bbox.nc")
MONTHLY_DIR  = os.path.join(OUTPUT_DIR, "_monthly_raw")

# ── Split variables by step type for separate CDS requests ────────────────────
# FIX (performance): mixing instant+accum in one request forces CDS to split
# them internally and serialise the queue jobs.  Two small requests schedule
# faster and can run concurrently on the CDS back-end.

CDS_INSTANT_VARS = [
    "2m_temperature",                      # t2m  — instantaneous [K]
    "2m_dewpoint_temperature",             # d2m  — instantaneous [K]
    "surface_pressure",                    # sp   — instantaneous [Pa]
    "10m_u_component_of_wind",             # u10  — instantaneous [m/s]
    "10m_v_component_of_wind",             # v10  — instantaneous [m/s]
    "boundary_layer_height",               # blh  — instantaneous [m]
    "total_cloud_cover",                   # tcc  — instantaneous [0–1]
    "forecast_albedo",                     # fal  — instantaneous [0–1]
]

CDS_ACCUM_VARS = [
    "surface_solar_radiation_downwards",   # ssrd — accumulated   [J/m²]
    "total_precipitation",                 # tp   — accumulated   [m]
]

# Short CDS names for the accumulated variables (used in deaccumulation)
ACCUM_SHORT = {"ssrd", "tp"}

os.makedirs(OUTPUT_DIR,  exist_ok=True)
os.makedirs(MONTHLY_DIR, exist_ok=True)


# ── Month iterator ─────────────────────────────────────────────────────────────
def iter_months(start: tuple[int, int], end: tuple[int, int]):
    y, m = start
    while (y, m) <= end:
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1


# ── CDS download ───────────────────────────────────────────────────────────────
def _build_request(year: int, month: int, variables: list[str]) -> dict:
    """Build the CDS API request body for a single month and variable list."""
    n_days   = calendar.monthrange(year, month)[1]
    all_days = [f"{d:02d}" for d in range(1, n_days + 1)]
    all_hrs  = [f"{h:02d}:00" for h in range(24)]
    return {
        "product_type": "reanalysis",
        "variable":     variables,
        "year":         str(year),
        "month":        f"{month:02d}",
        "day":          all_days,
        "time":         all_hrs,
        "area":         BBOX,
        "data_format":  "netcdf",   # FIX: explicit format avoids ambiguous server
                                    # behaviour; new CDS API key is 'data_format'
    }


def download_month(c: cdsapi.Client, year: int, month: int,
                   out_instant: str, out_accum: str) -> None:
    """Submit two separate CDS requests (instant / accum) for one month."""
    print(f"    → requesting instant variables …")
    c.retrieve(
        "reanalysis-era5-single-levels",
        _build_request(year, month, CDS_INSTANT_VARS),
        out_instant,
    )
    print(f"    → requesting accumulated variables …")
    c.retrieve(
        "reanalysis-era5-single-levels",
        _build_request(year, month, CDS_ACCUM_VARS),
        out_accum,
    )


# ── Open a CDS download (zip or plain nc) ─────────────────────────────────────
def _open_one_file(path: str) -> xr.Dataset:
    """
    Open a single CDS download, handling both ZIP and plain NetCDF formats.

    FIX (critical): xr.open_dataset is lazy.  We call .load() inside the
    context manager so all data is read into memory before the temp directory
    (or the original file handle) is released.

    FIX (robustness): the new CDS API may return either a .zip containing one
    or more .nc files, or a plain .nc directly.  Both are handled here.
    """
    if zipfile.is_zipfile(path):
        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(path) as z:
                z.extractall(tmp)
            nc_files = [
                os.path.join(tmp, name)
                for name in os.listdir(tmp)
                if name.endswith(".nc")
            ]
            if not nc_files:
                raise RuntimeError(f"No .nc files found inside zip: {path}")
            parts = [xr.open_dataset(f).load() for f in nc_files]  # .load() ← FIX
            ds = xr.merge(parts, compat="no_conflicts") if len(parts) > 1 else parts[0]
    else:
        # Plain NetCDF — load immediately so the file handle is not kept open
        ds = xr.open_dataset(path).load()

    # Normalise dimension name: new CDS API uses 'valid_time', old uses 'time'
    if "valid_time" in ds.dims:
        ds = ds.rename({"valid_time": "time"})
    if "valid_time" in ds.coords and "valid_time" not in ds.dims:
        ds = ds.drop_vars("valid_time")

    return ds


def open_month_datasets(instant_path: str, accum_path: str) -> xr.Dataset:
    """Load and merge the two monthly download files."""
    ds_instant = _open_one_file(instant_path)
    ds_accum   = _open_one_file(accum_path)

    # Sanity-check time alignment before merging
    if not ds_instant.time.equals(ds_accum.time):
        raise ValueError(
            "Time axes of instant and accum downloads do not match — "
            "CDS may have returned inconsistent data."
        )
    return xr.merge([ds_instant, ds_accum], compat="no_conflicts")


# ── Deaccumulation ─────────────────────────────────────────────────────────────
def deaccumulate_era5(da: xr.DataArray) -> xr.DataArray:
    """
    Convert ERA5 accumulated fields (tp, ssrd) to per-hour values.

    ERA5 accumulates from 00Z and 12Z forecast runs.  Step 1 of each run
    (01Z and 13Z UTC) already IS one hour of accumulation; all other steps
    are differenced from the previous time step within the same run.

    IMPORTANT: call this function on UTC-stamped data BEFORE applying the
    UTC+7 offset.  The reset-hour logic (h == 1 or h == 13) is UTC-specific.

    FIX (robustness): we verify that the time axis is contiguous (no missing
    hours) before differencing; a gap would produce nonsense values.
    """
    times = pd.DatetimeIndex(da.time.values)

    # Check for gaps
    expected_freq = pd.tseries.frequencies.to_offset("1h")
    inferred      = pd.infer_freq(times)
    if inferred not in ("h", "H", "T", None):   # 'h'/'H' = hourly in pandas
        pass  # infer_freq is fragile for long series; use diff check instead
    diffs = np.diff(times.asi8) // int(1e9)      # seconds between steps
    if not np.all(diffs == 3600):
        raise ValueError(
            f"Time axis of '{da.name}' is not contiguous hourly — "
            f"deaccumulation would produce incorrect results. "
            f"Unique gaps (s): {np.unique(diffs)}"
        )

    values = da.values.astype(np.float64)   # (time, lat, lon)
    hours  = times.hour                     # UTC hours — must be UTC, not UTC+7
    result = np.empty_like(values)

    result[0] = values[0]   # first step: no previous step to diff against
    for t in range(1, len(hours)):
        h = int(hours[t])
        if h in (1, 13):
            # Step 1 of a new forecast run: the stored value is already the
            # 1-h accumulation for this step (no reset artefact to remove)
            result[t] = values[t]
        else:
            # All other steps: diff within the same run
            result[t] = np.maximum(values[t] - values[t - 1], 0.0)

    return da.copy(data=result.astype(np.float32))


# ── Unit conversions & derived variables ──────────────────────────────────────
def postprocess(ds: xr.Dataset) -> xr.Dataset:
    """
    Deaccumulate, convert units, derive RH / WS / WD, rename, add CF attrs.
    Must be called on UTC-timestamped data (before UTC+7 shift).
    """
    rename_map = {
        "t2m":   "T2m",
        "d2m":   "Td2m",
        "sp":    "Psfc",
        "u10":   "U10",
        "v10":   "V10",
        "blh":   "PBLH",
        "tcc":   "CloudCover",
        "ssrd":  "SolarRad",
        "tp":    "Precip",
        "fal":   "Albedo",
    }

    # Deaccumulate BEFORE unit conversion and BEFORE UTC+7 shift
    for short in ACCUM_SHORT:
        if short in ds:
            ds[short] = deaccumulate_era5(ds[short])

    # Rename to output variable names
    present = {k: v for k, v in rename_map.items() if k in ds}
    ds = ds.rename(present)

    # Unit conversions
    if "T2m"      in ds: ds["T2m"]      = ds["T2m"]  - 273.15          # K → °C
    if "Td2m"     in ds: ds["Td2m"]     = ds["Td2m"] - 273.15          # K → °C
    if "Psfc"     in ds: ds["Psfc"]     = ds["Psfc"] / 100.0           # Pa → hPa
    if "SolarRad" in ds:
        ds["SolarRad"] = (ds["SolarRad"] / 3600.0).clip(min=0)         # J/m²/h → W/m²
    if "Precip"   in ds:
        ds["Precip"]   = (ds["Precip"]   * 1000.0).clip(min=0)         # m → mm
    if "CloudCover" in ds: ds["CloudCover"] = ds["CloudCover"] * 100    # 0–1 → %
    if "Albedo"     in ds: ds["Albedo"]     = ds["Albedo"]     * 100    # 0–1 → %

    # Relative humidity — August-Roche-Magnus approximation (T, Td in °C)
    if "T2m" in ds and "Td2m" in ds:
        T, Td = ds["T2m"], ds["Td2m"]
        rh = (
            100.0
            * np.exp(17.625 * Td / (243.04 + Td))
            / np.exp(17.625 * T  / (243.04 + T))
        )
        ds["RH"] = rh.clip(0, 100).astype(np.float32)

    # 10-m wind speed and direction (meteorological convention)
    if "U10" in ds and "V10" in ds:
        u, v = ds["U10"], ds["V10"]
        ds["WS10m"] = np.sqrt(u**2 + v**2).astype(np.float32)
        # Direction FROM which wind blows, measured clockwise from North
        ds["WD10m"] = ((270.0 - np.degrees(np.arctan2(v, u))) % 360.0).astype(np.float32)

    # CF-style variable attributes
    cf_attrs = {
        "T2m":        ("2-metre air temperature",                  "degC"),
        "Td2m":       ("2-metre dewpoint temperature",             "degC"),
        "RH":         ("2-metre relative humidity",                "%"),
        "Psfc":       ("surface pressure",                         "hPa"),
        "U10":        ("10-m U wind component (eastward)",         "m s-1"),
        "V10":        ("10-m V wind component (northward)",        "m s-1"),
        "WS10m":      ("10-m wind speed",                          "m s-1"),
        "WD10m":      ("10-m wind direction (meteorological)",     "degrees"),
        "PBLH":       ("planetary boundary layer height",          "m"),
        "CloudCover": ("total cloud cover",                        "%"),
        "SolarRad":   ("surface solar radiation downwelling",      "W m-2"),
        "Precip":     ("total precipitation",                      "mm"),
        "Albedo":     ("forecast surface albedo",                  "%"),
    }
    for var, (long_name, units) in cf_attrs.items():
        if var in ds:
            ds[var].attrs = {"long_name": long_name, "units": units}

    return ds


# ── Main ──────────────────────────────────────────────────────────────────────
def fetch_bbox() -> None:
    months = list(iter_months(START, END))
    print(f"ERA5 bounding-box download: {len(months)} months")
    print(f"Bbox : N={BBOX[0]} W={BBOX[1]} S={BBOX[2]} E={BBOX[3]}")
    print(f"Range: {START[0]}-{START[1]:02d}  →  {END[0]}-{END[1]:02d}\n")

    c = cdsapi.Client()

    for i, (year, month) in enumerate(months):
        tag      = f"{year}{month:02d}"
        clean_nc = os.path.join(MONTHLY_DIR, f"era5_{tag}.nc")

        if os.path.exists(clean_nc):
            print(f"[{i+1:2d}/{len(months)}] {year}-{month:02d}  skip (cached)")
            continue

        raw_instant = os.path.join(MONTHLY_DIR, f"era5_{tag}_instant.nc")
        raw_accum   = os.path.join(MONTHLY_DIR, f"era5_{tag}_accum.nc")

        print(f"[{i+1:2d}/{len(months)}] {year}-{month:02d}  requesting …")
        download_month(c, year, month, raw_instant, raw_accum)

        print(f"    → post-processing …")
        ds_raw   = open_month_datasets(raw_instant, raw_accum)
        ds_clean = postprocess(ds_raw)   # ← still in UTC here

        # Shift timestamps to UTC+7 (Vietnam Standard Time) AFTER deaccumulation
        times_utc7 = ds_clean.time.values + np.timedelta64(7 * 3600, "s")
        ds_clean   = ds_clean.assign_coords(time=times_utc7)
        ds_clean.time.attrs = {
            "long_name": "time",
            "note":      "UTC+7 (Asia/Bangkok), timezone-naive",
        }

        ds_clean.to_netcdf(clean_nc)

        # Clean up raw downloads only after successful save
        for f in (raw_instant, raw_accum):
            if os.path.exists(f):
                os.remove(f)

        print(f"    → saved  → {os.path.basename(clean_nc)}")

    # ── Merge monthly files ────────────────────────────────────────────────────
    monthly_files = sorted(glob.glob(os.path.join(MONTHLY_DIR, "era5_??????.nc")))
    if not monthly_files:
        print("No monthly files found — nothing to merge.")
        return

    print(f"\nMerging {len(monthly_files)} monthly files → {OUTPUT_FILE}")
    ds_all = xr.open_mfdataset(monthly_files, combine="by_coords")

    # FIX: use calendar.monthrange to get the true last day of END month,
    # instead of hardcoding day 28 which drops data in months with 29–31 days.
    last_day = calendar.monthrange(END[0], END[1])[1]
    t_end    = pd.Timestamp(f"{END[0]}-{END[1]:02d}-{last_day:02d} 23:00")
    ds_all   = ds_all.sel(time=slice(None, t_end))

    # Normalise spatial dimension names (CDS uses 'latitude'/'longitude')
    rename_dims = {}
    if "latitude"  not in ds_all.dims and "lat" in ds_all.dims:
        rename_dims["lat"] = "latitude"
    if "longitude" not in ds_all.dims and "lon" in ds_all.dims:
        rename_dims["lon"] = "longitude"
    if rename_dims:
        ds_all = ds_all.rename(rename_dims)

    ds_all.attrs = {
        "title":           "Vietnam ERA5 single-level weather",
        "bounding_box":    f"N={BBOX[0]} W={BBOX[1]} S={BBOX[2]} E={BBOX[3]}",
        "date_range":      f"{START[0]}-{START[1]:02d} to {END[0]}-{END[1]:02d}",
        "source":          "ERA5 reanalysis — Copernicus Climate Data Store (CDS API)",
        "timezone":        "UTC+7 (Asia/Bangkok), tz-naive timestamps",
        "grid_resolution": "0.25 degrees",
        "created_by":      "ERA5/fetch_era5_bbox.py",
    }

    ds_all.to_netcdf(OUTPUT_FILE)
    print(f"Done → {OUTPUT_FILE}")


if __name__ == "__main__":
    fetch_bbox()
