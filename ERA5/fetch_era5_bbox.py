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
  Timestamps stay in UTC throughout — no timezone shift is applied.
"""

import calendar
import glob
import os
import tempfile
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import cdsapi
import numpy as np
import pandas as pd
import xarray as xr

# ── Configuration ──────────────────────────────────────────────────────────────
BBOX         = [23.5, 102.0, 8.0, 110.0]   # [N, W, S, E] — CDS convention
START        = (2020, 1)
END          = (2022, 1)

# How many months to download concurrently.
# CDS fair-use limit is ~20 active jobs per user; 2 jobs per month → keep ≤8.
MAX_WORKERS  = 2

# Retry settings for CDS queue-rejection errors
MAX_RETRIES  = 5      # max attempts per month
RETRY_DELAY  = 120    # seconds before first retry (doubles each attempt)

OUTPUT_DIR   = "/home/slow_data/Air_Quality/ERA5"
OUTPUT_FILE  = os.path.join(OUTPUT_DIR, "Vietnam_ERA5_bbox.nc")
MONTHLY_DIR  = os.path.join(OUTPUT_DIR, "_monthly_raw")

# Skip the giant single-file merge.  Downstream code should use ERA5/load.py
# (xr.open_mfdataset over MONTHLY_DIR), which is more robust, uses less disk,
# and extends naturally when new months are added.
MERGE_MONTHLY = False

# ── Split variables by step type for separate CDS requests ────────────────────
# FIX (performance): mixing instant+accum in one request forces CDS to split
# them internally and serialise the queue jobs.  Two small requests schedule
# faster and can run concurrently on the CDS back-end.

CDS_INSTANT_VARS = [
    "2m_temperature",                      # t2m  — instantaneous [K]
    "2m_dewpoint_temperature",             # d2m  — instantaneous [K]
    "surface_pressure",                    # sp   — instantaneous [Pa]
    "mean_sea_level_pressure",             # msl  — instantaneous [Pa]
    "10m_u_component_of_wind",             # u10  — instantaneous [m/s]
    "10m_v_component_of_wind",             # v10  — instantaneous [m/s]
    "100m_u_component_of_wind",            # u100 — instantaneous [m/s]
    "100m_v_component_of_wind",            # v100 — instantaneous [m/s]
    "boundary_layer_height",               # blh  — instantaneous [m]
    "total_cloud_cover",                   # tcc  — instantaneous [0–1]
    "cloud_base_height",                   # cbh  — instantaneous [m]
    "total_column_water_vapour",           # tcwv — instantaneous [kg m-2]
    "forecast_albedo",                     # fal  — instantaneous [0–1]
    "convective_available_potential_energy",  # cape — instantaneous [J kg-1]
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


def download_month(year: int, month: int,
                   out_instant: str, out_accum: str) -> None:
    """
    Submit instant and accumulated CDS requests concurrently for one month.
    Each thread gets its own cdsapi.Client so sessions are not shared.
    """
    def _fetch(variables, out_path):
        # quiet=True suppresses per-request progress spam when many months run
        client = cdsapi.Client(quiet=True)
        client.retrieve(
            "reanalysis-era5-single-levels",
            _build_request(year, month, variables),
            out_path,
        )

    with ThreadPoolExecutor(max_workers=2) as ex:
        fi = ex.submit(_fetch, CDS_INSTANT_VARS, out_instant)
        fa = ex.submit(_fetch, CDS_ACCUM_VARS,   out_accum)
        fi.result()   # re-raises any exception from the thread
        fa.result()


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

    t_i = ds_instant.time.values
    t_a = ds_accum.time.values

    if not ds_instant.time.equals(ds_accum.time):
        # Diagnose the mismatch before deciding what to do
        _safe_print(
            f"    ⚠ time mismatch: instant has {len(t_i)} steps "
            f"({t_i[0]} … {t_i[-1]}), "
            f"accum has {len(t_a)} steps ({t_a[0]} … {t_a[-1]})"
        )
        # Tolerate a minor tail difference (CDS sometimes returns N vs N±1 hours
        # for the most recent months).  Take the intersection.
        common = np.intersect1d(t_i, t_a)
        if len(common) == 0:
            msg = (
                "Time axes of instant and accum downloads share no common timestamps"
                " — CDS returned completely inconsistent data.\n"
                f"  instant: {len(t_i)} steps  {t_i[0]} ... {t_i[-1]}\n"
                f"  accum  : {len(t_a)} steps  {t_a[0]} ... {t_a[-1]}"
            )
            raise ValueError(msg)
        dropped = max(len(t_i), len(t_a)) - len(common)
        _safe_print(f"    → using {len(common)} common timestamps (dropped {dropped} non-overlapping)")
        ds_instant = ds_instant.sel(time=common)
        ds_accum   = ds_accum.sel(time=common)

    # Drop CDS metadata variables that differ between instant/accum files
    # (expver = experiment version, changes for recent near-real-time data)
    for drop_var in ("expver", "number"):
        if drop_var in ds_instant:
            ds_instant = ds_instant.drop_vars(drop_var)
        if drop_var in ds_accum:
            ds_accum = ds_accum.drop_vars(drop_var)

    return xr.merge([ds_instant, ds_accum], compat="no_conflicts")


# ── Deaccumulation ─────────────────────────────────────────────────────────────
def deaccumulate_era5(da: xr.DataArray) -> xr.DataArray:
    """
    Convert ERA5 accumulated fields (tp, ssrd) to per-hour values.

    ERA5 accumulates from 00Z and 12Z forecast runs.  Step 1 of each run
    (01Z and 13Z UTC) already IS one hour of accumulation; all other steps
    are differenced from the previous time step within the same run.

    The reset-hour logic (h == 1 or h == 13) is UTC-specific; this script
    keeps timestamps in UTC throughout, so the check is always valid.

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
    hours  = times.hour                     # UTC hours
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
    Operates on UTC-timestamped data; no timezone shift is applied.
    """
    rename_map = {
        "t2m":   "T2m",
        "d2m":   "Td2m",
        "sp":    "Psfc",
        "msl":   "MSLP",
        "u10":   "U10",
        "v10":   "V10",
        "u100":  "U100",
        "v100":  "V100",
        "blh":   "PBLH",
        "tcc":   "CloudCover",
        "cbh":   "CBH",
        "tcwv":  "TCWV",
        "ssrd":  "SolarRad",
        "tp":    "Precip",
        "fal":   "Albedo",
        "cape":  "CAPE",
    }

    # Deaccumulate BEFORE unit conversion (the per-hour values are then
    # scaled into W/m² and mm by the conversions below)
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
    if "MSLP"     in ds: ds["MSLP"]     = ds["MSLP"] / 100.0           # Pa → hPa
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
        "MSLP":       ("mean sea level pressure",                  "hPa"),
        "U10":        ("10-m U wind component (eastward)",         "m s-1"),
        "V10":        ("10-m V wind component (northward)",        "m s-1"),
        "WS10m":      ("10-m wind speed",                          "m s-1"),
        "WD10m":      ("10-m wind direction (meteorological)",     "degrees"),
        "U100":       ("100-m U wind component (eastward)",        "m s-1"),
        "V100":       ("100-m V wind component (northward)",       "m s-1"),
        "PBLH":       ("planetary boundary layer height",          "m"),
        "CloudCover": ("total cloud cover",                        "%"),
        "CBH":        ("cloud base height",                        "m"),
        "TCWV":       ("total column water vapour",                "kg m-2"),
        "SolarRad":   ("surface solar radiation downwelling",      "W m-2"),
        "Precip":     ("total precipitation",                      "mm"),
        "Albedo":     ("forecast surface albedo",                  "%"),
        "CAPE":       ("convective available potential energy",    "J kg-1"),
    }
    for var, (long_name, units) in cf_attrs.items():
        if var in ds:
            ds[var].attrs = {"long_name": long_name, "units": units}

    return ds


# ── Per-month worker (called from thread pool) ────────────────────────────────
_print_lock = threading.Lock()

def _safe_print(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs)


def _process_month(i: int, total: int, year: int, month: int) -> str:
    """
    Download + post-process one month.  Returns the path to the clean .nc file.
    Retries up to MAX_RETRIES times on CDS queue-rejection errors.
    Designed to be called from a ThreadPoolExecutor worker.
    """
    import time

    tag      = f"{year}{month:02d}"
    clean_nc = os.path.join(MONTHLY_DIR, f"era5_{tag}.nc")

    if os.path.exists(clean_nc):
        _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  skip (cached)")
        return clean_nc

    raw_instant = os.path.join(MONTHLY_DIR, f"era5_{tag}_instant.nc")
    raw_accum   = os.path.join(MONTHLY_DIR, f"era5_{tag}_accum.nc")

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        # Clean up any partial downloads from a previous attempt
        for f in (raw_instant, raw_accum):
            if os.path.exists(f):
                os.remove(f)

        try:
            if attempt == 1:
                _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  requesting …")
            else:
                delay = RETRY_DELAY * (2 ** (attempt - 2))  # 120 s, 240 s, 480 s …
                _safe_print(
                    f"[{i:2d}/{total}] {year}-{month:02d}  retry {attempt}/{MAX_RETRIES} "
                    f"(waiting {delay}s) …"
                )
                time.sleep(delay)

            download_month(year, month, raw_instant, raw_accum)
            last_exc = None
            break  # success — exit retry loop

        except Exception as exc:
            last_exc = exc
            # Broader retry: retry on ANY exception (network blips, CDS 5xx,
            # zip-corruption, transient xarray errors, etc.).  The previous
            # narrow filter on "temporarily limited"/"rejected" missed too
            # many recoverable failures.  The outer logger in fetch_bbox()
            # records the month if all attempts are exhausted.
            _safe_print(
                f"[{i:2d}/{total}] {year}-{month:02d}  attempt {attempt}/{MAX_RETRIES} "
                f"failed: {exc!r}"
            )

    if last_exc is not None:
        raise last_exc

    _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  post-processing …")
    ds_raw   = open_month_datasets(raw_instant, raw_accum)
    ds_clean = postprocess(ds_raw)

    ds_clean.time.attrs = {
        "long_name": "time",
        "note":      "UTC, timezone-naive",
    }

    ds_clean.to_netcdf(clean_nc)

    for f in (raw_instant, raw_accum):
        if os.path.exists(f):
            os.remove(f)

    _safe_print(f"[{i:2d}/{total}] {year}-{month:02d}  saved → {os.path.basename(clean_nc)}")
    return clean_nc


# ── Main ──────────────────────────────────────────────────────────────────────
def fetch_bbox() -> None:
    months = list(iter_months(START, END))
    total  = len(months)
    print(f"ERA5 bounding-box download: {total} months  (MAX_WORKERS={MAX_WORKERS})")
    print(f"Bbox : N={BBOX[0]} W={BBOX[1]} S={BBOX[2]} E={BBOX[3]}")
    print(f"Range: {START[0]}-{START[1]:02d}  →  {END[0]}-{END[1]:02d}\n")

    # Download up to MAX_WORKERS months concurrently.
    # Each month itself downloads its 2 CDS jobs (instant + accum) in parallel,
    # so the total active CDS jobs = MAX_WORKERS * 2.  Keep MAX_WORKERS ≤ 8.
    failed = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_process_month, i + 1, total, y, m): (y, m)
            for i, (y, m) in enumerate(months)
        }
        for fut in as_completed(futures):
            y, m = futures[fut]
            try:
                fut.result()
            except Exception as exc:
                _safe_print(f"  ✗  {y}-{m:02d} FAILED: {exc}")
                failed.append((y, m))

    if failed:
        print(f"\nWarning: {len(failed)} month(s) failed and will be missing from the merge:")
        for y, m in sorted(failed):
            print(f"  {y}-{m:02d}")
        log_path = os.path.join(OUTPUT_DIR, "failed_requests.log")
        with open(log_path, "w") as f:
            f.write(f"# Failed ERA5 months ({pd.Timestamp.now().isoformat()})\n")
            for y, m in sorted(failed):
                f.write(f"{y}-{m:02d}\n")
        print(f"Failed list written to: {log_path}")

    # ── Merge monthly files ────────────────────────────────────────────────────
    if not MERGE_MONTHLY:
        print(
            f"\nMerge step skipped (MERGE_MONTHLY=False). "
            f"Use ERA5/load.py → load_era5_bbox() to read the monthly files lazily."
        )
        return

    monthly_files = sorted(glob.glob(os.path.join(MONTHLY_DIR, "era5_??????.nc")))
    if not monthly_files:
        print("No monthly files found — nothing to merge.")
        return

    print(f"\nMerging {len(monthly_files)} monthly files → {OUTPUT_FILE}")
    # Streaming write: only one month is in RAM at a time (~110 MB).
    # xarray's to_netcdf(mode="a") doesn't actually extend an unlimited dim
    # (it just overwrites matching-shape vars), so we use the netCDF4 library
    # directly to append along the time axis after seeding the file with
    # month 1 via xarray.
    import netCDF4

    last_day = calendar.monthrange(END[0], END[1])[1]
    t_end    = pd.Timestamp(f"{END[0]}-{END[1]:02d}-{last_day:02d} 23:00")

    global_attrs = {
        "title":           "Vietnam ERA5 single-level weather",
        "bounding_box":    f"N={BBOX[0]} W={BBOX[1]} S={BBOX[2]} E={BBOX[3]}",
        "date_range":      f"{START[0]}-{START[1]:02d} to {END[0]}-{END[1]:02d}",
        "source":          "ERA5 reanalysis — Copernicus Climate Data Store (CDS API)",
        "timezone":        "UTC, tz-naive timestamps",
        "grid_resolution": "0.25 degrees",
        "created_by":      "ERA5/fetch_era5_bbox.py",
    }

    def _load_month(fp: str) -> xr.Dataset:
        ds = xr.open_dataset(fp, engine="netcdf4").load()
        for drop_var in ("expver", "number"):
            if drop_var in ds:
                ds = ds.drop_vars(drop_var)
        rename_dims = {}
        if "latitude"  not in ds.dims and "lat" in ds.dims:
            rename_dims["lat"] = "latitude"
        if "longitude" not in ds.dims and "lon" in ds.dims:
            rename_dims["lon"] = "longitude"
        if rename_dims:
            ds = ds.rename(rename_dims)
        return ds.sel(time=slice(None, t_end))

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    # Seed the output file with the first month (xarray handles all the
    # encoding/attrs/coords setup for us; time is the unlimited dim).
    ds0 = _load_month(monthly_files[0])
    if ds0.sizes["time"] == 0:
        raise RuntimeError(f"First monthly file is empty after slicing: {monthly_files[0]}")
    ds0.attrs = global_attrs
    ds0.to_netcdf(OUTPUT_FILE, mode="w", unlimited_dims=["time"])
    _safe_print(f"  seeded {os.path.basename(monthly_files[0])}  ({ds0.sizes['time']} hours)")
    ds0.close()

    # Append remaining months along the unlimited time axis using netCDF4.
    for fp in monthly_files[1:]:
        ds = _load_month(fp)
        n = ds.sizes["time"]
        if n == 0:
            ds.close()
            continue

        with netCDF4.Dataset(OUTPUT_FILE, mode="a") as nc:
            t_start = nc.dimensions["time"].size
            sl = slice(t_start, t_start + n)

            # Append the time coord (convert to the file's numeric units).
            time_var = nc.variables["time"]
            time_vals = netCDF4.date2num(
                pd.DatetimeIndex(ds["time"].values).to_pydatetime(),
                units=time_var.units,
                calendar=getattr(time_var, "calendar", "standard"),
            )
            time_var[sl] = time_vals

            # Append every data variable that has a time dimension.
            for vname, da in ds.data_vars.items():
                if "time" not in da.dims or vname not in nc.variables:
                    continue
                nc.variables[vname][sl] = da.values

        _safe_print(f"  appended {os.path.basename(fp)}  ({n} hours)")
        ds.close()

    print(f"Done → {OUTPUT_FILE}")


if __name__ == "__main__":
    fetch_bbox()
