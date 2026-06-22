"""Stage A2 output: per-slot raw gridded AOD for every sensor.

For each 30-min UTC slot, runs Steps A1 (read + native-pixel QA) and A2
(box-average onto the 0.05° grid) for every requested sensor and writes a
single NetCDF to GRIDDED_DIR.  Downstream stages (`extract_satellite.py`,
`run_stage_a.py`) read this intermediate instead of redoing the binning,
which previously ran twice (collocate-extract + run_stage_a) and three
times across a full workflow (run_stage_a executes once before and once
after the LEO offset is built).

File schema (one file per slot: gridded_YYYYMMDD_HHMM.nc):

    Dimensions   : lat (NLAT), lon (NLON)
    Coordinates  : lat, lon (cell centres)
    Per sensor s ∈ {himawari_l2, himawari_l3, viirs_snpp,
                    viirs_noaa20, modis_maiac}:
        AOD_{s}        float32  cell mean AOD (NaN where no retrieval).
                                NOT pre-filtered for heterogeneity — apply
                                `cv_{s} <= CV_MAX` downstream if desired.
        aod_std_{s}    float32  within-cell std (NaN where n_valid_{s} < 2)
        cv_{s}         float32  std / max(mean, 0.02) — heterogeneity proxy
        n_valid_{s}    int16    native pixels actually binned into the cell
        n_total_{s}    int16    theoretical max pixels per cell at this lat
        vza_{s}        float32  mean viewing zenith angle, where available
        sza_{s}        float32  mean solar zenith angle, where available

Sensors with no valid pixel anywhere in the slot are omitted (not written
as all-NaN), so ``read_gridded_slot`` returning ``sensor not in result``
unambiguously means "no data this slot".

MODIS orbit-timestamp fallback (whole-day pooled grid) is intentionally
NOT supported by this intermediate: synthesised slot timestamps cannot be
matched to AERONET ±30 min windows, and the fallback path has not been
observed in modern MAIAC inputs.
"""

from __future__ import annotations
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

import numpy as np

from config import (
    LATS, LONS, NLAT, NLON,
    GRIDDED_DIR, SLOT_MINUTES,
)
from gridder  import bin_to_grid, grid_from_himawari
from himawari import read_l2_slot, read_l3_slot
from viirs    import read_viirs_slot
from modis    import read_modis_date, filter_modis_slot


ALL_SENSORS = (
    'himawari_l2', 'himawari_l3',
    'viirs_snpp', 'viirs_noaa20',
    'modis_maiac',
)

_SLOTS_PER_DAY = (24 * 60) // SLOT_MINUTES


# ── Path helpers ─────────────────────────────────────────────────────────────

def gridded_path(slot_utc: datetime, base: Path | str = GRIDDED_DIR) -> Path:
    base = Path(base)
    return (base
            / slot_utc.strftime('%Y') / slot_utc.strftime('%m')
            / slot_utc.strftime('%d')
            / f'gridded_{slot_utc.strftime("%Y%m%d_%H%M")}.nc')


def _slot_iter(day: date):
    base = datetime(day.year, day.month, day.day)
    for i in range(_SLOTS_PER_DAY):
        yield base + timedelta(minutes=i * SLOT_MINUTES)


# ── Slot-level gridding ──────────────────────────────────────────────────────

def _has_aod(g: Optional[dict[str, np.ndarray]]) -> bool:
    return g is not None and np.any(np.isfinite(g.get('aod_mean', np.array([]))))


def compute_slot_grids(
    slot_utc: datetime,
    sensors: tuple[str, ...] = ALL_SENSORS,
    modis_pixels: Optional[dict] = None,
) -> dict[str, dict[str, np.ndarray]]:
    """Run A1+A2 per sensor for one slot.

    Returns ``{sensor: grid_dict}`` for every sensor that produced at least
    one valid pixel.  Pass ``modis_pixels`` (from ``read_modis_date``) when
    iterating a calendar day's slots so the expensive HDF read is amortised.
    Sensors whose orbit timestamps are unavailable for the day are skipped.
    """
    out: dict[str, dict[str, np.ndarray]] = {}

    if 'himawari_l2' in sensors:
        res = read_l2_slot(slot_utc)
        if res is not None:
            g = grid_from_himawari(res)
            if _has_aod(g):
                out['himawari_l2'] = g

    if 'himawari_l3' in sensors:
        res = read_l3_slot(slot_utc)
        if res is not None:
            g = grid_from_himawari(res)
            if _has_aod(g):
                out['himawari_l3'] = g

    for sensor in ('viirs_snpp', 'viirs_noaa20'):
        if sensor not in sensors:
            continue
        px = read_viirs_slot(sensor, slot_utc)
        if px is None:
            continue
        g = bin_to_grid(px['lat'], px['lon'], px['aod'],
                        vza=px['vza'], sza=px['sza'])
        if _has_aod(g):
            out[sensor] = g

    if 'modis_maiac' in sensors and modis_pixels is not None:
        utc_sec = modis_pixels.get('orbit_utc_sec')
        # Skip MODIS for days whose tiles carry no orbit timestamps — the
        # would-be synthesised slot timestamp cannot be co-located with
        # AERONET, and would otherwise be written into every slot of the day.
        if utc_sec is not None and np.any(np.isfinite(utc_sec)):
            slot_modis = filter_modis_slot(modis_pixels, slot_utc)
            if slot_modis is not None:
                g = bin_to_grid(
                    slot_modis['lat'], slot_modis['lon'], slot_modis['aod'],
                    vza=slot_modis.get('vza'), sza=slot_modis.get('sza'),
                    ae=slot_modis.get('ae'),
                )
                if _has_aod(g):
                    out['modis_maiac'] = g

    return out


# ── NetCDF I/O ───────────────────────────────────────────────────────────────

def write_gridded_slot(
    slot_utc: datetime,
    per_sensor: dict[str, dict[str, np.ndarray]],
    base: Path | str = GRIDDED_DIR,
) -> Optional[Path]:
    """Write one slot's raw gridded data to NetCDF; returns the path written.

    Returns ``None`` (no file written) when no sensor has valid data.
    """
    if not per_sensor:
        return None

    import netCDF4 as nc

    out_path = gridded_path(slot_utc, base)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with nc.Dataset(str(out_path), 'w', format='NETCDF4') as ds:
        ds.title       = 'Stage A2 — raw gridded AOD per sensor (pre-correction)'
        ds.slot_utc    = slot_utc.isoformat()
        ds.Conventions = 'CF-1.8'

        ds.createDimension('lat', NLAT)
        ds.createDimension('lon', NLON)

        vl = ds.createVariable('lat', 'f4', ('lat',))
        vl.units = 'degrees_north'; vl.long_name = 'Latitude (cell centre)'
        vl[:] = LATS.astype(np.float32)

        vlo = ds.createVariable('lon', 'f4', ('lon',))
        vlo.units = 'degrees_east'; vlo.long_name = 'Longitude (cell centre)'
        vlo[:] = LONS.astype(np.float32)

        def _add(name, data, long_name, units='1', dtype='f4', fill=-9999.0):
            v = ds.createVariable(name, dtype, ('lat', 'lon'),
                                  fill_value=fill, zlib=True, complevel=4)
            v.long_name = long_name
            v.units     = units
            arr = np.where(np.isnan(data), fill, data) if dtype == 'f4' else data
            v[:] = arr

        for sensor, g in per_sensor.items():
            _add(f'AOD_{sensor}', g['aod_mean'],
                 f'Pre-correction gridded AOD from {sensor} '
                 '(NOT heterogeneity-filtered; apply cv threshold downstream)')
            if g.get('aod_std') is not None:
                _add(f'aod_std_{sensor}', g['aod_std'],
                     f'Within-cell AOD std dev for {sensor}')
            if g.get('cv') is not None:
                _add(f'cv_{sensor}', g['cv'],
                     f'Coefficient of variation (std / max(mean, 0.02)) for {sensor}')
            if g.get('vza_mean') is not None:
                _add(f'vza_{sensor}', g['vza_mean'],
                     f'Mean viewing zenith angle for {sensor}', units='degrees')
            if g.get('sza_mean') is not None:
                _add(f'sza_{sensor}', g['sza_mean'],
                     f'Mean solar zenith angle for {sensor}', units='degrees')
            if g.get('n_valid') is not None:
                _add(f'n_valid_{sensor}', g['n_valid'],
                     f'Number of valid native pixels binned into cell for {sensor}',
                     dtype='i2', fill=-1)
            if g.get('n_total') is not None:
                _add(f'n_total_{sensor}', g['n_total'],
                     f'Theoretical max native pixels per cell at this latitude for {sensor}',
                     dtype='i2', fill=-1)

    return out_path


def read_gridded_slot(
    slot_utc: datetime,
    sensors: tuple[str, ...] = ALL_SENSORS,
    base: Path | str = GRIDDED_DIR,
) -> Optional[dict[str, dict[str, np.ndarray]]]:
    """Read a previously written gridded slot.

    Returns ``None`` if the file is missing.  Sensors absent from the file
    (no data when gridded) are omitted from the result.
    """
    import netCDF4 as nc
    fpath = gridded_path(slot_utc, base)
    if not fpath.exists():
        return None

    out: dict[str, dict[str, np.ndarray]] = {}
    with nc.Dataset(str(fpath)) as ds:
        for sensor in sensors:
            name = f'AOD_{sensor}'
            if name not in ds.variables:
                continue
            aod = np.ma.filled(
                ds.variables[name][:].astype(np.float32), np.nan)
            if not np.any(np.isfinite(aod)):
                continue
            g: dict[str, np.ndarray] = {'aod_mean': aod}
            for sfx, key in (
                ('aod_std', 'aod_std'),
                ('cv',      'cv'),
                ('vza',     'vza_mean'),
                ('sza',     'sza_mean'),
            ):
                vname = f'{sfx}_{sensor}'
                if vname in ds.variables:
                    g[key] = np.ma.filled(
                        ds.variables[vname][:].astype(np.float32), np.nan)
            for sfx, key in (('n_valid', 'n_valid'), ('n_total', 'n_total')):
                vname = f'{sfx}_{sensor}'
                if vname in ds.variables:
                    arr = ds.variables[vname][:].astype(np.int16)
                    g[key] = np.ma.filled(arr, 0).astype(np.int16)
            out[sensor] = g
    return out if out else None


# ── Per-day driver ───────────────────────────────────────────────────────────

def grid_day(
    day: date,
    sensors: tuple[str, ...] = ALL_SENSORS,
    base: Path | str = GRIDDED_DIR,
    overwrite: bool = False,
    show_slot_bar: bool = False,
) -> tuple[int, int]:
    """Produce 30-min gridded NetCDFs for one calendar day.

    Returns ``(n_written, n_errors)``.  Slots whose target file already exists
    are skipped unless ``overwrite=True``.  Cells are never masked at this
    stage — quality metadata (cv, n_valid, n_total) is written to each NetCDF
    so downstream consumers can apply their own thresholds.
    """
    try:
        from tqdm import tqdm as _tqdm
        _HAS_TQDM = True
    except ImportError:
        _tqdm = None
        _HAS_TQDM = False

    written = 0
    errors  = 0

    modis_cache: Optional[dict] = None
    if 'modis_maiac' in sensors:
        modis_cache = read_modis_date(datetime(day.year, day.month, day.day))

    slots = list(_slot_iter(day))
    if show_slot_bar and _HAS_TQDM:
        slot_iter = _tqdm(slots, desc=f"  grid {day}", leave=False,
                          unit="slot", ncols=80)
    else:
        slot_iter = slots

    for slot_utc in slot_iter:
        out_path = gridded_path(slot_utc, base)
        if (not overwrite) and out_path.exists():
            continue
        try:
            per_sensor = compute_slot_grids(
                slot_utc, sensors=sensors, modis_pixels=modis_cache,
            )
            if not per_sensor:
                continue
            write_gridded_slot(slot_utc, per_sensor, base=base)
            written += 1
        except Exception as exc:
            errors += 1
            msg = f"    [grid ERROR] {slot_utc.isoformat()}: {exc}"
            if show_slot_bar and _HAS_TQDM:
                slot_iter.write(msg)   # type: ignore[union-attr]
            else:
                print(msg)

    return written, errors
