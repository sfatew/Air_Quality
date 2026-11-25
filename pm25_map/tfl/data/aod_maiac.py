"""MODIS MAIAC MCD19A2 (1 km, per orbit) → mean AOD of each station cell at each UTC hour.

As in Wang et al. 2024: Optical_Depth_055 with QA CloudMask = Clear and AdjacencyMask = Clear, orbits
placed in hours via the file's `Orbit_time_stamp`. An orbit is assigned to HH:00 UTC when it lies within
±AOD_SLOT_MIN of it; several orbits/pixels in one cell-hour are averaged pixel-weighted.
Hours without a clear retrieval stay missing — Li et al. 2020 drop such samples, Wang et al. 2024 gap-fill
them; the frame keeps them as NaN with `aod_npix = 0` so either choice can be made at model time.

Pixel geolocation uses the closed-form inverse of the MODIS sinusoidal grid (exact for the tile layout).
"""
import re
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pyhdf.SD import SD, SDC

import cells as K
import config as C

R_EARTH = 6371007.181
TILE_M = 1111950.5197665
X0, Y0 = -20015109.354, 10007554.677
NPIX = 1200
PIX_M = TILE_M / NPIX
FILL_AOD = -28672
_TILE_RE = re.compile(r"\.h(\d{2})v(\d{2})\.")


def tile_pixels_to_cells(h: int, v: int, wanted: set) -> tuple[np.ndarray, np.ndarray]:
    """Flat pixel indices of tile (h, v) that fall in `wanted` cells, and their cell ids."""
    i, j = np.indices((NPIX, NPIX))
    x = X0 + h * TILE_M + (j + 0.5) * PIX_M
    y = Y0 - v * TILE_M - (i + 0.5) * PIX_M
    lat = np.degrees(y / R_EARTH)
    with np.errstate(divide="ignore", invalid="ignore"):
        lon = np.degrees(x / (R_EARTH * np.cos(np.radians(lat))))
    inside = (lat >= C.LAT_MIN) & (lat < C.LAT_MAX) & (lon >= C.LON_MIN) & (lon < C.LON_MAX)
    _, _, cid = K.cell_of(np.where(inside, lat, C.LAT_MAX - 1e-6), np.where(inside, lon, C.LON_MIN))
    keep = inside & np.isin(cid, list(wanted))
    return np.flatnonzero(keep.ravel()), cid.ravel()[keep.ravel()]


def orbit_times(sd: SD) -> list[datetime]:
    out = []
    for tok in str(sd.attributes().get("Orbit_time_stamp", "")).split():
        y, d, hh, mm = int(tok[:4]), int(tok[4:7]), int(tok[7:9]), int(tok[9:11])
        out.append(datetime(y, 1, 1) + timedelta(days=d - 1, hours=hh, minutes=mm))
    return out


def one_day(args) -> pd.DataFrame:
    day, tile_maps = args
    folder = C.MAIAC_DIR / f"{day:%Y}" / f"{day:%j}"
    parts = []
    for f in sorted(folder.glob("MCD19A2.*.hdf")) if folder.exists() else []:
        m = _TILE_RE.search(f.name)
        key = (int(m[1]), int(m[2]))
        if key not in tile_maps:
            continue
        pix, cid = tile_maps[key]
        try:
            sd = SD(str(f), SDC.READ)
            aod = sd.select("Optical_Depth_055").get().reshape(-1, NPIX * NPIX)[:, pix]
            qa = sd.select("AOD_QA").get().reshape(-1, NPIX * NPIX)[:, pix].astype(np.int64)
            times = orbit_times(sd)
            sd.end()
        except Exception as e:                                   # corrupt file: skip, report
            print(f"[maiac] {f.name}: {e}", flush=True)
            continue
        for o in range(min(len(times), aod.shape[0])):
            slot = (times[o] + timedelta(minutes=30)).replace(minute=0, second=0)
            if abs((times[o] - slot).total_seconds()) > C.AOD_SLOT_MIN * 60 or slot.hour not in C.UTC_HOURS:
                continue
            a = aod[o].astype(float) * 0.001
            q = qa[o]
            ok = ((aod[o] != FILL_AOD) & (q != 0) & ((q & 0b111) == C.MAIAC_CLOUD_CLEAR)
                  & (((q >> 5) & 0b111) == C.MAIAC_ADJ_CLEAR) & (a >= C.AOD_VALID[0]) & (a <= C.AOD_VALID[1]))
            if ok.any():
                parts.append(pd.DataFrame({"cell_id": cid[ok], "time_utc": slot, "aod": a[ok]}))
    if not parts:
        return pd.DataFrame(columns=["cell_id", "time_utc", "aod_sum", "aod_npix"])
    d = pd.concat(parts, ignore_index=True)
    return d.groupby(["cell_id", "time_utc"]).aod.agg(aod_sum="sum", aod_npix="size").reset_index()


def extract(cells: pd.DataFrame, workers: int = 6) -> pd.DataFrame:
    wanted = set(cells.cell_id)
    tile_maps = {}
    for h, v in [(h, v) for h in range(23, 30) for v in range(3, 8)]:     # every tile that touches China
        pix, cid = tile_pixels_to_cells(h, v, wanted)
        if len(pix):
            tile_maps[(h, v)] = (pix, cid)
    print(f"[maiac] {len(tile_maps)} tiles cover {len(wanted)} station cells: {sorted(tile_maps)}", flush=True)
    days = pd.date_range(C.START, C.END, freq="D")
    with ProcessPoolExecutor(workers) as ex:
        res = list(ex.map(one_day, [(d.to_pydatetime(), tile_maps) for d in days], chunksize=4))
    d = pd.concat([r for r in res if len(r)], ignore_index=True)
    d = d.groupby(["cell_id", "time_utc"], as_index=False)[["aod_sum", "aod_npix"]].sum()
    d["aod"] = (d.aod_sum / d.aod_npix).astype(np.float32)
    return d[["cell_id", "time_utc", "aod", "aod_npix"]]


if __name__ == "__main__":
    a = extract(K.cells())
    C.OUT.mkdir(parents=True, exist_ok=True)
    a.to_pickle(C.OUT / "aod_maiac_cells.pkl")
    print(f"[maiac] {len(a):,} cell-hours with clear AOD · {a.cell_id.nunique()} cells · "
          f"by UTC hour {a.time_utc.dt.hour.value_counts().sort_index().to_dict()}")
