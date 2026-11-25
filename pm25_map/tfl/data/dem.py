"""Copernicus GLO-30 → per-cell mean and standard deviation of elevation (static)."""
import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import from_bounds

import cells as K
import config as C


def tile_path(lat_sw: int, lon_sw: int):
    ns, ew = ("N" if lat_sw >= 0 else "S"), ("E" if lon_sw >= 0 else "W")
    return C.DEM_DIR / f"Copernicus_DSM_COG_10_{ns}{abs(lat_sw):02d}_00_{ew}{abs(lon_sw):03d}_00_DEM.tif"


def extract(cells: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for cid, lat, lon in zip(cells.cell_id, cells.lat, cells.lon):
        s, w = lat - C.RES / 2, lon - C.RES / 2
        p = tile_path(int(np.floor(s + 1e-9)), int(np.floor(w + 1e-9)))     # 0.05° cells never straddle 1° tiles
        if not p.exists():                                                  # all-ocean tile on the bucket
            rows.append((cid, 0.0, 0.0))
            continue
        with rasterio.open(p) as src:
            a = src.read(1, window=from_bounds(w, s, w + C.RES, s + C.RES, src.transform), masked=True)
        a = a.compressed().astype(float)
        rows.append((cid, float(a.mean()) if a.size else 0.0, float(a.std()) if a.size else 0.0))
    return pd.DataFrame(rows, columns=["cell_id", *C.DEM_VARS])


if __name__ == "__main__":
    d = extract(K.cells())
    C.OUT.mkdir(parents=True, exist_ok=True)
    d.to_pickle(C.OUT / "dem_cells.pkl")
    print(d.describe().round(1).to_string())
