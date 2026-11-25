"""
Draw a 7-region map of Vietnam (provinces dissolved into the regions defined
in add_region.py) and overlay envisoft (tedp.vn) station locations.

Regions:
    NW  - North West
    NE  - North East
    RRD - Red River Delta
    NCC - North Central Coast
    SCC - South Central Coast
    CH  - Central Highlands
    SE  - South East (+ Mekong Delta, folded into SE)

Inputs:
    GADM_Vietnam/gadm41_VNM_1.shp        (province polygons)
    Masterdata/envisoft_station_map.csv  (stations with `region` column)

Output:
    Masterdata/regions_map.png

Usage:
    python plot_regions_map.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

# Reuse the canonical province -> region mapping and the ASCII normaliser
# defined alongside this script so the map and station classification stay
# in sync. (add_region.py lives in the same folder.)
sys.path.insert(0, str(Path(__file__).resolve().parent))
from add_region import PROVINCE_TO_REGION, _ascii_lower  # noqa: E402

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
MASTERDATA = Path(__file__).resolve().parents[1]
PROJECT_ROOT = MASTERDATA.parent
SHP_PATH = PROJECT_ROOT / "GADM_Vietnam" / "gadm41_VNM_1.shp"
CSV_PATH = MASTERDATA / "envisoft_station_map.csv"
OUT_PATH = MASTERDATA / "regions_map.png"

# ---------------------------------------------------------------------------
# Region metadata (display order, full name, fill colour)
# ---------------------------------------------------------------------------
REGION_ORDER = ["NW", "NE", "RRD", "NCC", "SCC", "CH", "SE"]

REGION_FULL = {
    "NW":  "North West",
    "NE":  "North East",
    "RRD": "Red River Delta",
    "NCC": "North Central Coast",
    "SCC": "South Central Coast",
    "CH":  "Central Highlands",
    "SE":  "South East (+ Mekong Delta)",
}

# Vietnamese-claimed archipelagos in the East Sea.
# Not part of the GADM polygons, so drawn as a small yellow star
# (a nod to the flag) at the approximate centre of each archipelago.
ARCHIPELAGOS = [
    # name,                       lon,   lat
    ("QĐ. Hoàng Sa\n(Việt Nam)",  112.0, 16.4),
    ("QĐ. Trường Sa\n(Việt Nam)", 114.5,  9.8),
]

# Colour-blind-friendly palette (ColorBrewer Set3 / Pastel1 mix).
# Picked so neighbouring regions don't share hues.
REGION_COLOR = {
    "NW":  "#8dd3c7",  # teal       (north-west mountains)
    "NE":  "#80b1d3",  # blue       (north-east hills)
    "RRD": "#fb8072",  # coral      (Hà Nội delta)
    "NCC": "#fdb462",  # orange     (Thanh Hoá–Huế coast)
    "SCC": "#ffed6f",  # yellow     (Đà Nẵng–Bình Thuận coast)
    "CH":  "#b3de69",  # lime       (Tây Nguyên plateau)
    "SE":  "#bc80bd",  # purple     (Đông Nam Bộ + Mekong)
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_regions() -> gpd.GeoDataFrame:
    """Read GADM provinces, attach region codes, and dissolve into 7 polygons."""
    gdf = gpd.read_file(SHP_PATH)

    def _key(name: str) -> str:
        return re.sub(r"\s+", " ", _ascii_lower(str(name)))

    # GADM's VARNAME_1 is plain-ASCII (e.g. "Ba Ria - Vung Tau"); the
    # mapping keys are lowercase ASCII, so just normalise and look up.
    gdf["region"] = gdf["VARNAME_1"].map(_key).map(PROVINCE_TO_REGION)

    missing = gdf[gdf["region"].isna()]
    if not missing.empty:
        print("Warning — provinces not mapped to any region:")
        print(missing[["NAME_1", "VARNAME_1"]].to_string(index=False))

    dissolved = (
        gdf.dropna(subset=["region"])
           .dissolve(by="region", as_index=False)[["region", "geometry"]]
    )
    return dissolved


def load_stations() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH)
    df = df[df["region"].isin(REGION_ORDER)].copy()
    return df


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------
def plot_map(regions: gpd.GeoDataFrame, stations: pd.DataFrame, out: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 12), dpi=150)

    # --- region fills ------------------------------------------------------
    for code in REGION_ORDER:
        sub = regions[regions["region"] == code]
        if sub.empty:
            continue
        sub.plot(
            ax=ax,
            color=REGION_COLOR[code],
            edgecolor="#2b2b2b",
            linewidth=0.6,
            alpha=0.85,
        )

    # --- station scatter ---------------------------------------------------
    # Black markers with white edge so they stay legible against any fill.
    # (See the design-choice note in the chat — you may want region-coloured
    # markers instead; that is a 2-line swap in the loop below.)
    ax.scatter(
        stations["longitude"],
        stations["latitude"],
        s=22,
        c="black",
        marker="o",
        edgecolor="white",
        linewidth=0.6,
        zorder=3,
    )

    # --- archipelagos (Hoàng Sa, Trường Sa) --------------------------------
    for name, lon, lat in ARCHIPELAGOS:
        ax.scatter(
            lon, lat,
            s=90, marker="*",
            facecolor="#FFCD00", edgecolor="#a30000", linewidth=0.6,
            zorder=4,
        )
        ax.text(
            lon + 0.25, lat + 0.25,
            name,
            ha="left", va="bottom",
            fontsize=7, color="#1f4e79", fontweight="bold",
            zorder=4,
        )

    # Extend x-axis so Trường Sa is not clipped (default extent stops
    # at the mainland's eastern edge near 109.5°E).
    ax.set_xlim(101.5, 117.0)

    # --- legend ------------------------------------------------------------
    counts = stations["region"].value_counts().to_dict()
    region_handles = [
        Patch(
            facecolor=REGION_COLOR[code],
            edgecolor="#2b2b2b",
            label=f"{code} — {REGION_FULL[code]}  ({counts.get(code, 0)})",
        )
        for code in REGION_ORDER
    ]
    station_handle = Line2D(
        [0], [0],
        marker="o", color="w",
        markerfacecolor="black", markeredgecolor="white", markersize=7,
        label=f"envisoft station (n={len(stations)})",
    )
    ax.legend(
        handles=region_handles + [station_handle],
        loc="upper right",
        fontsize=8,
        framealpha=0.95,
        title="Region (station count)",
        title_fontsize=9,
    )

    # --- cosmetics ---------------------------------------------------------
    ax.set_xlabel("Longitude (°E)")
    ax.set_ylabel("Latitude (°N)")
    ax.set_title("Vietnam — 7 regions with envisoft (tedp.vn) stations")
    ax.set_aspect("equal")
    ax.grid(True, linewidth=0.3, alpha=0.4)

    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out}")


# ---------------------------------------------------------------------------
def main() -> None:
    regions = load_regions()
    stations = load_stations()
    print(f"Loaded {len(regions)} dissolved regions, {len(stations)} stations")
    print("Stations per region:")
    print(stations["region"].value_counts().reindex(REGION_ORDER).to_string())
    plot_map(regions, stations, OUT_PATH)


if __name__ == "__main__":
    main()
