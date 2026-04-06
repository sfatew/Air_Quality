#!/usr/bin/env python3
import os
import sys
import numpy as np
import xarray as xr
import geopandas as gpd

# optional helpers
try:
    import rioxarray  # registers the .rio accessor
except Exception:
    rioxarray = None

try:
    import regionmask
except Exception:
    regionmask = None

def ensure_lon_range(ds, lon_name="lon"):
    """Convert lon 0..360 -> -180..180 if needed, returning a new dataset."""
    lon = ds.coords.get(lon_name)
    if lon is None:
        return ds
    if float(lon.max()) > 180.0:
        # wrap longitudes
        ds = ds.assign_coords({lon_name: (((ds[lon_name] + 180) % 360) - 180)}).sortby(lon_name)
    return ds

def detect_coord_names(ds):
    """Return names for lon/lat if present (1D or 2D)."""
    coords = list(ds.coords)
    lon = None; lat = None
    for candidate in ["lon", "longitude", "x", "LON", "LONGITUDE", "Longitudes"]:
        if candidate in coords:
            lon = candidate; break
    for candidate in ["lat", "latitude", "y", "LAT", "LATITUDE", "Latitudes"]:
        if candidate in coords:
            lat = candidate; break
    return lon, lat

def crop_nc_to_nc(input_nc, shapefile, output_nc, nodata=NODATA_VAL, keep_vars=None):
    """
    Crop input_nc to geometry inside shapefile and save as output_nc.
    keep_vars: list of variable names to keep; default None => keep all data_vars.
    """
    print(f"Opening: {input_nc}")
    ds = xr.open_dataset(input_nc, decode_times=True, mask_and_scale=False)  # avoid auto-masking until we handle nodata
    lon_name, lat_name = detect_coord_names(ds)
    gdf = gpd.read_file(shapefile)
    gdf = gdf.to_crs("EPSG:4326")  # we'll work in lat/lon

    if keep_vars is None:
        vars_to_process = list(ds.data_vars)
    else:
        vars_to_process = [v for v in keep_vars if v in ds]

    # Try the rioxarray path if we have 1D lon/lat or coords already equal-area
    # rioxarray wants coordinates called 'x' & 'y' OR a proper 2D geotransform/crs.
    # Approach:
    # 1) If 1D lon/lat exist, rename to x/y, write CRS EPSG:4326, then rio.clip
    # 2) If lon/lat are 2D (meshgrid), use regionmask to build boolean mask and apply

    # 1D coords check
    use_rio = False
    if lon_name and lat_name:
        lon_dim = ds.coords[lon_name].dims
        lat_dim = ds.coords[lat_name].dims
        if (len(lon_dim) == 1 and len(lat_dim) == 1) or (ds[lon_name].ndim == 1 and ds[lat_name].ndim == 1):
            use_rio = True

    if use_rio and rioxarray is not None:
        print("Using rioxarray clip path (1D lon/lat detected).")
        # convert lon range if needed
        ds = ensure_lon_range(ds, lon_name)
        # pick a copy to avoid mutating original names
        ds_clip = ds.copy()
        # rename lon/lat -> x/y for rio
        ds_clip = ds_clip.rename({lon_name: "x", lat_name: "y"})
        # ensure y is descending (rioxarray expects top-left origin for many operations)
        # rioxarray can handle both but some operations expect y decreasing; we won't force flip here
        # set CRS
        ds_clip.rio.write_crs("EPSG:4326", inplace=True, update_coords=True)

        # optional bbox pre-subset to speed up reproject/clip:
        minx, miny, maxx, maxy = gdf.total_bounds
        # note: sel slices depend on coord order; use min/max in sorted order
        ds_clip = ds_clip.sel(x=slice(minx-1.0, maxx+1.0), y=slice(maxy+1.0, miny-1.0))

        clipped_vars = {}
        for var in vars_to_process:
            da = ds_clip[var]
            # If variable has time dimension or others, keep them
            # Use rio.clip which accepts list of geometries and a CRS
            try:
                clipped = da.rio.clip(gdf.geometry, gdf.crs, drop=False, invert=False)
            except Exception as e:
                print(f"  Warning: rioxarray.clip failed for variable {var}: {e}")
                # fallback to simple bbox selection
                clipped = da
            # replace nodata (NaN) with fill value later in encoding
            clipped_vars[var] = clipped

        # build new dataset from clipped variables
        ds_out = xr.Dataset(clipped_vars, coords={k: v for k, v in ds_clip.coords.items() if k in ["x","y","time"] or True})
        # rename coords back to original lon/lat names (optional)
        ds_out = ds_out.rename({"x": lon_name, "y": lat_name})

    else:
        # Use regionmask or polygon-in-mask approach using 2D lon/lat if available
        print("Using regionmask / mask-by-lonlat path (2D lon/lat or no rioxarray).")
        if regionmask is None:
            raise RuntimeError("regionmask is required for 2D lat/lon masking. Install with `pip install regionmask`")

        # Identify lon/lat arrays: try common names 'longitude','latitude'
        if lon_name is None or lat_name is None:
            # try to find variables named latitude/longitude in dataset variables (2D)
            for cand_lon in ["longitude", "lon", "LON"]:
                if cand_lon in ds.variables and ds[cand_lon].ndim >= 2:
                    lon_name = cand_lon; break
            for cand_lat in ["latitude", "lat", "LAT"]:
                if cand_lat in ds.variables and ds[cand_lat].ndim >= 2:
                    lat_name = cand_lat; break

        if lon_name is None or lat_name is None:
            raise RuntimeError("Could not find lon/lat coordinates in dataset. Please inspect your NetCDF.")

        lon = ds[lon_name]
        lat = ds[lat_name]
        # ensure lon in -180..180 for masking geometry
        ds = ensure_lon_range(ds, lon_name)

        # regionmask.mask_geopandas wants lon, lat arrays (1D or 2D)
        mask = regionmask.mask_geopandas(gdf, lon=ds[lon_name], lat=ds[lat_name], inside=False)
        # mask is an xr.DataArray with integer labels; mask==0 or mask==<region index>
        # We want boolean inside/outside; mask regions are 0..n-1 for polygons; outside = NaN
        # Convert to boolean mask: True where inside any polygon
        bool_mask = ~np.isnan(mask)

        clipped_vars = {}
        for var in vars_to_process:
            da = ds[var]
            # if da has (time,y,x) dims, align mask dims (y,x)
            # regionmask returns dims same as lon/lat dims (e.g., y,x)
            # We broadcast mask across time if needed
            # da.where keeps values where condition True; outside -> NaN
            try:
                da_masked = da.where(bool_mask)
            except Exception:
                # attempt to align dims by selecting the last two dims
                da_masked = da.where(bool_mask)
            clipped_vars[var] = da_masked

        ds_out = xr.Dataset(clipped_vars, coords=ds.coords)

    # Replace remaining NaNs with nodata value (but keep attributes)
    # We'll prepare encoding so NetCDF has _FillValue set
    encodings = {}
    for var in ds_out.data_vars:
        # convert to float32 if numeric
        arr = ds_out[var]
        if np.issubdtype(arr.dtype, np.floating):
            ds_out[var] = arr.astype("float32")
        else:
            # keep as-is (e.g., integer or other); but many satellite data are float
            pass

        encodings[var] = {
            "_FillValue": np.float32(nodata),
            "dtype": "float32"
        }

        # replace NaN with nodata in the array (so we keep consistent values inside file variable)
        ds_out[var] = ds_out[var].where(~np.isnan(ds_out[var]), other=np.float32(nodata))

    # Preserve global attributes where possible
    out_attrs = ds.attrs.copy()
    ds_out.attrs.update(out_attrs)

    print(f"Writing cropped NetCDF -> {output_nc}")
    # Write out. Use NETCDF4 classic or NETCDF4
    ds_out.to_netcdf(output_nc, mode="w", format="NETCDF4", encoding=encodings)
    ds.close()
    print("Done.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python crop_nc_to_vietnam.py input.nc [vietnam_shp] [output.nc]")
        sys.exit(1)

    input_nc = sys.argv[1]
    shapefile = sys.argv[2] if len(sys.argv) >= 3 else "/home/work1/projects/Air_Quality/GADM_Vietnam/gadm41_VNM_0.shp"
    out_nc = sys.argv[3] if len(sys.argv) >= 4 else os.path.join(os.path.dirname(input_nc), "aod_vietnam_" + os.path.basename(input_nc))

    crop_nc_to_nc(input_nc, shapefile, out_nc)
