import os
import sys
import rasterio
import numpy as np
import xarray as xr
import geopandas as gpd
import subprocess
from rasterio.mask import mask
from rasterio.transform import from_origin

# Define a standard "No Data" value (Transparent)
NODATA_VAL = -9999.0

def nc_to_geotiff(nc_file, output_path):
    print(f"Reading: {nc_file}")
    ds = xr.open_dataset(nc_file, decode_timedelta=True)
    
    # 1. Define variables to extract
    # Use a dictionary to map Variable Name -> Data Array
    vars_to_extract = [v for v in ds.data_vars if v != 'Hour' or v!= 'longitude' or v!= 'latitude']
    data_map = {}

    lon = ds['longitude'].values
    lat = ds['latitude'].values
    
    # 2. CHECK FOR FLIP (The fix for the "White Shape" issue)
    # If latitude is increasing (e.g., -90 to 90), it is South-to-North.
    # GeoTIFF requires North-to-South (Top-Left origin).
    needs_flip = lat[0] < lat[-1]
    
    if needs_flip:
        print(" > Data is South-to-North. Flipping arrays...")

    # 3. Process each variable
    for v in vars_to_extract:
        if v in ds:
            val = ds[v].values
            
            # Remove time dimension if present (1, H, W) -> (H, W)
            if val.ndim == 3:
                val = val.squeeze()
            
            # FLIP DATA IF NEEDED
            if needs_flip:
                val = np.flipud(val)
                
            data_map[v] = val
        else:
            print(f"Warning: {v} not found in NetCDF. Filling with NoData.")
            # Create an empty array of the correct shape
            # L2 uses 'AOT' as primary field (L3 used 'AOT_Merged')
            ref_shape = data_map.get('AOT_Merged', np.zeros((len(lat), len(lon)))).shape
            data_map[v] = np.full(ref_shape, np.nan)

    ds.close()

    # 4. Align Grid
    pixel_size = 0.05
    lon_start = np.floor(lon.min() / pixel_size) * pixel_size
    # GeoTIFF origin is always the Top-Left (North-West), so we use lat.max()
    lat_start = np.ceil(lat.max() / pixel_size) * pixel_size

    transform = from_origin(
        lon_start, lat_start, pixel_size, pixel_size
    )

    # 5. Create Profile with NODATA
    # L2 primary field is 'AOT' (L3 used 'AOT_Merged')
    height, width = data_map['AOT'].shape
    
    profile = {
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'count': len(vars_to_extract),
        'dtype': 'float32',
        'crs': 'EPSG:4326',
        'transform': transform,
        'nodata': NODATA_VAL  # CRITICAL: Tells GIS this value is transparent
    }

    # 6. Write to File
    with rasterio.open(output_path, 'w', **profile) as dst:
        for i, name in enumerate(vars_to_extract):
            data = data_map[name]
            
            # Convert NaNs in the data to -9999
            data_filled = np.nan_to_num(data, nan=NODATA_VAL)
            
            dst.write(data_filled.astype('float32'), i + 1)
            dst.set_band_description(i + 1, name)

def crop_to_vietnam(input_tif, output_tif, vietnam_shapefile):
    with rasterio.open(input_tif) as src:
        # Get descriptions from the source before we do anything
        band_descriptions = src.descriptions 
        
        shape = gpd.read_file(vietnam_shapefile)
        shape = shape.to_crs(src.crs)
        
        try:
            out_image, out_transform = mask(src, shape.geometry, crop=True, nodata=NODATA_VAL)
        except ValueError:
            print("ERROR: Shape does not overlap raster. Check coordinates!")
            return

        out_meta = src.meta.copy()
        out_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
            "nodata": NODATA_VAL
        })

        with rasterio.open(output_tif, "w", **out_meta) as dest:
            dest.write(out_image)
            # FIX: Loop through the saved descriptions and apply them to the new file
            for i, desc in enumerate(band_descriptions):
                if desc: # Only set if description isn't None
                    dest.set_band_description(i + 1, desc)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Thiếu file .nc đầu vào")
        sys.exit(1)

    nc_path = sys.argv[1]

    base_dir = os.path.dirname(nc_path)
    filename = os.path.basename(nc_path).replace(".nc", "")
    aod_full_path = os.path.join(base_dir, f"aod_full_{filename}.tif")
    aod_vietnam_path = os.path.join(base_dir, f"aod_vietnam_{filename}.tif")

    shapefile_path = "/opt/airflow/dags/GADM_Vietnam/gadm41_VNM_0.shp"

    # 1. Convert
    nc_to_geotiff(nc_path, aod_full_path)
    
    # 2. Crop
    if os.path.exists(aod_full_path):
        crop_to_vietnam(aod_full_path, aod_vietnam_path, shapefile_path)

        # CLEANUP
        # IMPORTANT: I suggest commenting out the 'os.remove' lines 
        # until you verify the fix works, so you don't lose data.
        os.remove(nc_path) 
        os.remove(aod_full_path)

    # # 3. Run Extraction
    # EXTRACT_SCRIPT = "/home/work1/projects/Air_Quality/AOD data/extract_station_aod.py"
    # if os.path.exists(aod_vietnam_path):
    #      subprocess.run(["python", EXTRACT_SCRIPT, aod_vietnam_path])
    # else:
    #     print("Error: Final Vietnam TIF was not created.")