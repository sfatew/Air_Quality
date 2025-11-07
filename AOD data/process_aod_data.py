import os
import sys
import rasterio
import numpy as np
import xarray as xr
import geopandas as gpd
import subprocess
from rasterio.mask import mask

def nc_to_geotiff(nc_file, output_path):
    ds = xr.open_dataset(nc_file, decode_timedelta=True)
    aot = ds['AOT'].values
    aot_uncertainty = ds['AOT_uncertainty'].values
    lon = ds['longitude'].values
    lat = ds['latitude'].values
    ds.close()

    # Align lưới theo 0.05 độ
    pixel_size = 0.05
    lon_start = np.floor(lon.min() / pixel_size) * pixel_size
    lat_start = np.ceil(lat.max() / pixel_size) * pixel_size

    transform = rasterio.transform.from_origin(
        lon_start, lat_start, pixel_size, pixel_size
    )

    # Tạo profile cho file GeoTIFF
    profile = {
        'driver': 'GTiff',
        'height': aot.shape[0],
        'width': aot.shape[1],
        'count': 2,  # Thay đổi count thành 2 để lưu cả AOT và uncertainty
        'dtype': 'float32',
        'crs': 'EPSG:4326',
        'transform': transform,
    }

    # Ghi cả AOT và uncertainty vào file GeoTIFF
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(aot.astype('float32'), 1)  # Band 1: AOT
        dst.write(aot_uncertainty.astype('float32'), 2)  # Band 2: AOT_uncertainty

def crop_to_vietnam(input_tif, output_tif, vietnam_shapefile):
    with rasterio.open(input_tif) as src:
        shape = gpd.read_file(vietnam_shapefile)
        shape = shape.to_crs(src.crs)
        out_image, out_transform = mask(src, shape.geometry, crop=True)
        out_meta = src.meta.copy()
        out_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })

    with rasterio.open(output_tif, "w", **out_meta) as dest:
        dest.write(out_image)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Thiếu file .nc đầu vào")
        sys.exit(1)

    nc_path = sys.argv[1]
    

    base_dir = os.path.dirname(nc_path)
    filename = os.path.basename(nc_path).replace(".nc", "")
    aod_full_path = os.path.join(base_dir, f"aod_full_{filename}.tif")
    aod_vietnam_path = os.path.join(base_dir, f"aod_vietnam_{filename}.tif")

    # print("Path: ", aod_full_path)

    shapefile_path = "/home/work1/projects/Air_Quality/GADM_Vietnam/gadm41_VNM_0.shp"

    nc_to_geotiff(nc_path, aod_full_path)
    crop_to_vietnam(aod_full_path, aod_vietnam_path, shapefile_path)

    os.remove(nc_path)
    os.remove(aod_full_path)

    
    EXTRACT_SCRIPT = "/home/work1/projects/Air_Quality/AOD data/extract_station_aod.py"
    subprocess.run(["python", EXTRACT_SCRIPT, aod_vietnam_path])