#!/bin/bash
set -e

python fetch_era5_bbox.py
python extract_stations.py
