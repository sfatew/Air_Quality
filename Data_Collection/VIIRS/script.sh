#!/bin/bash
set -e

python download_viirs_l2.py --start "2020-01-01" --end "2022-01-01"
python crawl.py
