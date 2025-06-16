"""Sandbox script for use when developing new features"""

import os
import sys
import zipfile
from glob import glob
from typing import Set
from xml.etree import ElementTree as ET

import numpy as np
import polars as pl
import rasterio as rio
from tqdm.contrib.concurrent import process_map

from fell_loader import LidarLoader
from fell_loader.utils.partitioning import add_bng_partition_to_polars_df

self = LidarLoader()

lidar_dir = (
    "/run/media/ross/ff_data/extracts/bad_files/LIDAR-DTM-1m-2022-TM59ne.zip"
)

lidar, bbox = self.load_lidar_and_bbox_from_folder(lidar_dir)

lidar_df = self.generate_df_from_lidar_array(lidar, bbox)

lidar_df = add_bng_partition_to_polars_df(lidar_df)
lidar_df = self.set_output_schema(lidar_df)

# bbox: 655000, 295000, 656000, 300000
#       1000 wide, 5000 high
# lidar.shape: 5000, 1000
#       5000 rows, 1000 elements per row

# Get array dimensions
size_e, size_s = lidar.shape  # 5000, 1000

# Collapse elevations to 1 dimension, left to right then top to bottom
elevations = lidar.flatten(order="C")

# elevations.shape = 5000000,
