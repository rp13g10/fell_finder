"""Functions which help to determine which files are available to load, and
which ones have already been loaded."""

import os
from glob import glob
from typing import Set


def get_available_folders(data_dir: str) -> Set[str]:
    """Get a list of all of the data folders which are available within the
    data directory of this package. Data must have been downloaded from the
    DEFRA website:
    https://environment.data.gov.uk/DefraDataDownload/?Mode=survey
    Data should be from the composite DTM layer at 1m resolution, and folder
    names should match the 'lidar_composite_dtm-*' pattern. Any zip archives
    should be extracted before running this script.

    Raises:
        FileNotFoundError: If no folders matching the above pattern are found,
          an error will be raised.

    Returns:
        Set[str]: A set containing the absolute path to each data folder
    """
    all_lidar_dirs = glob(
        os.path.join(data_dir, "extracts/lidar/lidar_composite_dtm-*")
    )
    if not all_lidar_dirs:
        raise FileNotFoundError("No files found in data directory!")
    return set(all_lidar_dirs)
