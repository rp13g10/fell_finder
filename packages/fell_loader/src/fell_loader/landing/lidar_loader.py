"""Functions relating to the extraction & initial transformation of the LIDAR
extracts into a tabular format.
"""

import logging
import os
import re
import sys
import zipfile
from glob import glob
from xml.etree import ElementTree as ET

import diskcache
import numpy as np
import polars as pl
import rasterio as rio
from tqdm.contrib.concurrent import process_map

from fell_loader.utils.partitioning import add_bng_partition_to_polars_df

# Set the max number of LIDAR files which will be processed in parallel
# Allow up to 3gb of RAM per worker (should be closer to 2)
MAX_WORKERS = 8

logger = logging.getLogger(__name__)


class LidarLoader:
    """This contains all of the functions required to convert the data held in
    a LIDAR folder into tabular format. In most use cases, only the `load`
    method will need to be called.
    """

    def __init__(self) -> None:
        """Initialize the loader class. An active pyspark SQL context is
        required, as is the path to the folder which contains the LIDAR
        extracts to be parsed.

        Args:
            data_dir: The data directory for this project. It is expected
              that this will contain an 'extracts/lidar' subdirectory, into
              which all LIDAR data will have been extracted.

        """
        self.data_dir = os.environ["FF_DATA_DIR"]

        self.to_load = self.get_folders_to_load()

        self.dc = diskcache.Index(
            os.path.join(self.data_dir, "temp/lidar_bounds")
        )

    def _get_file_id_from_path(self, path: str) -> str:
        file_id = re.search(r".*([A-Z][A-Z]\d\d[ns][ew]).*", path)

        if not file_id:
            raise ValueError(f"Unable to extract file ID from path: {file_id}")

        return file_id.group(1)

    def get_available_folders(self) -> dict[str, str]:
        """Get a dict of all of the data folders which are available within the
        data directory of this package. Data must have been downloaded from the
        DEFRA website:
        https://environment.data.gov.uk/DefraDataDownload/?Mode=survey
        Data should be from the composite DTM layer at 1m resolution.

        Raises:
            FileNotFoundError: If no folders are found, an error will be raised

        Returns:
            A dict mapping the ID for each folder to its path on the
            filesystem. For example:

                {
                    'SU00ne': (
                        '/path/to/extracts/lidar/LIDAR-DTM-1m-2022-SU00ne.zip'
                    )
                }

        """
        pattern = os.path.join(self.data_dir, "extracts/lidar/*.zip")

        logger.debug(f"Searching for LIDAR data using pattern {pattern}")

        all_lidar_dirs = glob(pattern)
        if not all_lidar_dirs:
            raise FileNotFoundError(
                f"No files matching pattern {pattern} detected!"
            )

        logger.info(f"Found {len(all_lidar_dirs)} LIDAR source files")

        return {
            self._get_file_id_from_path(dir_): dir_ for dir_ in all_lidar_dirs
        }

    def get_loaded_folders(self) -> dict[str, str]:
        """Get a dict of all of the data folders which have already been
        loaded into the landing data layer.


        Returns:
            A dict mapping the ID for each folder to its path on the
            filesystem. For example:

                {'SU00ne': '/path/to/landing/lidar/SU00ne'}

        """
        all_loaded_dirs = glob(os.path.join(self.data_dir, "landing/lidar/*"))
        if not all_loaded_dirs:
            logger.debug("No existing LIDAR data detected")
            return {}

        logger.debug(f"Found {len(all_loaded_dirs)} parsed LIDAR files")

        return {
            self._get_file_id_from_path(dir_): dir_ for dir_ in all_loaded_dirs
        }

    def initialize_output_folder(self) -> None:
        """If the 'lidar' folder in the landing layer of the data directory
        doesn't exist yet, create it.
        """
        output_dir = os.path.join(self.data_dir, "landing", "lidar")

        if not os.path.exists(output_dir):
            logger.info(f"Initializing output folder '{output_dir}'")
            os.makedirs(output_dir)

    def get_folders_to_load(self) -> dict[str, str]:
        """Get a dict of all of the data folders which are available within the
        data directory of this package, and have not yet been loaded into
        the landing layer. Data must have been downloaded from the
        DEFRA website:
        https://environment.data.gov.uk/DefraDataDownload/?Mode=survey
        Data should be from the composite DTM layer at 1m resolution.

        Raises:
            FileNotFoundError: If no folders are found, an error will be raised

        Returns:
            A dict mapping the ID for each folder to its path on the
            filesystem. For example:

                {
                    'SU00ne': (
                        '/path/to/extracts/lidar/LIDAR-DTM-1m-2022-SU00ne.zip'
                    )
                }

        """
        available = self.get_available_folders()

        # Initialize after successful location of output folders, prevent
        # creation of folders in the wrong place if FF_DATA_DIR is not set
        # correctly
        self.initialize_output_folder()
        loaded = self.get_loaded_folders()

        to_load = {k: v for k, v in available.items() if k not in loaded}

        logger.info(f"{len(to_load)} LIDAR files to load")
        return to_load

    @staticmethod
    def _get_filenames_from_archive(
        archive: zipfile.ZipFile,
    ) -> tuple[str, str]:
        """Fetch the names of required .tif and .xml files from the provided
        archive. Improved error handling to be added in a later build.
        """
        tif_loc = next(
            x.filename for x in archive.filelist if ".tif" in x.filename
        )
        xml_loc = next(
            x.filename for x in archive.filelist if ".xml" in x.filename
        )
        return tif_loc, xml_loc

    @staticmethod
    def _get_bbox_from_xml(xml: str) -> np.ndarray:
        """Using the .xml data stored in each LIDAR folder, extract the
        coordinates which define the bounding box for the array.

        Args:
            xml: The contents of the XML file in the LIDAR folder

        Raises:
            AssertionError: If a malformed XML file is encountered, an
                exception will be raised

        Returns:
            An array which defines the bounding box for the LIDAR data, takes
            the form:

                [min_easting, max_easting, min_northing, max_northing]

        """
        # Parse the XML and locate all of the 'pos' tags within it
        tree = ET.fromstring(xml)
        corners = tree.findall("./spatRepInfo/Georect/cornerPts/pos")

        # Determine the min/max coordinates, there's probably a more elegant
        # way to do this
        min_easting = sys.maxsize
        max_easting = -sys.maxsize
        min_northing = sys.maxsize
        max_northing = -sys.maxsize
        for corner in corners:
            corner_text = corner.text
            if corner_text is None:
                raise AssertionError("oops!")
            easting, northing = corner_text.split(" ")
            easting = int(float(easting))
            northing = int(float(northing))

            min_easting = min(min_easting, easting)
            max_easting = max(max_easting, easting)
            min_northing = min(min_northing, northing)
            max_northing = max(max_northing, northing)

        assert all(
            (abs(x) != sys.maxsize)
            for x in [min_easting, max_easting, min_northing, max_northing]
        ), "Error while extracting bbox"
        assert min_easting < max_easting, "Error while extracting bbox"
        assert min_northing < max_northing, "Error while extracting bbox"

        bbox = np.array(
            [min_easting, min_northing, max_easting, max_northing], dtype=int
        )

        return bbox

    def load_lidar_and_bbox_from_folder(
        self, lidar_dir: str
    ) -> tuple[np.ndarray, np.ndarray]:
        """For a given data folder, read in the contents of the .tif file
        within as a numpy array.

        Args:
            lidar_dir: The location of the data folder to be loaded

        Returns:
            The contents of the .tif file within the provided data folder, and
            the bounding box for the area it covers. Each lidar file represents
            an area of 5km^2, so the shape of the first array will be
            5000*5000. The bounding box will be an array of 4 integers
            corresponding to:
                - easting_min
                - northing_min
                - easting_max
                - northing_max

        """
        with zipfile.ZipFile(lidar_dir, mode="r") as archive:
            tif_loc, xml_loc = self._get_filenames_from_archive(archive)

            with rio.open(archive.open(tif_loc)) as tif:
                lidar = tif.read()

            xml = archive.read(xml_loc).decode("utf8")
            bbox = self._get_bbox_from_xml(xml)

        lidar = lidar[0]
        return lidar, bbox

    @staticmethod
    def generate_df_from_lidar_array(
        lidar: np.ndarray, bbox: np.ndarray
    ) -> pl.DataFrame:
        """Parse a given array containing LIDAR data, and the corresponding
        bounding box. Returns a long-form dataframe containing eastings,
        northings and elevations.

        Args:
            lidar: An array containing LIDAR data
            bbox: The bounding box corresponding to the provided lidar array

        Returns:
            A dataframe containing the 'easting', 'northing' and 'elevation'
            columns

        """
        # Get array dimensions
        size_e, size_s = lidar.shape

        # Collapse elevations to 1 dimension, left to right then top to bottom
        elevations = lidar.flatten(order="C")

        # Repeat eastings by array (A, B, A, B)
        eastings = np.tile(range(bbox[0], bbox[2]), size_e).astype("int32")

        # Repeat northings by element (A, A, B, B), stepping backwards as row
        # 0 is the northernmost section of the grid
        northings = np.repeat(
            range(bbox[3] - 1, bbox[1] - 1, -1), size_s
        ).astype("int32")

        df = pl.DataFrame(
            {
                "easting": eastings,
                "northing": northings,
                "elevation": elevations,
            },
            orient="row",
        )

        return df

    @staticmethod
    def set_output_schema(lidar_df: pl.DataFrame) -> pl.DataFrame:
        """Set expected column order

        Args:
            lidar_df: The parsed lidar dataframe

        Returns:
            A copy of the input dataframe with an updated schema

        """
        lidar_df = lidar_df.select("easting", "northing", "elevation")

        return lidar_df

    def parse_lidar_folder(
        self, lidar_dir: str
    ) -> tuple[pl.DataFrame, np.ndarray]:
        """This function will read in the contents of a single LIDAR folder,
        transform it into a tabular format and return its contents as a pySpark
        Dataframe

        Args:
            lidar_dir: The location of the LIDAR folder to be loaded

        Returns:
            The contents of the provided LIDAR folder, and an array
            representing its bounding box

        """
        logger.info(f"Processing LIDAR data in {lidar_dir}")
        lidar, bbox = self.load_lidar_and_bbox_from_folder(lidar_dir)

        lidar_df = self.generate_df_from_lidar_array(lidar, bbox)

        lidar_df = add_bng_partition_to_polars_df(lidar_df)
        lidar_df = self.set_output_schema(lidar_df)
        return lidar_df, bbox

    def write_df_to_parquet(
        self, file_id: str, lidar_df: pl.DataFrame
    ) -> None:
        """Write the contents of a dataframe containing lidar data to the
        specified location. Data will be written in the parquet format, with
        partitioning set on the `ptn` column

        Args:
            file_id: The unique identifier for the lidar file which has been
                parsed
            lidar_df: A dataframe containing lidar data

        """
        tgt_loc = os.path.join(
            self.data_dir, "landing", "lidar", f"{file_id}.parquet"
        )

        logger.info(f"Writing LIDAR data to {tgt_loc}")
        lidar_df.write_parquet(tgt_loc, compression="snappy", use_pyarrow=True)

    def process_lidar_file(self, file_spec: tuple[str, str]) -> None:
        """Process a single zip file containing LIDAR data. If the file has
        already been processed, no action will be taken. If an error is
        encountered during process, the name of the offending file will be
        written to bad_files.txt (in the current working directory).

        Note that this accepts a single tuple as an argument so that it can
        be used more easily with parallel processing libraries.

        Args:
            file_spec: A tuple containing the unique identifier for the file
                to be loaded, and its path on the filesystem.

        """
        file_id, lidar_dir = file_spec

        try:
            lidar_df, bbox = self.parse_lidar_folder(lidar_dir)
            self.write_df_to_parquet(file_id, lidar_df)
            self.dc.update([(file_id, bbox)])
            del lidar_df
        except pl.exceptions.ShapeError:
            logger.warning(f"Unable to parse file {lidar_dir}")

    def write_bounds_to_parquet(self) -> None:
        """Write the bounding box for a file ID to disk. This needs to be a
        thread-safe operation as multiple files may be processed at once

        Args:
            file_id: The unique identifier for the lidar file which has been
                parsed
            bbox: The bounding box for the file

        """
        logger.info("Updating lidar_bounds table")
        records = []
        for file_id, (
            min_easting,
            min_northing,
            max_easting,
            max_northing,
        ) in self.dc.items():  # type: ignore
            records.append(
                dict(
                    file_id=file_id,
                    min_easting=min_easting,
                    min_northing=min_northing,
                    max_easting=max_easting,
                    max_northing=max_northing,
                )
            )

        pl.from_dicts(records).write_parquet(
            os.path.join(self.data_dir, "landing/lidar_bounds.parquet")
        )

    def load(self) -> None:
        """Primary user facing function for this class. Parses every available
        LIDAR extract and stores the output as a partitioned parquet dataset.

        Data will be written to `data/landing/lidar` within the configured
        data_dir
        """
        if self.to_load:
            process_map(
                self.process_lidar_file,
                self.to_load.items(),
                desc="Parsing LIDAR data",
                chunksize=1,
                max_workers=MAX_WORKERS,
            )

            self.write_bounds_to_parquet()
