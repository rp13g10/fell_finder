"""Container objects which correspond to physical geography."""

from dataclasses import dataclass
from bng_latlon import WGS84toOSGB36

from fell_finder.utils.partitioning import get_partitions


@dataclass
class BBox:
    """Contains information about the physical boundaries of one or more
    routes

    Args:
        min_lat (float): Minimum latitude
        min_lon (float): Minimum longitude
        max_lat (float): Maximum latitude
        max_lon (float): Maximum longitude"""

    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

    def __post_init__(self):
        """After initialization, fetch the corresponding partitions for the
        provided coordinates"""
        min_easting, min_northing = WGS84toOSGB36(self.min_lat, self.min_lon)
        max_easting, max_northing = WGS84toOSGB36(self.max_lat, self.max_lon)

        min_easting_ptn, min_northing_ptn = get_partitions(
            min_easting, min_northing
        )
        max_easting_ptn, max_northing_ptn = get_partitions(
            max_easting, max_northing
        )

        self.min_easting_ptn = min_easting_ptn
        self.min_northing_ptn = min_northing_ptn
        self.max_easting_ptn = max_easting_ptn
        self.max_northing_ptn = max_northing_ptn
