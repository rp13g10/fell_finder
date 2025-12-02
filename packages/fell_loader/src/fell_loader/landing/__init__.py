"""First ingestion layer, simply brings in the raw data in a format which is
suitable for downstream processing
"""

from fell_loader.landing.lidar import LidarLoader
from fell_loader.landing.osm import OsmLoader

__all__ = ["LidarLoader", "OsmLoader"]
