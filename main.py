"""Primary execution script (for now). Triggers ingestion of LIDAR and OSM
data, joins the two datasets together to create a single augmented graph."""

from fell_finder.ingestion import LidarLoader

DATA_DIR = "/home/ross/repos/fell_finder/data"

lidar_parser = LidarLoader(DATA_DIR)
lidar_parser.load()
