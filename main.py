"""Primary execution script (for now). Triggers ingestion of LIDAR and OSM
data, joins the two datasets together to create a single augmented graph."""

from fell_finder.ingestion import LidarLoader, OsmLoader

DATA_DIR = "/home/ross/repos/fell_finder/data"

lidar_loader = LidarLoader(DATA_DIR)
lidar_loader.load()
del lidar_loader

osm_loader = OsmLoader(DATA_DIR)
osm_loader.load()
del osm_loader
