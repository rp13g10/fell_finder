"""Primary execution script (for now). Triggers ingestion of LIDAR and OSM
data, joins the two datasets together to create a single augmented graph."""

from pyspark.sql import SparkSession

from fell_finder.ingestion import LidarLoader, OsmLoader
from fell_finder.enrichment import GraphEnricher
from fell_finder.contraction import GraphContractor
from fell_finder.selection import Selector

from fell_finder.containers.routes import RouteConfig

DATA_DIR = "/home/ross/repos/fell_finder/data"

lidar_loader = LidarLoader(DATA_DIR)
lidar_loader.load()
del lidar_loader

osm_loader = OsmLoader(DATA_DIR)
osm_loader.load()
del osm_loader

# Config set for testing on personal laptop, will need tuning for cloud envs
spark = (
    SparkSession.builder.appName("fell_finder")  # type: ignore
    .config("spark.master", "local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.driver.memoryOverhead", "2g")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.memoryOverhead", "2g")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .config("spark.sql.files.maxPartitionBytes", "1048576")
    .config("spark.sql.hive.filesourcePartitionFileCacheSize", "1048576000")
    .getOrCreate()
)

graph_enricher = GraphEnricher(DATA_DIR, spark)
graph_enricher.enrich()
del graph_enricher

graph_contractor = GraphContractor(DATA_DIR, spark)
graph_contractor.contract()
del graph_contractor

spark.stop()

config = RouteConfig(
    start_lat=50.9690528,
    start_lon=-1.3832098,
    target_distance=10000.0,
    tolerance=0.05,
    route_mode="hilly",
    max_candidates=512,
)

selector = Selector(config, DATA_DIR)
start_inx, graph = selector.create_graph()
