"""Quick script to step through the ingestion process, checking to see where
a particular way is dropping out & why."""

# ruff: noqa: ERA001, F401, E501

import os

from fell_loader import (
    OsmLoader,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("fell_finder")  # type: ignore
    .config("spark.master", "local[*]")
    .config("spark.driver.memory", "16g")
    .config("spark.driver.memoryOverhead", "4g")
    .config("spark.sql.files.maxPartitionBytes", "67108864")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "512")
    .config("spark.local.dir", os.path.join(os.environ["FF_DATA_DIR"], "temp"))
    .config("spark.log.level", "WARN")
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    .getOrCreate()
)

self = OsmLoader(spark)

nodes, ways = self.read_osm_data()

nodes = self.assign_bng_coords(nodes)
nodes = self.set_node_output_schema(nodes)

ways = ways.filter(F.col("id") == 286723911)

ways = self.unpack_tags(ways)
ways = self.get_roads_and_paths(ways)
ways = self.flag_footways(ways)

ways = self.remove_restricted_routes(ways)
ways = self.remove_unsafe_routes(ways)
ways = self.set_flat_flag(ways)
ways = self.set_oneway_flag(ways)
ways = self.get_tag_as_column(ways, "surface")

edges = self.generate_edges(ways)
edges = self.add_reverse_edges(edges)
edges = self.get_edge_start_coords(nodes, edges)
edges = self.get_edge_end_coords(nodes, edges)
edges = self.set_edge_output_schema(edges)
