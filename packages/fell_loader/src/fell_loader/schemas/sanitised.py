"""Schemas for datasets in the sanitised layer"""

from pyspark.sql import types as T

NODES_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("elevation", T.DoubleType()),
    ]
)

EDGES_SCHEMA = T.StructType(
    [
        T.StructField("src", T.LongType()),
        T.StructField("dst", T.LongType()),
        T.StructField("way_id", T.LongType()),
        T.StructField("way_inx", T.IntegerType()),
        T.StructField("src_lat", T.DoubleType()),
        T.StructField("src_lon", T.DoubleType()),
        T.StructField("dst_lat", T.DoubleType()),
        T.StructField("dst_lon", T.DoubleType()),
        T.StructField("highway", T.StringType()),
        T.StructField("surface", T.StringType()),
        T.StructField("elevation_gain", T.DoubleType()),
        T.StructField("elevation_loss", T.DoubleType()),
        T.StructField("distance", T.DoubleType()),
    ]
)
