"""Schemas for datasets in the landing layer"""

from pyspark.sql import types as T

LIDAR_SCHEMA = T.StructType(
    [
        T.StructField("easting", T.IntegerType()),
        T.StructField("northing", T.IntegerType()),
        T.StructField("elevation", T.DoubleType()),
    ]
)

NODES_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("easting", T.IntegerType()),
        T.StructField("northing", T.IntegerType()),
        T.StructField("timestamp", T.LongType()),
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
        T.StructField("src_easting", T.IntegerType()),
        T.StructField("src_northing", T.IntegerType()),
        T.StructField("dst_easting", T.IntegerType()),
        T.StructField("dst_northing", T.IntegerType()),
        T.StructField("tags", T.MapType(T.StringType(), T.StringType())),
        T.StructField("timestamp", T.LongType()),
    ]
)
