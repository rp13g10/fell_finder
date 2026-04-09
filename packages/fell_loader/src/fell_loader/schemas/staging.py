"""Schemas for datasets in the staging layer"""

from pyspark.sql import types as T

NODES_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("elevation", T.DoubleType()),
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
        T.StructField("elevation", T.ArrayType(T.DoubleType())),
        T.StructField("tags", T.MapType(T.StringType(), T.StringType())),
        T.StructField("timestamp", T.LongType()),
    ]
)
