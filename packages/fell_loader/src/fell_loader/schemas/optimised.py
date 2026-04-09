"""Schemas for datasets in the optimised layer"""

from pyspark.sql import types as T

NODES_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("elevation", T.DoubleType()),
        T.StructField("ptn", T.StringType()),
    ]
)

EDGES_SCHEMA = T.StructType(
    [
        T.StructField("src", T.LongType()),
        T.StructField("dst", T.LongType()),
        T.StructField("src_lat", T.DoubleType()),
        T.StructField("src_lon", T.DoubleType()),
        T.StructField("highway", T.StringType()),
        T.StructField("surface", T.StringType()),
        T.StructField("elevation_gain", T.DoubleType()),
        T.StructField("elevation_loss", T.DoubleType()),
        T.StructField("distance", T.DoubleType()),
        T.StructField("lats", T.ArrayType(T.DoubleType())),
        T.StructField("lons", T.ArrayType(T.DoubleType())),
        T.StructField("eles", T.ArrayType(T.DoubleType())),
        T.StructField("dists", T.ArrayType(T.DoubleType())),
        T.StructField("ptn", T.StringType()),
    ]
)
