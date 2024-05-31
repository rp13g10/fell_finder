"""Handles the joining of graph data with the corresponding elevation data"""

from pyspark.sql import SparkSession


class GraphEnricher:
    def __init__(self, spark: SparkSession):
        self.spark = spark
