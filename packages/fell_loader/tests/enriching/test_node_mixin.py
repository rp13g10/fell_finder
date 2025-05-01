"""Tests for methods relating to enrichment of nodes"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from fell_loader.enriching.node_mixin import NodeMixin


def test_tag_nodes(test_session: SparkSession):
    """Make sure the table join has been set up properly"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # ----- Nodes -----

    # fmt: off
    _ = (
        ['easting', 'northing', 'easting_ptn', 'northing_ptn', 'other_nodes'])

    test_node_data = [
        ['left'   , 'left'    , 'left'       , 'left'        , 'other_nodes'],
        ['both'   , 'both'    , 'both'       , 'both'        , 'other_nodes'],
    ]
    # fmt: on

    test_node_schema = StructType(
        [
            StructField("easting", StringType()),
            StructField("northing", StringType()),
            StructField("easting_ptn", StringType()),
            StructField("northing_ptn", StringType()),
            StructField("other_nodes", StringType()),
        ]
    )

    test_node_df = test_session.createDataFrame(
        test_node_data, test_node_schema
    )

    # ----- Elevation -----

    # fmt: off
    _ = (
        ['easting', 'northing', 'easting_ptn', 'northing_ptn', 'other_elevation'])

    test_ele_data = [
        ['both'   , 'both'    , 'both'       , 'both'        , 'other_elevation'],
        ['right'  , 'right'   , 'right'      , 'right'       , 'other_elevation']
    ]
    # fmt: on

    test_ele_schema = StructType(
        [
            StructField("easting", StringType()),
            StructField("northing", StringType()),
            StructField("easting_ptn", StringType()),
            StructField("northing_ptn", StringType()),
            StructField("other_elevation", StringType()),
        ]
    )

    test_ele_df = test_session.createDataFrame(test_ele_data, test_ele_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['easting', 'northing', 'easting_ptn', 'northing_ptn', 'other_nodes', 'other_elevation'])

    target_data = [
        ['both'   , 'both'    , 'both'       , 'both'        , 'other_nodes', 'other_elevation']
    ]
    # fmt: on

    target_schema = StructType(
        [
            StructField("easting", StringType()),
            StructField("northing", StringType()),
            StructField("easting_ptn", StringType()),
            StructField("northing_ptn", StringType()),
            StructField("other_nodes", StringType()),
            StructField("other_elevation", StringType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = NodeMixin.tag_nodes(test_node_df, test_ele_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_set_node_output_schema(test_session: SparkSession):
    """Make sure the correct fields are being brought through"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    test_cols = [
        "id",
        "lat",
        "lon",
        "elevation",
        "easting_ptn",
        "northing_ptn",
        "other",
    ]

    test_data = [[0 for _ in test_cols]]

    test_df = test_session.createDataFrame(test_data, test_cols)

    # Target Data -------------------------------------------------------------

    target_cols = [
        "id",
        "lat",
        "lon",
        "elevation",
        "easting_ptn",
        "northing_ptn",
    ]

    target_data = [[0 for _ in target_cols]]

    target_df = test_session.createDataFrame(target_data, target_cols)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = NodeMixin.set_node_output_schema(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)
