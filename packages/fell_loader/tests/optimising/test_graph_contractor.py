"""Tests for the GraphContractor class"""

import inspect
from unittest.mock import MagicMock, call, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual

from fell_loader.optimising.graph_contractor import GraphContractor


@patch("fell_loader.optimising.graph_contractor.glob")
class TestGetAvailablePartitions:
    """Make sure partitions can correctly be identified from the filesystem"""

    def test_match_found(self, mock_glob: MagicMock):
        """Successful extraction"""

        # Arrange
        mock_glob.return_value = [
            "/home/ross/repos/fell_finder/data/parsed/edges/easting_ptn=81/northing_ptn=23/0a9a05d0bb264860986c1e2431beb29f-0.parquet",
            "/home/ross/repos/fell_finder/data/parsed/lidar/easting_ptn=88/northing_ptn=23/b935432376cd47f4b067cee64328a019-0.parquet",
            "/home/ross/repos/fell_finder/data/parsed/nodes/easting_ptn=81/northing_ptn=23/e35da41d3a0a48ccaed8ea2090d75d26-0.parquet",
        ]

        target = {(81, 23), (88, 23)}

        test_enricher = GraphContractor("data_dir", "dummy_spark")  # type: ignore

        # Act
        result = test_enricher.get_available_partitions()

        # Assert
        assert result == target
        mock_glob.assert_called_once_with(
            "data_dir/enriched/nodes/**/*.parquet", recursive=True
        )

    def test_no_match_found(self, mock_glob: MagicMock):
        """Raise an error if no match identified"""
        # Arrange
        mock_glob.return_value = [
            "/home/ross/repos/fell_finder/data/parsed/edges/easting_ptn=81/0a9a05d0bb264860986c1e2431beb29f-0.parquet",
            "/home/ross/repos/fell_finder/data/parsed/lidar/easting_ptn=88/b935432376cd47f4b067cee64328a019-0.parquet",
            "/home/ross/repos/fell_finder/data/parsed/nodes/easting_ptn=81/e35da41d3a0a48ccaed8ea2090d75d26-0.parquet",
        ]

        test_enricher = GraphContractor("data_dir", "dummy_spark")  # type: ignore

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            _ = test_enricher.get_available_partitions()


def test_load_df():
    """Make sure the correct read calls are being generated"""

    # Arrange
    mock_spark = MagicMock()
    mock_contractor = GraphContractor("data_dir", mock_spark)
    mock_contractor.num_ptns = 123

    mock_df = MagicMock()
    mock_contractor.spark.read.parquet.return_value = mock_df

    test_dataset = "nodes"
    target_read_call = "data_dir/enriched/nodes"
    target_repartition_args = (123, "easting_ptn", "northing_ptn")

    # Act
    mock_contractor.load_df(test_dataset)

    # Assert
    mock_contractor.spark.read.parquet.assert_called_once_with(
        target_read_call
    )
    mock_df.repartition.assert_called_once_with(*target_repartition_args)


def test_get_node_degrees(test_session: SparkSession):
    """Make sure node degrees are being calculated properly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'other'])

    test_data = [
        # One in, no out
        [10   , 0    , 'other'],
        # One out, no in
        [1    , 11   , 'other'],
        # One in, one out
        [12   , 2    , 'other'],
        [2    , 12   , 'other'],
        # Two in, two out
        [13   , 3    , 'other'],
        [23   , 3    , 'other'],
        [3    , 13   , 'other'],
        [3    , 33   , 'other'],
        # Three in, two out
        [14   , 4    , 'other'],
        [24   , 4    , 'other'],
        [34   , 4    , 'other'],
        [4    , 14   , 'other'],
        [4    , 24   , 'other']
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("other", StringType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['id', 'in_degree', 'out_degree', 'degree'])

    tgt_data = [
        # One in, no out
        [0   , 1          , 0           , 1],
        [10  , 0          , 1           , 1],
        # One out, no in
        [1   , 0          , 1           , 1],
        [11  , 1          , 0           , 1],
        # One in, one out
        [2   , 1          , 1           , 1],
        [12  , 1          , 1           , 1],
        # Two in, two out
        [3   , 2          , 2           , 3],
        [13  , 1          , 1           , 1],
        [23  , 0          , 1           , 1],
        [33  , 1          , 0           , 1],
        # Three in, two out
        [4   , 3          , 2           , 3],
        [14  , 1          , 1           , 1],
        [24  , 1          , 1           , 1],
        [34  , 0          , 1           , 1],
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("in_degree", LongType()),
            StructField("out_degree", LongType()),
            StructField("degree", LongType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = GraphContractor._get_node_degrees(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_add_degrees_to_nodes(test_session: SparkSession):
    """Make sure the table join has been set up properly"""

    # Arrange #################################################################

    test_contractor = GraphContractor("data_dir", "spark")  # type: ignore

    # Test Data ---------------------------------------------------------------

    # ----- Nodes -----
    # fmt: off
    _ = (
        ['id', 'node_attr'])

    test_node_data = [
        # Edges present
        [0   , 'node_attr'],
        # Edges not present
        [1   , 'node_attr'],
    ]
    # fmt: on

    test_node_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("node_attr", StringType()),
        ]
    )

    test_node_df = test_session.createDataFrame(
        test_node_data, test_node_schema
    )

    # ----- Degrees -----
    # fmt: off
    _ = (
        ['id', 'degree'])

    test_deg_data = [
        # Edges present
        [0   , 'degree']
        # Edges not present
    ]
    # fmt: on

    test_deg_schema = StructType(
        [StructField("id", IntegerType()), StructField("degree", StringType())]
    )

    test_deg_df = test_session.createDataFrame(test_deg_data, test_deg_schema)

    test_contractor._get_node_degrees = MagicMock(return_value=test_deg_df)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['id', 'node_attr', 'degree'])

    tgt_data = [
        # Edges present
        [0   , 'node_attr', 'degree'],
        # Edges not present
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("node_attr", StringType()),
            StructField("degree", StringType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = test_contractor.add_degrees_to_nodes(test_node_df, "dummy")  # type: ignore

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_derive_node_flags(test_session: SparkSession):
    """Make sure nodes are correctly being flagged for contraction
    or removal"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['id', 'degree'])

    test_data = [
        [0   , 1],
        [1   , 2],
        [2   , 3]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("degree", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['id', 'degree', 'contract_flag', 'dead_end_flag'])

    tgt_data = [
        [0   , 1       , 0              , 1],
        [1   , 2       , 1              , 0],
        [2   , 3       , 0              , 0]
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("degree", IntegerType()),
            StructField("contract_flag", IntegerType(), False),
            StructField("dead_end_flag", IntegerType(), False),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = GraphContractor.derive_node_flags(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_derive_way_start_end_flags(test_session: SparkSession):
    """Make sure the first & last nodes in each way are being flagged
    properly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['id', 'way_id', 'way_inx'])

    test_data = [
        # Single node
        [0   , 0       , 1],
        # Two nodes
        [1   , 1       , 1],
        [2   , 1       , 2],
        # Three nodes
        [3   , 2       , 1],
        [4   , 2       , 2],
        [5   , 2       , 3]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("way_id", IntegerType()),
            StructField("way_inx", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['id', 'way_id', 'way_inx', 'way_start_flag', 'way_end_flag'])

    tgt_data = [
        # Single node
        [0   , 0       , 1        , 1               , 1],
        # Two nodes
        [1   , 1       , 1        , 1               , 0],
        [2   , 1       , 2        , 0               , 1],
        # Three nodes
        [3   , 2       , 1        , 1               , 0],
        [4   , 2       , 2        , 0               , 0],
        [5   , 2       , 3        , 0               , 1]
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("way_id", IntegerType()),
            StructField("way_inx", IntegerType()),
            StructField("way_start_flag", IntegerType(), False),
            StructField("way_end_flag", IntegerType(), False),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = GraphContractor.derive_way_start_end_flags(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_derive_chain_src_dst(test_session: SparkSession):
    """Make sure that the chain source/destination nodes are being detected
    properly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # ----- Nodes -----
    # fmt: off
    _ = (
        ['id', 'contract_flag', 'elevation', 'dead_end_flag'])

    test_node_data = [
        # Way length 1
        [0   , 1              , 0          , 0],
        [1   , 1              , 1          , 0],
        # Way length 2
        [10  , 1              , 0          , 0],
        [11  , 1              , 1          , 0],
        [12  , 1              , 2          , 0],
        # Way length 3
        [20  , 1              , 0          , 0],
        [21  , 1              , 1          , 0],
        [22  , 1              , 2          , 0],
        [23  , 1              , 3          , 0],
        # Way length 5, middle is a junction
        [30  , 1              , 0          , 0],
        [31  , 1              , 1          , 0],
        [32  , 1              , 2          , 0],
        [33  , 0              , 3          , 0],
        [34  , 1              , 4          , 0],
        [35  , 1              , 5          , 0]
    ]
    # fmt: on

    test_node_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("contract_flag", IntegerType()),
            StructField("elevation", IntegerType()),
            StructField("dead_end_flag", IntegerType()),
        ]
    )

    test_node_df = test_session.createDataFrame(
        test_node_data, test_node_schema
    )

    # ----- Edges -----
    # fmt: off
    _ = (
        ['src', 'dst', 'way_start_flag', 'way_end_flag'])

    test_edge_data = [
        # Way length 1
        [0    , 1    , 1               , 1],
        # Way length 2
        [10   , 11   , 1               , 0],
        [11   , 12   , 0               , 1],
        # Way length 3
        [20   , 21   , 1               , 0],
        [21   , 22   , 0               , 0],
        [22   , 23   , 0               , 1],
        # Way length 5, middle is a junction
        [30   , 31   , 1               , 0],
        [31   , 32   , 0               , 0],
        [32   , 33   , 0               , 0],
        [33   , 34   , 0               , 0],
        [34   , 35   , 0               , 1],
    ]
    # fmt: on

    test_edge_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("way_start_flag", IntegerType()),
            StructField("way_end_flag", IntegerType()),
        ]
    )

    test_edge_df = test_session.createDataFrame(
        test_edge_data, test_edge_schema
    )

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'chain_src', 'chain_dst', 'src_elevation', 'src_dead_end_flag', 'dst_elevation', 'dst_dead_end_flag'])

    tgt_data = [
        # Way length 1
        [0    , 1    , 0         , 1           , 0              , 0                  , 1              , 0],
        # Way length 2
        [10   , 11   , 10        , None        , 0              , 0                  , 1              , 0],
        [11   , 12   , None      , 12          , 1              , 0                  , 2              , 0],
        # Way length 3
        [20   , 21   , 20        , None        , 0              , 0                  , 1              , 0],
        [21   , 22   , None      , None        , 1              , 0                  , 2              , 0],
        [22   , 23   , None      , 23          , 2              , 0                  , 3              , 0],
        # Way length 5, middle is a junction
        [30   , 31   , 30        , None        , 0              , 0                  , 1              , 0],
        [31   , 32   , None      , None        , 1              , 0                  , 2              , 0],
        [32   , 33   , None      , 33          , 2              , 0                  , 3              , 0],
        [33   , 34   , 33        , None        , 3              , 0                  , 4              , 0],
        [34   , 35   , None      , 35          , 4              , 0                  , 5              , 0],
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("chain_src", IntegerType()),
            StructField("chain_dst", IntegerType()),
            StructField("src_elevation", IntegerType()),
            StructField("src_dead_end_flag", IntegerType()),
            StructField("dst_elevation", IntegerType()),
            StructField("dst_dead_end_flag", IntegerType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)
    tgt_df = tgt_df.select(*sorted(tgt_df.columns))

    # Act #####################################################################
    res_df = GraphContractor.derive_chain_src_dst(test_node_df, test_edge_df)
    res_df = res_df.select(*sorted(res_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_propagate_chain_src_dst(test_session: SparkSession):
    """Make sure that chain source/destination nodes are being propagated
    down the chain correctly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'way_id', 'way_inx', 'chain_src', 'chain_dst'])

    test_data = [
        # Way length 1
        [0    , 1    , 0       , 1        , 0         , 1],
        # Way length 2
        [10   , 11   , 1       , 1        , 10        , None],
        [11   , 12   , 1       , 2        , None      , 12],
        # Way length 3
        [20   , 21   , 2       , 1        , 20        , None],
        [21   , 22   , 2       , 2        , None      , None],
        [22   , 23   , 2       , 3        , None      , 23],
        # Way length 5, middle is a junction
        [30   , 31   , 3       , 1        , 30        , None],
        [31   , 32   , 3       , 2        , None      , None],
        [32   , 33   , 3       , 3        , None      , 33],
        [33   , 34   , 3       , 4        , 33        , None],
        [34   , 35   , 3       , 5        , None      , 35],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("way_id", IntegerType()),
            StructField("way_inx", IntegerType()),
            StructField("chain_src", IntegerType()),
            StructField("chain_dst", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'way_id', 'way_inx', 'chain_src', 'chain_dst'])

    tgt_data = [
        # Way length 1
        [0    , 1    , 0       , 1        , 0         , 1],
        # Way length 2
        [10   , 11   , 1       , 1        , 10        , 12],
        [11   , 12   , 1       , 2        , 10        , 12],
        # Way length 3
        [20   , 21   , 2       , 1        , 20        , 23],
        [21   , 22   , 2       , 2        , 20        , 23],
        [22   , 23   , 2       , 3        , 20        , 23],
        # Way length 5, middle is a junction
        [30   , 31   , 3       , 1        , 30        , 33],
        [31   , 32   , 3       , 2        , 30        , 33],
        [32   , 33   , 3       , 3        , 30        , 33],
        [33   , 34   , 3       , 4        , 33        , 35],
        [34   , 35   , 3       , 5        , 33        , 35],
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("way_id", IntegerType()),
            StructField("way_inx", IntegerType()),
            StructField("chain_src", IntegerType()),
            StructField("chain_dst", IntegerType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = GraphContractor.propagate_chain_src_dst(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_contract_chains(test_session: SparkSession):
    """Make sure that chains are being collapsed properly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['chain_src', 'chain_dst', 'way_inx', 'highway'  , 'surface'  , 'elevation_gain', 'elevation_loss', 'distance', 'easting_ptn', 'northing_ptn', 'src_lat', 'src_lon', 'src_elevation', 'src_dead_end_flag', 'dst_lat', 'dst_lon', 'dst_elevation', 'dst_dead_end_flag'])

    test_data = [
        # Single record
        [0          , 1          , 1        , 'highway_1', 'surface_1', 1.0             , 1.0             , 1.0       , 12           , 12            , 1.0      , 1.0      , 1.0            , 0                  , 1.1     , 1.1       , 1.1            , 0],
        # Two records
        [1          , 2          , 1        , 'highway_1', 'surface_1', 1.0             , 1.0             , 1.0       , 12           , 12            , 1.0      , 1.0      , 1.0            , 0                  , 1.1     , 1.1       , 1.1            , 0],
        [1          , 2          , 2        , 'highway_1', 'surface_1', 1.0             , 1.0             , 1.0       , 12           , 12            , 1.1      , 1.1      , 1.1            , 0                  , 1.2     , 1.2       , 1.2            , 0],
        # Three records
        [2          , 3          , 1        , 'highway_1', 'surface_1', 1.0             , 1.0             , 1.0       , 12           , 12            , 1.0      , 1.0      , 1.0            , 0                  , 1.1     , 1.1       , 1.1            , 0],
        [2          , 3          , 2        , 'highway_1', 'surface_1', 1.0             , 1.0             , 1.0       , 12           , 12            , 1.1      , 1.1      , 1.1            , 0                  , 1.2     , 1.2       , 1.2            , 0],
        [2          , 3          , 3        , 'highway_2', 'surface_2', 1.0             , 1.0             , 1.0       , 12           , 12            , 1.2      , 1.2      , 1.2            , 0                  , 1.3     , 1.3       , 1.3            , 0],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("chain_src", IntegerType()),
            StructField("chain_dst", IntegerType()),
            StructField("way_inx", IntegerType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
            StructField("elevation_gain", DoubleType()),
            StructField("elevation_loss", DoubleType()),
            StructField("distance", DoubleType()),
            StructField("easting_ptn", IntegerType()),
            StructField("northing_ptn", IntegerType()),
            StructField("src_lat", DoubleType()),
            StructField("src_lon", DoubleType()),
            StructField("src_elevation", DoubleType()),
            StructField("src_dead_end_flag", IntegerType()),
            StructField("dst_lat", DoubleType()),
            StructField("dst_lon", DoubleType()),
            StructField("dst_elevation", DoubleType()),
            StructField("dst_dead_end_flag", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # Complex fields - Single record
    geom_1 = [
        {"way_inx": 1, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12}
    ]
    src_geom_1 = [
        {
            "way_inx": 1,
            "src_lat": 1.0,
            "src_lon": 1.0,
            "src_elevation": 1.0,
            "src_dead_end_flag": 0,
        }
    ]
    dst_geom_1 = [
        {
            "way_inx": 1,
            "dst_lat": 1.1,
            "dst_lon": 1.1,
            "dst_elevation": 1.1,
            "dst_dead_end_flag": 0,
        }
    ]

    # Complex fields - Two records
    geom_2 = [
        {"way_inx": 1, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12},
        {"way_inx": 2, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12},
    ]
    src_geom_2 = [
        {
            "way_inx": 1,
            "src_lat": 1.0,
            "src_lon": 1.0,
            "src_elevation": 1.0,
            "src_dead_end_flag": 0,
        },
        {
            "way_inx": 2,
            "src_lat": 1.1,
            "src_lon": 1.1,
            "src_elevation": 1.1,
            "src_dead_end_flag": 0,
        },
    ]
    dst_geom_2 = [
        {
            "way_inx": 1,
            "dst_lat": 1.1,
            "dst_lon": 1.1,
            "dst_elevation": 1.1,
            "dst_dead_end_flag": 0,
        },
        {
            "way_inx": 2,
            "dst_lat": 1.2,
            "dst_lon": 1.2,
            "dst_elevation": 1.2,
            "dst_dead_end_flag": 0,
        },
    ]

    # Complex fields - Three records
    geom_3 = [
        {"way_inx": 1, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12},
        {"way_inx": 2, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12},
        {"way_inx": 3, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12},
    ]
    src_geom_3 = [
        {
            "way_inx": 1,
            "src_lat": 1.0,
            "src_lon": 1.0,
            "src_elevation": 1.0,
            "src_dead_end_flag": 0,
        },
        {
            "way_inx": 2,
            "src_lat": 1.1,
            "src_lon": 1.1,
            "src_elevation": 1.1,
            "src_dead_end_flag": 0,
        },
        {
            "way_inx": 3,
            "src_lat": 1.2,
            "src_lon": 1.2,
            "src_elevation": 1.2,
            "src_dead_end_flag": 0,
        },
    ]
    dst_geom_3 = [
        {
            "way_inx": 1,
            "dst_lat": 1.1,
            "dst_lon": 1.1,
            "dst_elevation": 1.1,
            "dst_dead_end_flag": 0,
        },
        {
            "way_inx": 2,
            "dst_lat": 1.2,
            "dst_lon": 1.2,
            "dst_elevation": 1.2,
            "dst_dead_end_flag": 0,
        },
        {
            "way_inx": 3,
            "dst_lat": 1.3,
            "dst_lon": 1.3,
            "dst_elevation": 1.3,
            "dst_dead_end_flag": 0,
        },
    ]

    # fmt: off
    _ = (
        ['chain_src', 'chain_dst', 'highway'                 , 'surface'                 , 'elevation_gain', 'elevation_loss', 'distance', 'geom', 'src_geom', 'dst_geom'])

    tgt_data = [
        # Single record
        [0          , 1          , ['highway_1']             , ['surface_1']             , 1.0             , 1.0             , 1.0       , geom_1, src_geom_1, dst_geom_1],
        # Two records
        [1          , 2          , ['highway_1']             , ['surface_1']             , 2.0             , 2.0             , 2.0       , geom_2, src_geom_2, dst_geom_2],
        # Three records
        [2          , 3          , ['highway_1', 'highway_2'], ['surface_1', 'surface_2'], 3.0             , 3.0             , 3.0       , geom_3, src_geom_3, dst_geom_3],
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("chain_src", IntegerType()),
            StructField("chain_dst", IntegerType()),
            StructField("highway", ArrayType(StringType(), False), False),
            StructField("surface", ArrayType(StringType(), False), False),
            StructField("elevation_gain", DoubleType()),
            StructField("elevation_loss", DoubleType()),
            StructField("distance", DoubleType()),
            StructField(
                "geom",
                ArrayType(
                    StructType(
                        [
                            StructField("way_inx", IntegerType()),
                            StructField("distance", DoubleType()),
                            StructField("easting_ptn", IntegerType()),
                            StructField("northing_ptn", IntegerType()),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "src_geom",
                ArrayType(
                    StructType(
                        [
                            StructField("way_inx", IntegerType()),
                            StructField("src_lat", DoubleType()),
                            StructField("src_lon", DoubleType()),
                            StructField("src_elevation", DoubleType()),
                            StructField("src_dead_end_flag", IntegerType()),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "dst_geom",
                ArrayType(
                    StructType(
                        [
                            StructField("way_inx", IntegerType()),
                            StructField("dst_lat", DoubleType()),
                            StructField("dst_lon", DoubleType()),
                            StructField("dst_elevation", DoubleType()),
                            StructField("dst_dead_end_flag", IntegerType()),
                        ]
                    ),
                    False,
                ),
                False,
            ),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = GraphContractor.contract_chains(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_generate_new_edges_from_chains(test_session: SparkSession):
    """Make sure that new edges are being generated correctly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # Complex fields - Single record
    geom_1 = [
        {"way_inx": 1, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12}
    ]
    src_geom_1 = [
        {
            "way_inx": 1,
            "src_lat": 1.0,
            "src_lon": 1.0,
            "src_elevation": 1.0,
            "src_dead_end_flag": 0,
        }
    ]
    dst_geom_1 = [
        {
            "way_inx": 1,
            "dst_lat": 1.1,
            "dst_lon": 1.1,
            "dst_elevation": 1.1,
            "dst_dead_end_flag": 0,
        }
    ]

    # Complex fields - Two records
    geom_2 = [
        {"way_inx": 1, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12},
        {"way_inx": 2, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12},
    ]
    src_geom_2 = [
        {
            "way_inx": 1,
            "src_lat": 1.0,
            "src_lon": 1.0,
            "src_elevation": 1.0,
            "src_dead_end_flag": 0,
        },
        {
            "way_inx": 2,
            "src_lat": 1.1,
            "src_lon": 1.1,
            "src_elevation": 1.1,
            "src_dead_end_flag": 0,
        },
    ]
    dst_geom_2 = [
        {
            "way_inx": 1,
            "dst_lat": 1.1,
            "dst_lon": 1.1,
            "dst_elevation": 1.1,
            "dst_dead_end_flag": 0,
        },
        {
            "way_inx": 2,
            "dst_lat": 1.2,
            "dst_lon": 1.2,
            "dst_elevation": 1.2,
            "dst_dead_end_flag": 0,
        },
    ]

    # Complex fields - Three records
    geom_3 = [
        {"way_inx": 1, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12},
        {"way_inx": 2, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12},
        {"way_inx": 3, "distance": 1.0, "easting_ptn": 12, "northing_ptn": 12},
    ]
    src_geom_3 = [
        {
            "way_inx": 1,
            "src_lat": 1.0,
            "src_lon": 1.0,
            "src_elevation": 1.0,
            "src_dead_end_flag": 0,
        },
        {
            "way_inx": 2,
            "src_lat": 1.1,
            "src_lon": 1.1,
            "src_elevation": 1.1,
            "src_dead_end_flag": 0,
        },
        {
            "way_inx": 3,
            "src_lat": 1.2,
            "src_lon": 1.2,
            "src_elevation": 1.2,
            "src_dead_end_flag": 0,
        },
    ]
    dst_geom_3 = [
        {
            "way_inx": 1,
            "dst_lat": 1.1,
            "dst_lon": 1.1,
            "dst_elevation": 1.1,
            "dst_dead_end_flag": 0,
        },
        {
            "way_inx": 2,
            "dst_lat": 1.2,
            "dst_lon": 1.2,
            "dst_elevation": 1.2,
            "dst_dead_end_flag": 0,
        },
        {
            "way_inx": 3,
            "dst_lat": 1.3,
            "dst_lon": 1.3,
            "dst_elevation": 1.3,
            "dst_dead_end_flag": 0,
        },
    ]

    # fmt: off
    _ = (
        ['chain_src', 'chain_dst', 'highway'                 , 'surface'                 , 'elevation_gain', 'elevation_loss', 'distance', 'geom', 'src_geom', 'dst_geom'])

    test_data = [
        # Single record
        [0          , 1          , ['highway_1']             , ['surface_1']             , 1.0             , 1.0             , 1.0       , geom_1, src_geom_1, dst_geom_1],
        # Two records
        [1          , 2          , ['highway_1']             , ['surface_1']             , 2.0             , 2.0             , 2.0       , geom_2, src_geom_2, dst_geom_2],
        # Three records
        [2          , 3          , ['highway_1', 'highway_2'], ['surface_1', 'surface_2'], 3.0             , 3.0             , 3.0       , geom_3, src_geom_3, dst_geom_3],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("chain_src", IntegerType()),
            StructField("chain_dst", IntegerType()),
            StructField("highway", ArrayType(StringType(), False), False),
            StructField("surface", ArrayType(StringType(), False), False),
            StructField("elevation_gain", DoubleType()),
            StructField("elevation_loss", DoubleType()),
            StructField("distance", DoubleType()),
            StructField(
                "geom",
                ArrayType(
                    StructType(
                        [
                            StructField("way_inx", IntegerType()),
                            StructField("distance", DoubleType()),
                            StructField("easting_ptn", IntegerType()),
                            StructField("northing_ptn", IntegerType()),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "src_geom",
                ArrayType(
                    StructType(
                        [
                            StructField("way_inx", IntegerType()),
                            StructField("src_lat", DoubleType()),
                            StructField("src_lon", DoubleType()),
                            StructField("src_elevation", DoubleType()),
                            StructField("src_dead_end_flag", IntegerType()),
                        ]
                    ),
                    False,
                ),
                False,
            ),
            StructField(
                "dst_geom",
                ArrayType(
                    StructType(
                        [
                            StructField("way_inx", IntegerType()),
                            StructField("dst_lat", DoubleType()),
                            StructField("dst_lon", DoubleType()),
                            StructField("dst_elevation", DoubleType()),
                            StructField("dst_dead_end_flag", IntegerType()),
                        ]
                    ),
                    False,
                ),
                False,
            ),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'highway'     , 'surface'     , 'elevation_gain', 'elevation_loss', 'distance', 'geom_lat'          , 'geom_lon'          , 'geom_elevation'    , 'geom_distance'      , 'src_dead_end_flag', 'dst_dead_end_flag', 'src_lat', 'src_lon'])

    tgt_data = [
        # Single record
        [0    , 1    , 'highway_1'   , 'surface_1'   , 1.0             , 1.0             , 1.0       , [1.0, 1.1]          , [1.0, 1.1]          , [1.0, 1.1]          , [0.0, 1.0]           , 0                  , 0                  , 1.0       , 1.0],
        # Two records
        [1    , 2    , 'highway_1'   , 'surface_1'   , 2.0             , 2.0             , 2.0       , [1.0, 1.1, 1.2]     , [1.0, 1.1, 1.2]     , [1.0, 1.1, 1.2]     , [0.0, 1.0, 1.0]      , 0                  , 0                  , 1.0       , 1.0],
        # Three records
        [2    , 3    , 'unclassified', 'unclassified', 3.0             , 3.0             , 3.0       , [1.0, 1.1, 1.2, 1.3], [1.0, 1.1, 1.2, 1.3], [1.0, 1.1, 1.2, 1.3], [0.0, 1.0, 1.0, 1.0] , 0                  , 0                  , 1.0       , 1.0],
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
            StructField("elevation_gain", DoubleType()),
            StructField("elevation_loss", DoubleType()),
            StructField("distance", DoubleType()),
            StructField("geom_lat", ArrayType(DoubleType()), False),
            StructField("geom_lon", ArrayType(DoubleType()), False),
            StructField("geom_elevation", ArrayType(DoubleType()), False),
            StructField("geom_distance", ArrayType(DoubleType()), False),
            StructField("src_dead_end_flag", IntegerType()),
            StructField("dst_dead_end_flag", IntegerType()),
            StructField("src_lat", DoubleType()),
            StructField("src_lon", DoubleType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = GraphContractor.generate_new_edges_from_chains(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_drop_dead_ends(test_session: SparkSession):
    """Make sure that any dead ends are being removed cleanly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_dead_end_flag', 'dst_dead_end_flag'])

    test_data = [
        [0                  , 0],
        [0                  , 1],
        [1                  , 0],
        [1                  , 1]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("src_dead_end_flag", IntegerType()),
            StructField("dst_dead_end_flag", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_dead_end_flag', 'dst_dead_end_flag'])

    tgt_data = [
        [0                  , 0],
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("src_dead_end_flag", IntegerType()),
            StructField("dst_dead_end_flag", IntegerType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = GraphContractor.drop_dead_ends(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_set_edge_output_schema(test_session: SparkSession):
    """Make sure that the correct output schema is being set"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'highway', 'surface', 'elevation_gain', 'elevation_loss', 'distance', 'geom_lat'  , 'geom_lon'  , 'geom_elevation'  , 'geom_distance'  , 'src_lat', 'src_lon', 'other'])

    test_data = [
        ['src', 'dst', 'highway', 'surface', 'elevation_gain', 'elevation_loss', 'distance', ['geom_lat'], ['geom_lon'], ['geom_elevation'], ['geom_distance'], -25.5     , 75.0, 'other']
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
            StructField("elevation_gain", StringType()),
            StructField("elevation_loss", StringType()),
            StructField("distance", StringType()),
            StructField("geom_lat", ArrayType(StringType())),
            StructField("geom_lon", ArrayType(StringType())),
            StructField("geom_elevation", ArrayType(StringType())),
            StructField("geom_distance", ArrayType(StringType())),
            StructField("src_lat", DoubleType()),
            StructField("src_lon", DoubleType()),
            StructField("other", StringType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'src_lat', 'src_lon', 'highway', 'surface', 'elevation_gain', 'elevation_loss', 'distance', 'lats'      , 'lons'      , 'eles'            , 'dists'          , 'ptn'])

    tgt_data = [
        ['src', 'dst', -25.5    , 75.0     , 'highway', 'surface', 'elevation_gain', 'elevation_loss', 'distance', ['geom_lat'], ['geom_lon'], ['geom_elevation'], ['geom_distance'],  'n25_75']
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("src_lat", DoubleType()),
            StructField("src_lon", DoubleType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
            StructField("elevation_gain", StringType()),
            StructField("elevation_loss", StringType()),
            StructField("distance", StringType()),
            StructField("lats", ArrayType(StringType())),
            StructField("lons", ArrayType(StringType())),
            StructField("eles", ArrayType(StringType())),
            StructField("dists", ArrayType(StringType())),
            StructField("ptn", StringType(), False),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = GraphContractor.set_edge_output_schema(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_drop_unused_nodes(test_session: SparkSession):
    """Make sure that any nodes without a corresponding edge (i.e. those which
    were along the middle of a chain) are being removed"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # ----- Nodes -----
    # fmt: off
    _ = (
        ['id'])

    test_node_data = [
        # Source and dest
        [0],
        # Source only
        [1],
        # Dest only
        [2],
        # Neither
        [3]
    ]
    # fmt: on

    test_node_schema = StructType(
        [
            StructField("id", IntegerType()),
        ]
    )

    test_node_df = test_session.createDataFrame(
        test_node_data, test_node_schema
    )

    # ----- Edges -----
    # fmt: off
    _ = (
        ['src', 'dst'])

    test_edge_data = [
        # Source and dest
        [0, 2],
        [1, 0],
        # Source only
        [1, 2],
        # Dest only
        [4, 2],
        # Neither
    ]
    # fmt: on

    test_edge_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
        ]
    )

    test_edge_df = test_session.createDataFrame(
        test_edge_data, test_edge_schema
    )

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['id'])

    tgt_data = [
        # Source and dest
        [0],
        # Source only
        [1],
        # Dest only
        [2],
        # Neither
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("id", IntegerType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = GraphContractor.drop_unused_nodes(test_node_df, test_edge_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_set_node_output_schema(test_session: SparkSession):
    """Make sure that the output schema for the nodes dataset is being set
    properly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ["id", "lat", "lon", "elevation", "other"])

    test_data = [
        ["id", -25.5, 75.0 , "elevation", "other"],
        [None, 0.0  , 0.0  , "elevation", "other"],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("id", StringType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
            StructField("elevation", StringType()),
            StructField("other", StringType()),
        ],
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ["id", "lat", "lon", "elevation", "ptn"])

    tgt_data = [
        ["id", -25.5, 75.0 , "elevation", "n25_75"],
    ]

    # fmt: on

    tgt_schema = StructType(
        [
            StructField("id", StringType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
            StructField("elevation", StringType()),
            StructField("ptn", StringType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = GraphContractor.set_node_output_schema(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_store_df():
    """Make sure that the correct calls are being generated when storing data
    back to disk"""

    # Arrange
    test_contractor = GraphContractor("data_dir", "spark")  # type: ignore

    test_df = MagicMock()
    test_df.write.mode.return_value = test_df
    test_df.csv.return_value = test_df

    test_target = "target"

    target_path = "data_dir/optimised/target"

    # Act
    test_contractor.store_df(test_df, test_target)

    # Assert
    test_df.write.mode.assert_called_once_with("overwrite")
    test_df.csv.assert_called_once_with(
        target_path, compression=None, sep="\t", encoding="utf8", header=False
    )


def test_contract():
    """Make sure that the end-to-end process is generating the correct
    method calls"""

    # Arrange #################################################################

    # Creation, partition discovery
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = "42"
    test_contractor = GraphContractor("data_dir", mock_spark)

    test_contractor.get_available_partitions = MagicMock(
        return_value=list(range(56))
    )

    # Method calls
    methods = inspect.getmembers(GraphContractor, predicate=inspect.isroutine)

    method_names = [name for name, _ in methods if name[0] != "_"]

    method_names.remove("contract")
    method_names.remove("get_available_partitions")

    for method_name in method_names:
        setattr(test_contractor, method_name, MagicMock())

    # Act #####################################################################
    test_contractor.contract()

    # Assert ##################################################################

    # Shuffle partitions get set then un-set
    mock_spark.conf.set.assert_has_calls(
        [
            call("spark.sql.shuffle.partitions", "56"),
            call("spark.sql.shuffle.partitions", "42"),
        ]
    )

    # All defined methods should be called
    for method_name in method_names:
        method_mock = getattr(test_contractor, method_name)
        method_mock.assert_called()
