"""Tests for the user-facing graph enricher class"""

from unittest.mock import MagicMock

import pytest

from fell_loader.enriching.graph_enricher import GraphEnricher


class MockGraphEnricher(GraphEnricher):
    """Mock implementation of GraphEnricher with static params"""

    def __init__(self) -> None:
        self.data_dir = "data_dir"
        self.edge_resolution_m = 10
        self.spark: MagicMock = MagicMock()


def test_load_df():
    """Make sure data is being read in and repartitioned correctly"""

    # Arrange #################################################################

    test_enricher = MockGraphEnricher()

    test_dataset = "nodes"

    test_df = MagicMock()
    test_enricher.spark.read.parquet.return_value = test_df

    target_read_call = "data_dir/parsed/nodes"

    # Act #####################################################################
    result = test_enricher.load_df(test_dataset)

    # Assert ##################################################################
    test_enricher.spark.read.parquet.assert_called_once_with(target_read_call)
    assert result is test_enricher.spark.read.parquet.return_value


class TestStoreDf:
    """Make sure data is being stored to disk correctly"""

    def test_with_partitions(self):
        """Make sure data is being stored to disk correctly"""

        # Arrange #################################################################

        test_enricher = MockGraphEnricher()

        test_df = MagicMock()
        test_target = "nodes"
        test_ptns = True

        target_dir = "data_dir/enriched/nodes"
        target_kwargs = {"compression": "snappy", "partitionBy": "ptn"}

        # Act #####################################################################

        test_enricher.store_df(test_df, test_target, test_ptns)

        # Assert ##################################################################
        test_df.write.mode.return_value.parquet.assert_called_once_with(
            target_dir, **target_kwargs
        )

    def test_without_partitions(self):
        """Make sure data is being stored to disk correctly"""

        # Arrange #################################################################

        test_enricher = MockGraphEnricher()

        test_df = MagicMock()
        test_target = "nodes"
        test_ptns = False

        target_dir = "data_dir/enriched/nodes"
        target_kwargs = {"compression": "snappy"}

        # Act #####################################################################

        test_enricher.store_df(test_df, test_target, test_ptns)

        # Assert ##################################################################
        test_df.write.mode.return_value.parquet.assert_called_once_with(
            target_dir, **target_kwargs
        )


@pytest.mark.skip
def test_enrich():
    """Test needs building out to show E2E execution"""
