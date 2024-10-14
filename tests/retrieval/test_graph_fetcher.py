"""Tests for the GraphFetcher class"""

import os
from typing import Dict, Any
from unittest.mock import patch, MagicMock
from pytest import approx
from fell_finder.containers.config import RouteConfig
from fell_finder.containers.geometry import BBox
from fell_finder.retrieval.graph_fetcher import GraphFetcher

# Common config for all test cases
CONFIG_ARGS: Dict[str, Any] = dict(
    # Coords for Buckingham Palace
    start_lat=51.501080,
    start_lon=-0.142339,
    target_distance=10000.0,
    route_mode="hilly",
    max_candidates=42,
)


class TestGraphFetcher:
    """Make sure all functionality is working correctly"""

    def test_get_bounding_box_for_route(self):
        """Make sure bounding boxes are created properly"""

        # Arrange
        test_config = RouteConfig(
            **CONFIG_ARGS,
            restricted_surfaces_perc=0.0,
            restricted_surfaces=[],
        )
        test_data_dir = "data_dir"

        target_attrs = dict(
            # Observed to be ~5 miles south-west of the palace
            min_lat=51.4516183713,
            min_lon=-0.2216311269,
            # Observed to be ~5 miles north-east of the palace
            max_lat=51.5504877260,
            max_lon=-0.0632182602,
        )

        # Act
        graph_fetcher = GraphFetcher(test_config, test_data_dir)

        # Assert
        for attr, target in target_attrs.items():
            result = getattr(graph_fetcher.bbox, attr)
            assert result == approx(target)

    def test_retrieve_nodes_for_bounding_box(self):
        """Make sure some data is retrieved, but filters are in place"""
        # Arrange
        test_config = RouteConfig(
            # Coords for Buckingham Palace
            **CONFIG_ARGS,
            restricted_surfaces_perc=0.0,
            restricted_surfaces=[],
        )
        test_data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data/"
        )
        test_graph_fetcher = GraphFetcher(test_config, test_data_dir)

        # Act
        res_nodes_list, res_all_nodes = (
            test_graph_fetcher.retrieve_nodes_for_bounding_box()
        )

        # Assert
        assert 0 < len(res_nodes_list) < 651  # Returns some, but not all data

    def test_get_id_index_mappings(self):
        """Make sure id/inx mappings are being created properly"""

        # Arrange
        test_all_nodes = list("abcdefghij")
        test_indices = list(range(1, 11))

        target = {
            "a": 1,
            "b": 2,
            "c": 3,
            "d": 4,
            "e": 5,
            "f": 6,
            "g": 7,
            "h": 8,
            "i": 9,
            "j": 10,
        }

        # Act
        result = GraphFetcher.get_id_index_mappings(
            test_all_nodes,  # type: ignore
            test_indices,
        )

        # Assert
        assert result == target

    def test_retrieve_edges_for_bounding_box(self):
        raise AssertionError()

    def test_initialize_graph(self):
        raise AssertionError()

    def test_fetch_coarse_subgraph(self):
        raise AssertionError()

    def test_find_nearest_node(self):
        raise AssertionError()

    def test_inverse_inx_to_node_inx(self):
        raise AssertionError()

    def test_tag_distances_to_start(self):
        raise AssertionError()

    def test_generate_fine_subgraph(self):
        raise AssertionError()

    def test_create_graph(self):
        raise AssertionError()
