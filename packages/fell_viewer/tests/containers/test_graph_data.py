"""Tests for the graph data classes"""

from fell_viewer.containers.graph_data import GraphEdge, GraphNode


class TestGraphNode:
    """Make sure node data is being stored properly"""

    # Arrange
    test_node_dict = {x: x for x in ["lat", "lon", "elevation"]}

    def test_init(self):
        """Make sure that values are mapping to the correct attributes"""

        # Arrange
        target_props = ["lat", "lon", "elevation"]

        # Act
        result = GraphNode(self.test_node_dict)

        # Assert
        for prop in target_props:
            assert getattr(result, prop) == prop

        assert result.index is None
        assert result.dist_to_start is None

    def test_attr_setters(self):
        """Make sure values are correctly updated when required"""

        # Arrange
        test_index = "index"
        test_dist_to_start = "dist_to_start"

        # Act
        result = GraphNode(self.test_node_dict)
        result.set_index(test_index)  # type: ignore
        result.set_dist_to_start(test_dist_to_start)  # type: ignore

        # Assert
        assert result.index == "index"
        assert result.dist_to_start == "dist_to_start"


class TestGraphEdge:
    """Make sure edge data is being stored properly"""

    # Arrange
    test_edge_dict = {
        x: x
        for x in [
            "highway",
            "surface",
            "distance",
            "elevation_gain",
            "elevation_loss",
            "geometry",
        ]
    }

    def test_init(self):
        """Make sure that values are mapping to the correct attributes"""

        # Arrange
        target_props = [
            "highway",
            "surface",
            "distance",
            "elevation_gain",
            "elevation_loss",
            "geometry",
        ]

        # Act
        result = GraphEdge(self.test_edge_dict)

        # Assert
        for prop in target_props:
            assert getattr(result, prop) == prop

        assert result.index is None

    def test_attr_setters(self):
        """Make sure values are correctly updated when required"""

        # Arrange
        test_index = "index"

        # Act
        result = GraphEdge(self.test_edge_dict)
        result.set_index(test_index)  # type: ignore

        # Assert
        assert result.index == "index"
