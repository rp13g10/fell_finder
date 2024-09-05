"""Containers for the data to be stored against graph nodes/edges"""

from typing import Any, Dict


class GraphNode:
    """Container for additional node attributes which are not supported
    natively by rustworkx"""

    def __init__(self, node_dict: Dict[str, Any]) -> None:
        self.index = None

        self.lat = node_dict["lat"]
        self.lon = node_dict["lon"]
        self.elevation = node_dict["elevation"]

        self.dist_to_start = None

    def set_index(self, index: int) -> None:
        """Set the node index to the provided value"""
        self.index = index

    def set_dist_to_start(self, dist: float) -> None:
        """Set the distance from the node to the start point"""
        self.dist_to_start = dist


class GraphEdge:
    """Container for additional edge attributes which are not supported
    natively by rustworkx"""

    def __init__(self, edge_dict: Dict[str, Any]) -> None:
        self.index = None

        self.highway = edge_dict["highway"]
        self.surface = edge_dict["surface"]
        self.distance = edge_dict["distance"]
        self.elevation_gain = edge_dict["elevation_gain"]
        self.elevation_loss = edge_dict["elevation_loss"]
        self.geometry = edge_dict["geometry"]

    def set_index(self, index: int) -> None:
        """Set the edge index to the provided value"""
        self.index = index
