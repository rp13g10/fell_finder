"""Container objects which are used to represent various structures used
during the route finding process"""

from dataclasses import dataclass, field
from typing import List, Set


@dataclass
class Route:
    """Container for the information required to represent a single route

    Args:
        route: A list of the node IDs which are crossed as part of
          this route, in the order that they are crossed
        visited: A set of all the unique nodes which are visited as part of
          this route
        distance: The total distance of the route
        elevation_gain: The elevation gain for this route
        elevation_loss: The elevation loss for this route
        elevation_gain_potential: The elevation gain required in order
          to get back to the route's starting point
        elevation_loss_potential: The elevation loss required in order
          to get back to the route's starting point"""

    route: List[int]
    route_id: str

    current_position: int
    visited: Set[int]

    distance: float = 0.0
    elevation_gain: float = 0.0
    elevation_loss: float = 0.0
    elevation_gain_potential: float = 0.0
    elevation_loss_potential: float = 0.0

    @property
    def ratio(self) -> float:
        """Calculate the ratio of elevation gain to distance travelled"""
        return self.elevation_gain / self.distance


@dataclass
class RouteConfig:
    """Contains user configuration options for route calculation

    Args:
        start_lat: Latitude for the route start point
        start_lon: Longitude for the route start point
        target_distance: Target distance for the route (in m)
        tolerance: How far above/below the target distance a completed route
          can be while still being considered valid
        route_mode: Set to 'hilly' to generate the hilliest possible
          route, or 'flat' for the flattest possible route
        max_candidates: The maximum number of candidate routes which
          should be held in memory. Lower this to increase calculation speed,
          increase it to potentially increase the quality of routes generated.
        highway_types: The different types of terrain which should
          be considered for this route. If not provided, all terrain types
          will be considered. Defaults to None.
        surface_types: The different  types of surface which should be
          considered for this route. If not provided, all surface types will be
          considered. Defaults to None.
    """

    start_lat: float
    start_lon: float

    target_distance: float
    tolerance: float

    route_mode: str
    max_candidates: int

    highway_types: List[str] = field(default_factory=list)
    surface_types: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Calculate min_distance and max_distance based on user provided
        target_distance and tolerance"""

        self.min_distance = self.target_distance / (1 + self.tolerance)
        self.max_distance = self.target_distance * (1 + self.tolerance)
