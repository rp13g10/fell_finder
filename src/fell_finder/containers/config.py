"""Containers which represent user/app configuration variables"""

from dataclasses import dataclass, field
from typing import List

from fell_finder import app_config


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

    route_mode: str
    max_candidates: int

    restricted_surfaces_perc: float
    restricted_surfaces: List[str] = field(default_factory=list)

    highway_types: List[str] = field(default_factory=list)
    surface_types: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Calculate min_distance and max_distance based on user provided
        target_distance and tolerance"""

        self.min_distance = self.target_distance / (
            1 + app_config["routing"]["distance_tolerance"]
        )
        self.max_distance = self.target_distance * (
            1 + app_config["routing"]["distance_tolerance"]
        )
        self.restricted_surfaces_perc = float(self.restricted_surfaces_perc)
