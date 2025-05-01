"""Containers which represent user/app configuration variables"""

import os
from dataclasses import dataclass, field
from typing import List, Literal


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

    route_mode: Literal["hilly", "flat"]
    max_candidates: int

    restricted_surfaces_perc: float
    restricted_surfaces: List[str] = field(default_factory=list)

    highway_types: List[str] = field(default_factory=list)
    surface_types: List[str] = field(default_factory=list)

    # TODO: Hook this up to a user input (or part of an admin panel?)
    distance_tolerance: float = float(
        os.environ.get("FF_DIST_TOLERANCE", "0.1")
    )

    def __post_init__(self) -> None:
        """Calculate min_distance and max_distance based on user provided
        target_distance and tolerance"""

        # Ensure inputs are stored using the expected datatype
        for attr in [
            "start_lat",
            "start_lon",
            "target_distance",
            "restricted_surfaces_perc",
        ]:
            setattr(self, attr, float(getattr(self, attr)))

        for attr in ["max_candidates"]:
            setattr(self, attr, int(getattr(self, attr)))
