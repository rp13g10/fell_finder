"""Contains the classes which represents routes which are either in progress,
or have been completed"""

import json
import uuid
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import List, Tuple, Set, Self, Dict
import gpxpy
import gpxpy.gpx

from fell_finder import app_config


@dataclass(slots=True)
class StepData:
    """Container for metrics calculated when stepping from the end of one
    route to a neighbouring node."""

    next_node: int

    distance: float
    elevation_gain: float
    elevation_loss: float
    surface: str

    lats: List[float] = field(default_factory=list)
    lons: List[float] = field(default_factory=list)
    distances: List[float] = field(default_factory=list)
    elevations: List[float] = field(default_factory=list)


@dataclass(slots=True)
class RouteGeometry:
    """Class representing the physical geometry of a route"""

    lats: List[float] = field(default_factory=list)
    lons: List[float] = field(default_factory=list)
    distances: List[float] = field(default_factory=list)
    elevations: List[float] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Export the contents of this class to a dict"""
        return {
            "lats": self.lats,
            "lons": self.lons,
            "distances": self.distances,
            "elevations": self.elevations,
        }

    def to_gpx(self) -> str:
        """Export the geometry of the route to a GPX file, presented as a
        string for easier use with the Dash frontend"""

        gpx = gpxpy.gpx.GPX()
        route = gpxpy.gpx.GPXRoute()
        route.type = "run"
        gpx.routes.append(route)

        for lat, lon, elevation in zip(self.lats, self.lons, self.elevations):
            point = gpxpy.gpx.GPXRoutePoint(
                latitude=lat, longitude=lon, elevation=elevation
            )
            route.points.append(point)

        gpx_xml = gpx.to_xml()

        return gpx_xml

    @property
    def current_distance(self) -> float:
        """Return the current distance travelled from the start point"""

        if self.distances:
            return self.distances[-1]
        return 0.0

    @property
    def max_lat(self) -> float:
        """The max latitude visited at any point in the route"""
        return max(self.lats)

    @property
    def max_lon(self) -> float:
        """The max longitude visited at any point in the route"""
        return max(self.lons)

    @property
    def min_lat(self) -> float:
        """The min latitude visited at any point in the route"""
        return min(self.lats)

    @property
    def min_lon(self) -> float:
        """The max longitude visited at any point in the route"""
        return min(self.lons)

    @property
    def centre(self) -> Tuple[float, float]:
        """Returns the coordinates representing the centre of the route"""
        mid_lat = self.max_lat - ((self.max_lat - self.min_lat) / 2)
        mid_lon = self.max_lon - ((self.max_lon - self.min_lon) / 2)

        return mid_lat, mid_lon

    @property
    def bounds(self) -> Tuple[Tuple[float, float], Tuple[float, float]]:
        """Returns the pair of coordinates representing the bounds of the
        area covered by the route"""
        return ((self.min_lat, self.max_lon), (self.max_lat, self.min_lon))

    @property
    def coords(self) -> List[Tuple[float, float]]:
        """Returns a list of all lat/lon pairs for the route"""
        return [(lat, lon) for lat, lon in zip(self.lats, self.lons)]


@dataclass(slots=True)
class RouteMetrics:
    """Class representing the overall route metrics"""

    distance: float = 0.0
    elevation_gain: float = 0.0
    elevation_loss: float = 0.0

    surface_distances: Dict[str, float] = field(
        default_factory=lambda: defaultdict(lambda: 0.0)
    )

    def __post_init__(self) -> None:
        """If a standard dict is provided instead of a defaultdict, convert
        it to ensure that additional steps can still be taken"""
        if not isinstance(self.surface_distances, defaultdict):
            self.surface_distances = defaultdict(
                lambda: 0.0, self.surface_distances
            )

    def to_dict(self) -> Dict:
        """Export the contents of this class to a dict"""
        return {
            "distance": self.distance,
            "elevation_gain": self.elevation_gain,
            "elevation_loss": self.elevation_loss,
            "surface_distances": self.surface_distances,
        }

    @property
    def elevation_gain_potential(self) -> float:
        """Work out the gain required to get back to the start"""
        if self.elevation_loss > self.elevation_gain:
            return self.elevation_loss - self.elevation_gain
        return 0.0

    @property
    def elevation_loss_potential(self) -> float:
        """Work out the loss required to get back to the start"""
        if self.elevation_gain > self.elevation_loss:
            return self.elevation_gain - self.elevation_loss
        return 0.0

    @property
    def ratio(self) -> float:
        """Calculate the ratio of elevation gain to distance travelled"""
        return (
            self.elevation_gain + self.elevation_gain_potential
        ) / self.distance


class Route:
    """Represents a single route while it is being generated"""

    __slots__ = ["points", "visited", "route_id", "geometry", "metrics"]

    def __init__(self, start_node: int) -> None:
        self.points = [start_node]
        self.visited = {start_node}

        self.geometry = RouteGeometry()
        self.metrics = RouteMetrics()

        self.generate_new_id()

    @staticmethod
    def from_dict(route_dict: Dict) -> "Route":
        """Enable the creation of a route from a dictionary"""

        route = Route(0)  # Dummy start node

        route.points = route_dict["points"]
        route.visited = set(route_dict["visited"])
        route.route_id = route_dict["route_id"]
        route.geometry = RouteGeometry(**route_dict["geometry"])
        route.metrics = RouteMetrics(**route_dict["metrics"])

        return route

    @staticmethod
    def from_str(route_str: str) -> "Route":
        """Enable the creation of a route from a string"""

        route_dict = json.loads(route_str)

        return Route.from_dict(route_dict)

    def to_dict(self) -> Dict:
        """Enable the conversion of a route to a dictionary for portability"""
        route_dict = {
            "points": self.points,
            "visited": list(self.visited),
            "route_id": self.route_id,
            "geometry": self.geometry.to_dict(),
            "metrics": self.metrics.to_dict(),
        }
        return route_dict

    def to_str(self) -> str:
        """Enable the conversion of a route to a string for portability"""
        route_dict = self.to_dict()
        route_str = json.dumps(route_dict)
        return route_str

    def generate_new_id(self) -> None:
        """Generate a new unique identifier for the route"""
        self.route_id = uuid.uuid4().hex

    @property
    def start_node(self) -> int:
        """Get the ID of the first node in the route"""
        return self.points[0]

    @property
    def cur_node(self) -> int:
        """Get the ID of the current node in the route"""
        return self.points[-1]

    @property
    def is_closed_loop(self) -> bool:
        """Check whether or not the current route forms a closed loop"""
        if len(self.points) == 1:
            # Prevent instant flagging as closed loop
            return False
        return self.start_node == self.cur_node

    @property
    def overlap_n_nodes(self) -> int:
        """Get the number of nodes which can be visited more than once, this
        is configured via the route_overlap_threshold variable in the config"""
        overlap = app_config["routing"]["route_overlap_threshold"]
        n_nodes = int(len(self.points) * overlap)
        n_nodes = max(n_nodes, 3)
        return n_nodes

    @property
    def first_n_nodes(self) -> Set[int]:
        """Return the first few nodes in the route, the number of nodes
        returned will vary with the configured overlap threshold"""

        first_n = set(self.points[: self.overlap_n_nodes])
        return first_n

    @property
    def last_3_nodes(self) -> Set[int]:
        """Return the last 3 nodes in the route, useful when checking to make
        sure an infinite loop isn't trying to form"""

        last_3 = set(self.points[-3:])
        return last_3

    def take_step(self, step: StepData) -> Self:
        """Return a new route which is based on the provided StepData. Note
        that this will not modify the current route.

        Args:
            step: The details of the step to be taken

        Returns:
            A new route containing the current route plus the additional step
        """
        new_route = deepcopy(self)
        new_route.generate_new_id()

        new_route.points.append(step.next_node)
        new_route.visited.add(step.next_node)

        new_route.metrics.distance += step.distance
        new_route.metrics.elevation_gain += step.elevation_gain
        new_route.metrics.elevation_loss += step.elevation_loss
        new_route.metrics.surface_distances[step.surface] += step.distance

        new_route.geometry.lats += step.lats
        new_route.geometry.lons += step.lons

        for distance in step.distances:
            current_distance = new_route.geometry.current_distance
            new_route.geometry.distances.append(distance + current_distance)
        new_route.geometry.elevations += step.elevations

        return new_route

    def finalize(self) -> None:
        """Wipe out any data which is no longer required after route
        completion"""
        self.points = []
        self.visited = set()
