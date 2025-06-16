"""Defines the various dataclasses which are used to send and receive
information through this package."""

import os
from dataclasses import dataclass, field
from typing import List, Literal
from typing import Any
import dash_leaflet as dl

import gpxpy
import gpxpy.gpx

# TODO: Look into using pydantic, provides serialization support


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


@dataclass
class BBox:
    """Defines the corners of the rectangle containing a single route"""

    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

    def to_dict(self) -> dict[str, float]:
        """Export the contents of this class to a dict"""
        return {
            "min_lat": self.min_lat,
            "min_lon": self.min_lon,
            "max_lat": self.max_lat,
            "max_lon": self.max_lon,
        }

    @staticmethod
    def from_dict(contents: dict[str, float]) -> "BBox":
        """Convert back from a dict to a dataclass"""
        return BBox(
            min_lat=contents["min_lat"],
            min_lon=contents["min_lon"],
            max_lat=contents["max_lat"],
            max_lon=contents["max_lon"],
        )

    @property
    def centre(self) -> tuple[float, float]:
        """Returns the coordinates representing the centre of the route"""
        mid_lat = self.max_lat - ((self.max_lat - self.min_lat) / 2)
        mid_lon = self.max_lon - ((self.max_lon - self.min_lon) / 2)

        return mid_lat, mid_lon

    @property
    def bounds(self) -> tuple[tuple[float, float], tuple[float, float]]:
        """Return the bounds of a route in a format which can be used by
        plotly maps"""

        return ((self.min_lat, self.max_lon), (self.max_lat, self.min_lon))

    def to_viewport(self) -> dict[str, Any]:
        """Returns a dict which can be used to define the viewport of a plotly
        map plot. Contains the 'bounds' and 'center' keys."""

        bounds = {
            "bounds": self.bounds,
            "center": self.centre,
        }

        return bounds


@dataclass
class RouteGeometry:
    """Contains all information about the physical geometry of a route"""

    lats: list[float]
    lons: list[float]
    dists: list[float]
    eles: list[float]
    bbox: BBox

    @property
    def coords(self) -> list[tuple[float, float]]:
        """Returns a list of all lat/lon pairs for the route"""
        return [(lat, lon) for lat, lon in zip(self.lats, self.lons)]

    def to_dict(self) -> dict[str, Any]:
        """Export the contents of this class to a dict"""
        return {
            "lats": self.lats,
            "lons": self.lons,
            "dists": self.dists,
            "eles": self.eles,
            "bbox": self.bbox.to_dict(),
        }

    @staticmethod
    def from_dict(
        content: dict[str, Any], coords: bool = False
    ) -> "RouteGeometry":
        """Convert back from a dict to a dataclass"""

        if coords:
            coords_data = content["coords"]
            lats, lons = [], []
            for lat, lon in coords_data:
                lats.append(lat)
                lons.append(lon)
        else:
            lats = content["lats"]
            lons = content["lons"]

        return RouteGeometry(
            lats=lats,
            lons=lons,
            dists=content["dists"],
            eles=content["eles"],
            bbox=BBox.from_dict(content["bbox"]),
        )

    def to_gpx(self) -> str:
        """Export the geometry of the route to a GPX file, presented as a
        string for easier use with the Dash frontend"""

        gpx = gpxpy.gpx.GPX()
        route = gpxpy.gpx.GPXRoute()
        route.type = "run"
        gpx.routes.append(route)

        for lat, lon, elevation in zip(self.lats, self.lons, self.eles):
            point = gpxpy.gpx.GPXRoutePoint(
                latitude=lat, longitude=lon, elevation=elevation
            )
            route.points.append(point)

        gpx_xml = gpx.to_xml()

        return gpx_xml


@dataclass
class RouteMetrics:
    """Contains all of the metrics for a single route"""

    dist: float
    gain: float
    loss: float
    s_dists: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        """Export the contents of this class to a dict"""
        return {
            "dist": self.dist,
            "gain": self.gain,
            "loss": self.loss,
            "s_dists": self.s_dists,
        }

    @staticmethod
    def from_dict(content: dict[str, Any]) -> "RouteMetrics":
        """Convert back from a dict to a dataclass"""
        return RouteMetrics(
            dist=content["dist"],
            gain=content["gain"],
            loss=content["loss"],
            s_dists=content["s_dists"],
        )


@dataclass
class Route:
    """Contains the details of a single, completed route"""

    geometry: RouteGeometry
    metrics: RouteMetrics
    route_id: int

    @staticmethod
    def from_api_response(api_response: dict[str, Any]) -> "Route":
        """Enable the creation of a route from an API response"""

        geometry_dict = api_response["geometry"]
        geometry = RouteGeometry.from_dict(geometry_dict, coords=True)

        metrics_dict = api_response["metrics"]["common"]
        metrics = RouteMetrics.from_dict(metrics_dict)

        route = Route(
            geometry=geometry, metrics=metrics, route_id=api_response["id"]
        )

        return route

    def to_dict(self) -> dict[str, Any]:
        """Enable the conversion of a route to a dictionary for portability"""
        route_dict = {
            "route_id": self.route_id,
            "geometry": self.geometry.to_dict(),
            "metrics": self.metrics.to_dict(),
        }
        return route_dict

    @staticmethod
    def from_dict(content: dict[str, Any]) -> "Route":
        """Convert back from a dict to a dataclass"""
        return Route(
            route_id=content["route_id"],
            geometry=RouteGeometry.from_dict(content["geometry"]),
            metrics=RouteMetrics.from_dict(content["metrics"]),
        )

    def to_polyline(self, id_: str) -> dl.Polyline:
        """For a generated route, generate a dash-leaflet polyline which can be
        displayed on a map

        Args:
            id_: The id which should be set for the generated polyline

        Returns:
            A polyline which can be added to a dash_leaflet map

        """

        polyline = dl.Polyline(
            positions=self.geometry.coords,
            id=id_,
            interactive=True,
        )

        return polyline
