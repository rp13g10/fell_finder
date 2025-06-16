"""Ensure data passed from the webapp to the backend is being stored
properly"""

import os
from typing import Any

import pytest
from pytest import approx

from fell_viewer.common.containers import (
    RouteConfig,
    BBox,
    Route,
    RouteGeometry,
    RouteMetrics,
)


def test_route_config():
    """Ensure derived properties are set properly and appropriate datatypes
    are being enforced"""

    # Arrange
    test_kwargs = dict(
        start_lat="50.0",
        start_lon="1.0",
        target_distance="10.0",
        route_mode="hilly",
        max_candidates="64",
        restricted_surfaces_perc="0.1",
        restricted_surfaces=["restricted_surface"],
        highway_types=["highway"],
        surface_types=["surface", "restricted_surface"],
    )

    os.environ["FF_DIST_TOLERANCE"] = "0.1"

    target_attrs = dict(
        start_lat=50.0,
        start_lon=1.0,
        target_distance=10.0,
        route_mode="hilly",
        max_candidates=64,
        restricted_surfaces_perc=0.1,
        restricted_surfaces=["restricted_surface"],
        highway_types=["highway"],
        surface_types=["surface", "restricted_surface"],
    )

    # Act
    result = RouteConfig(**test_kwargs)  # type: ignore

    # Assert
    for attr, target in target_attrs.items():
        assert getattr(result, attr) == target


class TestBBox:
    """Check behaviour of the BBox dataclass"""

    def test_from_dict(self):
        """Check that reads from dict are set up properly"""

        # Arrange
        test_dict = dict(min_lat=0.1, min_lon=0.2, max_lat=0.3, max_lon=0.4)
        target = BBox(min_lat=0.1, min_lon=0.2, max_lat=0.3, max_lon=0.4)

        # Act
        result = BBox.from_dict(test_dict)

        # Assert
        assert result == target

    def test_to_dict(self):
        """Check that conversion to dict is set up properly"""

        # Arrange
        test_bbox = BBox(min_lat=0.1, min_lon=0.2, max_lat=0.3, max_lon=0.4)
        target = dict(min_lat=0.1, min_lon=0.2, max_lat=0.3, max_lon=0.4)

        # Act
        result = test_bbox.to_dict()

        # Assert
        assert result == target

    def test_centre(self):
        """Check that the centre point is being calculated correctly"""

        # Arrange
        test_bbox = BBox(min_lat=0.1, min_lon=0.2, max_lat=0.3, max_lon=0.4)
        target = (0.2, 0.3)

        # Act
        result = test_bbox.centre

        # Assert
        assert approx(result) == target

    def test_bounds(self):
        """Check that the bounds are being set correctly"""

        # Arrange
        test_bbox = BBox(min_lat=0.1, min_lon=0.2, max_lat=0.3, max_lon=0.4)
        target = ((0.1, 0.4), (0.3, 0.2))

        # Act
        result = test_bbox.bounds

        # Assert
        assert result == target

    def test_to_viewport(self):
        """Check that viewport data is generated properly"""

        # Arrange
        test_bbox = BBox(min_lat=0.1, min_lon=0.2, max_lat=0.3, max_lon=0.4)
        target = {"bounds": ((0.1, 0.4), (0.3, 0.2)), "center": (0.2, 0.3)}

        # Act
        result = test_bbox.to_viewport()

        # Assert
        assert len(result) == 2
        assert result["bounds"] == target["bounds"]
        assert approx(result["center"]) == target["center"]


class TestRouteGeometry:
    """Make sure route geometry is being evaluated and stored correctly"""

    test_props: dict[str, Any] = {
        "lats": [-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
        "lons": [
            50.0,
            51.0,
            52.0,
            53.0,
            54.0,
            55.0,
            56.0,
            57.0,
            58.0,
            59.0,
            60.0,
        ],
        "dists": [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        "eles": [2.0, 1.0, 2.0, 3.0, 2.0, 1.0, 5.0, 4.0, 3.0, 1.0, 6.0],
        "bbox": dict(min_lat=-5.0, max_lat=5.0, min_lon=50.0, max_lon=60.0),
    }

    def test_to_dict(self):
        """Check that conversion to/from dict is working properly"""
        # Arrange
        test_geo = RouteGeometry.from_dict(self.test_props)

        # Act
        result = test_geo.to_dict()

        # Assert
        assert isinstance(test_geo, RouteGeometry)
        assert isinstance(test_geo.bbox, BBox)
        assert result == self.test_props

    @pytest.mark.skip()
    def to_gpx():
        """Test that GPX routes are being generated properly."""

        # TODO: This needs building out. Exports confirmed working as I've been
        #       able to import them into Garmin.


class TestRouteMetrics:
    """Make sure route metrics are being evaluated and stored correctly"""

    test_props = {
        "dist": 5.0,
        "gain": 25.0,
        "loss": 10.0,
        "s_dists": {"surface_1", 1.0},
    }

    def test_to_dict(self):
        """to_dict should return the input data"""
        # Arrange
        test_mets = RouteMetrics.from_dict(self.test_props)

        # Act
        result = test_mets.to_dict()

        # Assert
        assert isinstance(test_mets, RouteMetrics)
        assert result == self.test_props


class TestRoute:
    """Make sure the primary route class has been set up properly"""

    def test_to_dict(self):
        """Check that dict conversion is working properly"""
        # Arrange

        test_dict = {
            "route_id": "route_id",
            "geometry": {
                "lats": [-5.0, 0.0, 5.0],
                "lons": [45.0, 50.0, 55.0],
                "dists": [0.0, 5.0, 10.0],
                "eles": [100.0, 150.0, 200.0],
                "bbox": dict(
                    min_lat=-5.0, max_lat=5.0, min_lon=50.0, max_lon=60.0
                ),
            },
            "metrics": {
                "dist": 1.0,
                "loss": 2.0,
                "gain": 3.0,
                "s_dists": {"surface_1": 1.0},
            },
        }

        # Act
        result_route = Route.from_dict(test_dict)
        result_dict = result_route.to_dict()

        # Assert
        assert isinstance(result_route, Route)
        assert isinstance(result_route.geometry, RouteGeometry)
        assert isinstance(result_route.metrics, RouteMetrics)
        assert isinstance(result_route.geometry.bbox, BBox)
        assert result_route.route_id == "route_id"
        assert result_dict == test_dict

    def test_from_api_response(self):
        """Check that creation from API response is working"""

        # Arrange
        test_response = {
            "id": "route_id",
            "geometry": {
                "coords": [(-5.0, 45.0), (0.0, 50.0), (5.0, 55.0)],
                "dists": [0.0, 5.0, 10.0],
                "eles": [100.0, 150.0, 200.0],
                "bbox": dict(
                    min_lat=-5.0, max_lat=5.0, min_lon=50.0, max_lon=60.0
                ),
            },
            "metrics": {
                "common": {
                    "dist": 1.0,
                    "loss": 2.0,
                    "gain": 3.0,
                    "s_dists": {"surface_1": 1.0},
                }
            },
        }
        target_dict = {
            "route_id": "route_id",
            "geometry": {
                "lats": [-5.0, 0.0, 5.0],
                "lons": [45.0, 50.0, 55.0],
                "dists": [0.0, 5.0, 10.0],
                "eles": [100.0, 150.0, 200.0],
                "bbox": dict(
                    min_lat=-5.0, max_lat=5.0, min_lon=50.0, max_lon=60.0
                ),
            },
            "metrics": {
                "dist": 1.0,
                "loss": 2.0,
                "gain": 3.0,
                "s_dists": {"surface_1": 1.0},
            },
        }

        # Act
        result_route = Route.from_api_response(test_response)
        result_dict = result_route.to_dict()

        # Assert
        assert isinstance(result_route, Route)
        assert isinstance(result_route.geometry, RouteGeometry)
        assert isinstance(result_route.metrics, RouteMetrics)
        assert isinstance(result_route.geometry.bbox, BBox)
        assert result_route.route_id == "route_id"
        assert result_dict == target_dict
