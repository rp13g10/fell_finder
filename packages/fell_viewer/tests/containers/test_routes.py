"""Tests for the containers which store route information"""

import json
from collections import defaultdict
from textwrap import dedent

from fell_viewer.containers.routes import (
    Route,
    RouteGeometry,
    RouteMetrics,
    StepData,
)


def test_step_data():
    """Make sure data is being stored correctly and defaults have been set up
    properly"""
    # Arrange
    test_props = {
        "next_node": 0,
        "distance": 10.0,
        "elevation_gain": 1.0,
        "elevation_loss": 1.0,
        "surface": "surface",
    }

    target_list_props = ["lats", "lons", "distances", "elevations"]

    # Act
    result = StepData(**test_props)

    # Assert
    for prop in test_props:
        assert getattr(result, prop) == test_props[prop]

    for prop in target_list_props:
        assert isinstance(getattr(result, prop), list)


class TestRouteGeometry:
    """Make sure route geometry is being evaluated and stored correctly"""

    test_props = {
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
        "distances": [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        "elevations": [2.0, 1.0, 2.0, 3.0, 2.0, 1.0, 5.0, 4.0, 3.0, 1.0, 6.0],
    }

    def test_to_dict(self):
        """to_dict should return the input data"""
        # Arrange
        test_geo = RouteGeometry(**self.test_props)

        # Act
        result = test_geo.to_dict()

        # Assert
        assert result == self.test_props

    def test_current_distance_with_data(self):
        """Check the current_distance property when there is data"""

        # Arrange
        test_geo = RouteGeometry(**self.test_props)
        target = 10.0

        # Act
        result = test_geo.current_distance

        # Assert
        assert result == target

    def test_current_distance_without_data(self):
        """Check the current_distance property when there is no data"""

        # Arrange
        test_geo = RouteGeometry()
        target = 0.0

        # Act
        result = test_geo.current_distance

        # Assert
        assert result == target

    def test_max_lat(self):
        """Check that the correct value is returned"""
        # Arrange
        test_geo = RouteGeometry(**self.test_props)
        target = 5.0

        # Act
        result = test_geo.max_lat

        # Assert
        assert result == target

    def test_min_lat(self):
        """Check that the correct value is returned"""

        # Arrange
        test_geo = RouteGeometry(**self.test_props)
        target = -5.0

        # Act
        result = test_geo.min_lat

        # Assert
        assert result == target

    def test_max_lon(self):
        """Check that the correct value is returned"""

        # Arrange
        test_geo = RouteGeometry(**self.test_props)
        target = 60.0

        # Act
        result = test_geo.max_lon

        # Assert
        assert result == target

    def test_min_lon(self):
        """Check that the correct value is returned"""

        # Arrange
        test_geo = RouteGeometry(**self.test_props)
        target = 50.0

        # Act
        result = test_geo.min_lon

        # Assert
        assert result == target

    def test_centre(self):
        """Check that the correct value is returned"""

        # Arrange
        test_geo = RouteGeometry(**self.test_props)
        target = (0.0, 55.0)

        # Act
        result = test_geo.centre

        # Assert
        assert result == target

    def test_bounds(self):
        """Check that the correct value is returned"""

        # Arrange
        test_geo = RouteGeometry(**self.test_props)
        target = ((-5.0, 60.0), (5.0, 50.0))

        # Act
        result = test_geo.bounds

        # Assert
        assert result == target

    def test_coords(self):
        """Check that the correct value is returned"""

        # Arrange
        test_geo = RouteGeometry(**self.test_props)
        target = [
            (-5.0, 50.0),
            (-4.0, 51.0),
            (-3.0, 52.0),
            (-2.0, 53.0),
            (-1.0, 54.0),
            (0.0, 55.0),
            (1.0, 56.0),
            (2.0, 57.0),
            (3.0, 58.0),
            (4.0, 59.0),
            (5.0, 60.0),
        ]

        # Act
        result = test_geo.coords

        # Assert
        assert result == target


class TestRouteMetrics:
    """Make sure route metrics are being evaluated and stored correctly"""

    test_props = {
        "distance": 5.0,
        "elevation_gain": 25.0,
        "elevation_loss": 10.0,
    }

    test_alt_props = {
        "distance": 5.0,
        "elevation_gain": 10.0,
        "elevation_loss": 25.0,
    }

    def test_to_dict(self):
        """to_dict should return the input data"""
        # Arrange
        test_mets = RouteMetrics(**self.test_props)  # type: ignore

        # Act
        result = test_mets.to_dict()

        # Assert
        for prop, value in self.test_props.items():
            assert result[prop] == value
        assert isinstance(result["surface_distances"], defaultdict)
        assert len(result["surface_distances"]) == 0

    def test_elevation_gain_potential_non_zero(self):
        """Check that the correct value is retured"""

        # Arrange
        test_mets = RouteMetrics(**self.test_alt_props)  # type: ignore
        target = 15.0

        # Act
        result = test_mets.elevation_gain_potential

        # Assert
        assert result == target

    def test_elevation_gain_potential_zero(self):
        """Check that the correct value is retured"""

        # Arrange
        test_mets = RouteMetrics(**self.test_props)  # type: ignore
        target = 0.0

        # Act
        result = test_mets.elevation_gain_potential

        # Assert
        assert result == target

    def test_elevation_loss_potential_non_zero(self):
        """Check that the correct value is retured"""

        # Arrange
        test_mets = RouteMetrics(**self.test_props)  # type: ignore
        target = 15.0

        # Act
        result = test_mets.elevation_loss_potential

        # Assert
        assert result == target

    def test_elevation_loss_potential_zero(self):
        """Check that the correct value is retured"""

        # Arrange
        test_mets = RouteMetrics(**self.test_alt_props)  # type: ignore
        target = 0.0

        # Act
        result = test_mets.elevation_loss_potential

        # Assert
        assert result == target

    def test_ratio(self):
        """Check that the correct value is returned"""

        # Arrange
        test_mets = RouteMetrics(**self.test_alt_props)  # type: ignore
        target = (10.0 + 15.0) / 5.0  # 5.0

        # Act
        result = test_mets.ratio

        # Assert
        assert result == target


class TestRoute:
    """Make sure the primary route class has been set up properly"""

    def test_from_dict(self):
        """Check that route creation from static dict is working"""
        # Arrange

        test_dict = {
            "points": [0, 1, 0],
            "visited": [0, 1],
            "route_id": "route_id",
            "geometry": {
                "lats": [-5.0, 0.0, 5.0],
                "lons": [45.0, 50.0, 55.0],
                "distances": [0.0, 5.0, 10.0],
                "elevations": [100.0, 150.0, 200.0],
            },
            "metrics": {
                "distance": 1.0,
                "elevation_gain": 2.0,
                "elevation_loss": 3.0,
                "surface_distances": {"surface_1": 1.0},
            },
        }

        # Act
        result = Route.from_dict(test_dict)

        # Assert
        assert result.points == [0, 1, 0]
        assert result.visited == {0, 1}
        assert result.route_id == "route_id"
        assert isinstance(result.geometry, RouteGeometry)
        assert isinstance(result.metrics, RouteMetrics)

    def test_from_str(self):
        """Check that route creation from static str is working"""
        # Arrange
        test_str = dedent("""{
            "points": [0, 1, 0],
            "visited": [0, 1],
            "route_id": "route_id",
            "geometry": {
                "lats": [-5.0, 0.0, 5.0],
                "lons": [45.0, 50.0, 55.0],
                "distances": [0.0, 5.0, 10.0],
                "elevations": [100.0, 150.0, 200.0]
            },
            "metrics": {
                "distance": 1.0,
                "elevation_gain": 2.0,
                "elevation_loss": 3.0,
                "surface_distances": {"surface_1": 1.0}
            }
        }""")

        # Act
        result = Route.from_str(test_str)

        # Assert
        assert result.points == [0, 1, 0]
        assert result.visited == {0, 1}
        assert result.route_id == "route_id"
        assert isinstance(result.geometry, RouteGeometry)
        assert isinstance(result.metrics, RouteMetrics)

    def test_to_dict(self):
        """Check that conversion back to dict retains all information"""
        # Arrange

        target = {
            "points": [0, 1, 0],
            "visited": [0, 1],
            "route_id": "route_id",
            "geometry": {
                "lats": [-5.0, 0.0, 5.0],
                "lons": [45.0, 50.0, 55.0],
                "distances": [0.0, 5.0, 10.0],
                "elevations": [100.0, 150.0, 200.0],
            },
            "metrics": {
                "distance": 1.0,
                "elevation_gain": 2.0,
                "elevation_loss": 3.0,
                "surface_distances": {"surface_1": 1.0},
            },
        }

        # Act
        result = Route.from_dict(target).to_dict()

        # Assert
        assert result == target

    def test_to_str(self):
        """Check that conversion back to str retains all information"""
        # Arrange

        test_data = {
            "points": [0, 1, 0],
            "visited": [0, 1],
            "route_id": "route_id",
            "geometry": {
                "lats": [-5.0, 0.0, 5.0],
                "lons": [45.0, 50.0, 55.0],
                "distances": [0.0, 5.0, 10.0],
                "elevations": [100.0, 150.0, 200.0],
            },
            "metrics": {
                "distance": 1.0,
                "elevation_gain": 2.0,
                "elevation_loss": 3.0,
                "surface_distances": {"surface_1": 1.0},
            },
        }
        target = json.dumps(test_data)

        # Act
        result = Route.from_dict(test_data).to_str()

        # Assert
        assert result == target

    def test_generate_new_id(self):
        """Make sure new IDs are being generated on request"""
        # Arrange
        test_route = Route(0)
        start_id = test_route.route_id

        # Act
        test_route.generate_new_id()

        # Assert
        assert test_route.route_id != start_id
        assert isinstance(test_route.route_id, str)

    def test_start_node(self):
        """Make sure the start node is correctly determined"""

        # Arrange
        test_route = Route(0)
        test_route.points = [0, 1, 2, 3]

        target = 0

        # Act
        result = test_route.start_node

        # Assert
        assert result == target

    def test_cur_node(self):
        """Make sure the current node is correctly determined"""

        # Arrange
        test_route = Route(0)
        test_route.points = [0, 1, 2, 3]

        target = 3

        # Act
        result = test_route.cur_node

        # Assert
        assert result == target

    def test_is_closed_loop_true(self):
        """Make sure closed loops are correctly flagged"""
        # Arrange
        test_route = Route(0)
        test_route.points = [0, 1, 2, 3, 2, 1, 0]

        # Act
        result = test_route.is_closed_loop

        # Assert
        assert result

    def test_is_closed_loop_false(self):
        """Make sure open loops are not flagged by mistake"""

        # Arrange
        test_route = Route(0)
        test_route.points = [0, 1, 2, 3, 2, 1]

        # Act
        result = test_route.is_closed_loop

        # Assert
        assert not result

    def test_is_closed_loop_length_one(self):
        """Make sure loops are not instantly flagged as closed on creation"""

        # Arrange
        test_route = Route(0)
        test_route.points = [0]

        # Act
        result = test_route.is_closed_loop

        # Assert
        assert not result

    def test_last_3_nodes(self):
        """Overlap should be set according to threshold for long routes"""
        # Arrange
        test_route = Route(0)
        test_route.points = list(range(25))

        target = {22, 23, 24}

        # Act
        result = test_route.last_3_nodes

        # Assert
        assert result == target

    def test_take_step(self):
        """Check that taking a step applies the correct changes"""
        # Arrange

        test_dict = {
            "points": [0, 1, 0],
            "visited": [0, 1],
            "route_id": "route_id",
            "geometry": {
                "lats": [-5.0, 0.0, 5.0],
                "lons": [45.0, 50.0, 55.0],
                "distances": [0.0, 5.0, 10.0],
                "elevations": [100.0, 150.0, 200.0],
            },
            "metrics": {
                "distance": 1.0,
                "elevation_gain": 2.0,
                "elevation_loss": 3.0,
                "surface_distances": {"surface_1": 1.0},
            },
        }

        test_step = StepData(
            next_node=2,
            distance=1.0,
            elevation_gain=1.0,
            elevation_loss=1.0,
            surface="surface_2",
            lats=[10.0, 15.0],
            lons=[60.0, 65.0],
            distances=[0.5, 0.5],
            elevations=[201.0, 200.0],
        )

        target_dict = {
            "points": [0, 1, 0, 2],
            "visited": [0, 1, 2],
            "route_id": "route_id",
            "geometry": {
                "lats": [-5.0, 0.0, 5.0, 10.0, 15.0],
                "lons": [45.0, 50.0, 55.0, 60.0, 65.0],
                "distances": [0.0, 5.0, 10.0, 10.5, 11.0],
                "elevations": [100.0, 150.0, 200.0, 201.0, 200.0],
            },
            "metrics": {
                "distance": 2.0,
                "elevation_gain": 3.0,
                "elevation_loss": 4.0,
                "surface_distances": {"surface_1": 1.0, "surface_2": 1.0},
            },
        }

        test_route = Route.from_dict(test_dict)

        # Act
        result_route = test_route.take_step(test_step)
        result_dict = result_route.to_dict()

        # Assert

        # Check new ID generated, remove from dicts before comparing
        assert result_dict["route_id"] != target_dict["route_id"]
        del result_dict["route_id"]
        del target_dict["route_id"]

        # Check output is as expected
        assert result_dict == target_dict

        # Ensure no changes are made to original route objects
        assert result_route is not test_route
        assert len(test_route.points) == 3
        assert len(result_route.points) == 4

    def test_finalize(self):
        """Make sure data is correctly cleared out on finalization"""
        # Arrange
        test_route = Route(0)
        test_route.points = [0, 1]
        test_route.visited = {0, 1}

        # Act
        test_route.finalize()

        # Assert
        assert not test_route.points
        assert not test_route.visited
