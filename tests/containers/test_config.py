"""Ensure data passed from the webapp to the backend is being stored
properly"""

from unittest.mock import patch, MagicMock
from fell_finder.containers.config import RouteConfig


@patch("fell_finder.app_config")
def test_route_config(mock_app_config: MagicMock):
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

    mock_app_config["routing"] = {"distance_tolerance": 0.1}

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
        min_distance=10.0 / 1.1,
        max_distance=10.0 * 1.1,
    )

    # Act
    result = RouteConfig(**test_kwargs)  # type: ignore

    # Assert
    for attr, target in target_attrs.items():
        assert getattr(result, attr) == target
