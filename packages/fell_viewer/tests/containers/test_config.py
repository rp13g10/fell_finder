"""Ensure data passed from the webapp to the backend is being stored
properly"""

import os

from fell_viewer.containers.config import RouteConfig


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
