"""Setup configuration for the fell_finder package"""

from setuptools import setup

setup(
    name="fell_finder",
    version="0.1.0",
    where="src",
    include=["fell_finder"],
    install_requires=[
        "polars",
        "pyspark",
        "pandas",
        "pyarrow",
        "numpy",
        "tqdm",
        "pytest",
        "pytest-cov",
        "pytest-xdist",
        "rasterio",
        "bng-latlon",
        "networkx",
        "geopy",
        "thefuzz",
        "plotly",
        "dash",
        "dash-leaflet",
    ],
)
