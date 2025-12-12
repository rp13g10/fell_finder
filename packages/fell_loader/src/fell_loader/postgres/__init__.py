"""Contains code for the final stage of the ingestion pipeline, which loads
optimised CSV files into the postgres database.
"""

from fell_loader.postgres.graph import GraphUploader

__all__ = ["GraphUploader"]
