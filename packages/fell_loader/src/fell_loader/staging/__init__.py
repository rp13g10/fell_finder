"""Second ingestion layer, joins elevation data onto the OSM dataset as part
of a delta. This is a very expensive join operation, so data is processed
in batches to allow stop/resume. Similarly, if map data is updated, only
nodes which have changed will be re-processed
"""

from fell_loader.staging.edges import EdgeStager
from fell_loader.staging.nodes import NodeStager

__all__ = ["EdgeStager", "NodeStager"]
