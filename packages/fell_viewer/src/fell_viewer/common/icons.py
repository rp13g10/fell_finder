"""Contains any icons which are used across the webapp, encoded into base64
bytes"""

import base64
import os

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(CUR_DIR, "../assets")

with open(os.path.join(ASSETS_DIR, "map_pin.png"), "rb") as fobj:
    MAP_PIN_PNG = base64.b64encode(fobj.read())

with open(os.path.join(ASSETS_DIR, "route_start.png"), "rb") as fobj:
    ROUTE_START_PNG = base64.b64encode(fobj.read())
