"""Contains utility functions which help to encode content ready for
displaying as part of the website"""

import base64
import os

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(CUR_DIR, "../assets")


def get_image_as_str(image_file: str) -> str:
    """Reads in the contents of the requested image file from the assets
    folder and returns it in a format which can be set as the"""
    with open(os.path.join(ASSETS_DIR, image_file), "rb") as fobj:
        b64_bytes = base64.b64encode(fobj.read())

    file_format = image_file.split(".")[-1]

    img_str = f"data:/image/{file_format};base64,{b64_bytes.decode('utf8')}"

    return img_str
