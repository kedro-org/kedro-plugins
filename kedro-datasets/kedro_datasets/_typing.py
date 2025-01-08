"""
`_typing.py` defines custom data types for Kedro-viz integration. It uses NewType from the typing module.
These types are used to facilitate data rendering in the Kedro-viz front-end.
"""

from typing import NewType

TablePreview = NewType("TablePreview", dict)
ImagePreview = NewType("ImagePreview", str)
PlotlyPreview = NewType("PlotlyPreview", dict)
JSONPreview = NewType("JSONPreview", str)
