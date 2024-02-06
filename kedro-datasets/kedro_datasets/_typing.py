"""
`constants.py` defines custom data types for Kedro-viz integration. It uses NewType from the typing module.
These types are used to facilitate data rendering in the Kedro-viz front-end.
"""

from typing import NewType

Table = NewType("Table", dict)
Image = NewType("Image", str)
Plotly = NewType("Plotly", dict)
JSON = NewType("JSON", dict)


# experiment tracking datasets types
MetricsTracking = NewType("MetricsTracking", dict)
JSONTracking = NewType("JSONTracking", dict)
