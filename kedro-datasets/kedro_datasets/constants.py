"""
`constants.py` defines custom data types for Kedro-viz integration. It uses NewType from the typing module.
These types are used to facilitate data rendering in the Kedro-viz front-end.
"""

from typing import NewType

Dataframe = NewType("Dataframe", dict)
Image = NewType("Image", str)
Plot = NewType("Plot", dict)
JSON = NewType("JSON", str)


# experiment tracking datasets types
MetricsTracking = NewType("MetricsTracking", str)
JSONTracking = NewType("JSONTracking", str)
