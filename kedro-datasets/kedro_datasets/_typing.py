"""
`_typing.py` defines custom data types for Kedro-viz integration. It uses NewType from the typing module.
These types are used to facilitate data rendering in the Kedro-viz front-end.
"""

from typing import TypedDict, TypeAlias


# Define a more structured TablePreview with TypedDict for IDE hinting
class TablePreview(TypedDict):
    index: list[int]  # Row indices as a list of integers
    columns: list[str]  # Column names as a list of strings
    data: list[list[float]]  # Nested lists representing rows of data (floats or other data types)


# TypeAlias to keep ImagePreview simpler
ImagePreview: TypeAlias = str


# Define PlotlyPreview as a TypedDict for clearer structure (if necessary)
class PlotlyPreview(TypedDict):
    data: dict
    layout: dict


# JSONPreview is a simple dictionary, so we could keep the NewType here or use TypeAlias
JSONPreview: TypeAlias = dict

# Experiment tracking datasets types
MetricsTrackingPreview: TypeAlias = dict
JSONTrackingPreview: TypeAlias = dict
