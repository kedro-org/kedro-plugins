from typing import NewType

Dataframe = NewType("Dataframe", dict)
Image = NewType("Image", str)
Plot = NewType("Plot", dict)
JSON = NewType("JSON", str)


# experiment_tracking datasets
MetricsTracking = NewType("MetricsTracking", str)
JSONTracking = NewType("JSONTracking", str)
