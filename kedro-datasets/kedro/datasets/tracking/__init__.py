"""Dataset implementations to save data for Kedro Experiment Tracking"""

__all__ = ["MetricsDataSet", "JSONDataSet"]


from contextlib import suppress

with suppress(ImportError):
    from datasets.tracking.metrics_dataset import MetricsDataSet
with suppress(ImportError):
    from datasets.tracking.json_dataset import JSONDataSet
