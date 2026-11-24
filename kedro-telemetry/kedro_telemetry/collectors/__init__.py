"""Collectors that gather project metrics for the telemetry events."""

from kedro_telemetry.collectors.datasets import collect_dataset_properties
from kedro_telemetry.collectors.validators import collect_validator_properties

__all__ = ["collect_dataset_properties", "collect_validator_properties"]
