"""Provides interface to Hugging Face transformers and datasets."""

from typing import Any

import lazy_loader as lazy

try:
    from .hugging_face_dataset import HFDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    HFDataset: Any

try:
    from .arrow_dataset import ArrowDataset
except (ImportError, RuntimeError):
    ArrowDataset: Any

try:
    from .parquet_dataset import ParquetDataset
except (ImportError, RuntimeError):
    ParquetDataset: Any

try:
    from .json_dataset import JSONDataset
except (ImportError, RuntimeError):
    JSONDataset: Any

try:
    from .csv_dataset import CSVDataset
except (ImportError, RuntimeError):
    CSVDataset: Any

try:
    from .lance_dataset import LanceDataset
except (ImportError, RuntimeError):
    LanceDataset: Any

try:
    from .hdf5_dataset import HDF5Dataset
except (ImportError, RuntimeError):
    HDF5Dataset: Any

try:
    from .transformer_pipeline_dataset import HFTransformerPipelineDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    HFTransformerPipelineDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "hugging_face_dataset": ["HFDataset"],
        "arrow_dataset": ["ArrowDataset"],
        "parquet_dataset": ["ParquetDataset"],
        "json_dataset": ["JSONDataset"],
        "csv_dataset": ["CSVDataset"],
        "lance_dataset": ["LanceDataset"],
        "hdf5_dataset": ["HDF5Dataset"],
        "transformer_pipeline_dataset": ["HFTransformerPipelineDataset"],
    },
)
