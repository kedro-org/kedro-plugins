"""``AbstractDataset`` implementations that produce pandas DataFrames."""

from typing import Any

import lazy_loader as lazy

try:
    from .csv_dataset import CSVDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    CSVDataset: Any

try:
    from .deltatable_dataset import DeltaTableDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    DeltaTableDataset: Any

try:
    from .excel_dataset import ExcelDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    ExcelDataset: Any

try:
    from .feather_dataset import FeatherDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    FeatherDataset: Any

try:
    from .gbq_dataset import GBQQueryDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    GBQQueryDataset: Any

try:
    from .gbq_dataset import GBQTableDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    GBQTableDataset: Any

try:
    from .generic_dataset import GenericDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    GenericDataset: Any

try:
    from .hdf_dataset import HDFDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    HDFDataset: Any

try:
    from .json_dataset import JSONDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    JSONDataset: Any

try:
    from .parquet_dataset import ParquetDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    ParquetDataset: Any

try:
    from .sql_dataset import SQLQueryDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    SQLQueryDataset: Any

try:
    from .sql_dataset import SQLTableDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    SQLTableDataset: Any

try:
    from .xml_dataset import XMLDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    XMLDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "csv_dataset": ["CSVDataset"],
        "deltatable_dataset": ["DeltaTableDataset"],
        "excel_dataset": ["ExcelDataset"],
        "feather_dataset": ["FeatherDataset"],
        "gbq_dataset": [
            "GBQQueryDataset",
            "GBQTableDataset",
        ],
        "generic_dataset": ["GenericDataset"],
        "hdf_dataset": ["HDFDataset"],
        "json_dataset": ["JSONDataset"],
        "parquet_dataset": ["ParquetDataset"],
        "sql_dataset": [
            "SQLQueryDataset",
            "SQLTableDataset",
        ],
        "xml_dataset": ["XMLDataset"],
    },
)
