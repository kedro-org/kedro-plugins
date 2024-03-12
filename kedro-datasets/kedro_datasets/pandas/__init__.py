"""``AbstractDataset`` implementations that produce pandas DataFrames."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
CSVDataset: Any
DeltaTableDataset: Any
ExcelDataset: Any
FeatherDataset: Any
GBQQueryDataset: Any
GBQTableDataset: Any
GenericDataset: Any
HDFDataset: Any
JSONDataset: Any
ParquetDataset: Any
SQLQueryDataset: Any
SQLTableDataset: Any
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
