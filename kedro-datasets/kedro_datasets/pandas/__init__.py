"""``AbstractDataset`` implementations that produce pandas DataFrames."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
CSVDataSet: type[CSVDataset]
CSVDataset: Any
DeltaTableDataSet: type[DeltaTableDataset]
DeltaTableDataset: Any
ExcelDataSet: type[ExcelDataset]
ExcelDataset: Any
FeatherDataSet: type[FeatherDataset]
FeatherDataset: Any
GBQQueryDataSet: type[GBQQueryDataset]
GBQQueryDataset: Any
GBQTableDataSet: type[GBQTableDataset]
GBQTableDataset: Any
GenericDataSet: type[GenericDataset]
GenericDataset: Any
HDFDataSet: type[HDFDataset]
HDFDataset: Any
JSONDataSet: type[JSONDataset]
JSONDataset: Any
ParquetDataSet: type[ParquetDataset]
ParquetDataset: Any
SQLQueryDataSet: type[SQLQueryDataset]
SQLQueryDataset: Any
SQLTableDataSet: type[SQLTableDataset]
SQLTableDataset: Any
XMLDataSet: type[XMLDataset]
XMLDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "csv_dataset": ["CSVDataSet", "CSVDataset"],
        "deltatable_dataset": ["DeltaTableDataSet", "DeltaTableDataset"],
        "excel_dataset": ["ExcelDataSet", "ExcelDataset"],
        "feather_dataset": ["FeatherDataSet", "FeatherDataset"],
        "gbq_dataset": [
            "GBQQueryDataSet",
            "GBQQueryDataset",
            "GBQTableDataSet",
            "GBQTableDataset",
        ],
        "generic_dataset": ["GenericDataSet", "GenericDataset"],
        "hdf_dataset": ["HDFDataSet", "HDFDataset"],
        "json_dataset": ["JSONDataSet", "JSONDataset"],
        "parquet_dataset": ["ParquetDataSet", "ParquetDataset"],
        "sql_dataset": [
            "SQLQueryDataSet",
            "SQLQueryDataset",
            "SQLTableDataSet",
            "SQLTableDataset",
        ],
        "xml_dataset": ["XMLDataSet", "XMLDataset"],
    },
)
