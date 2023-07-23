"""``AbstractDataSet`` implementations that produce pandas DataFrames."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
CSVDataSet: Any
DeltaTableDataSet: Any
ExcelDataSet: Any
FeatherDataSet: Any
GBQQueryDataSet: Any
GBQTableDataSet: Any
GenericDataSet: Any
HDFDataSet: Any
JSONDataSet: Any
ParquetDataSet: Any
SQLQueryDataSet: Any
SQLTableDataSet: Any
XMLDataSet: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "csv_dataset": ["CSVDataSet"],
        "deltatable_dataset": ["DeltaTableDataSet"],
        "excel_dataset": ["ExcelDataSet"],
        "feather_dataset": ["FeatherDataSet"],
        "gbq_dataset": ["GBQQueryDataSet", "GBQTableDataSet"],
        "generic_dataset": ["GenericDataSet"],
        "hdf_dataset": ["HDFDataSet"],
        "json_dataset": ["JSONDataSet"],
        "parquet_dataset": ["ParquetDataSet"],
        "sql_dataset": ["SQLQueryDataSet", "SQLTableDataSet"],
        "xml_dataset": ["XMLDataSet"],
    },
)
