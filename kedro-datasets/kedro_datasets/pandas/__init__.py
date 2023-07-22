"""``AbstractDataSet`` implementations that produce pandas DataFrames."""

import lazy_loader as lazy

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
