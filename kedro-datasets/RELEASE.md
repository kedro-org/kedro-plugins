# Upcoming Release:

## Major features and improvements:
* Added pandas 2.0 support.
* Added SQLAlchemy 2.0 support (and dropped support for versions below 1.4).
* Reduced constructor arguments for `APIDataSet` by replacing most arguments with a single constructor argument `load_args`. This makes it more consistent with other Kedro DataSets and the underlying `requests` API, and automatically enables the full configuration domain: stream, certificates, proxies, and more.
## Bug fixes and other changes
* Relaxed `delta-spark` upper bound to allow compatibility with Spark 3.1.x and 3.2.x.

# Release 1.2.1:

## Major features and improvements:

## Bug fixes and other changes
* Renamed `TensorFlowModelDataset` to `TensorFlowModelDataSet` to be consistent with all other plugins in kedro-datasets.

# Release 1.2.0:

## Major features and improvements:
* Added `fsspec` resolution in `SparkDataSet` to support more filesystems.
* Added the `_preview` method to the Pandas `ExcelDataSet` and `CSVDataSet` classes.

## Bug fixes and other changes
* Fixed a docstring in the Pandas `SQLQueryDataSet` as part of the Sphinx revamp on Kedro.

# Release 1.1.1:

## Bug fixes and other changes

* Fixed problematic docstrings causing Read the Docs builds on Kedro to fail.

# Release 1.1.0:

## Major features and improvements:

* Added the following new datasets:

| Type                                 | Description                                                                | Location                      |
| ------------------------------------ | -------------------------------------------------------------------------- | ----------------------------- |
| `polars.CSVDataSet` | A `CSVDataSet` backed by [polars](https://www.pola.rs/), a lighting fast dataframe package built entirely using Rust. | `kedro_datasets.polars` |
| `snowflake.SnowparkTableDataSet` | Work with [Snowpark](https://www.snowflake.com/en/data-cloud/snowpark/) DataFrames from tables in Snowflake. | `kedro_datasets.snowflake` |

## Bug fixes and other changes
* Add `mssql` backend to the `SQLQueryDataSet` DataSet using `pyodbc` library.
* Added a warning when the user tries to use `SparkDataSet` on Databricks without specifying a file path with the `/dbfs/` prefix.

# Release 1.0.2:

## Bug fixes and other changes
* Change reference to `kedro.pipeline.Pipeline` object throughout test suite with `kedro.modular_pipeline.pipeline` factory.
* Relaxed PyArrow range in line with pandas.
* Fixed outdated links to the dill package documentation.

# Release 1.0.1:

## Bug fixes and other changes
* Fixed docstring formatting in `VideoDataSet` that was causing the documentation builds to fail.


# Release 1.0.0:

First official release of Kedro-Datasets.

Datasets are Kedro’s way of dealing with input and output in a data and machine-learning pipeline. [Kedro supports numerous datasets](https://kedro.readthedocs.io/en/stable/kedro.extras.datasets.html) out of the box to allow you to process different data formats including Pandas, Plotly, Spark and more.

The datasets have always been part of the core Kedro Framework project inside `kedro.extras`. In Kedro `0.19.0`, we will remove datasets from Kedro to reduce breaking changes associated with dataset dependencies. Instead, users will need to use the datasets from the `kedro-datasets` repository instead.

## Major features and improvements
* Changed `pandas.ParquetDataSet` to load data using pandas instead of parquet

# Release 0.1.0:

The initial release of `kedro-datasets`.

## Thanks to our main contributors


We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.
