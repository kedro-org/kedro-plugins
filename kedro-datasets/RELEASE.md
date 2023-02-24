
# Upcoming Release:

# Release 1.0.1:

## Bug fixes and other changes
* Fixed doc string formatting in `VideoDataSet` causing the documentation builds to fail.

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
