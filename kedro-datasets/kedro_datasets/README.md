# Datasets

Welcome to `kedro_datasets.datasets`, the home of Kedro's data connectors. Here you will find `AbstractDataset` implementations created by the core Kedro team and external contributors.

## What `AbstractDataset` implementations are supported?

We support a range of data descriptions, including CSV, Excel, Parquet, Feather, HDF5, JSON, Pickle, SQL Tables, SQL Queries, Spark DataFrames and more.

These data descriptions are supported with the APIs of `pandas`, `spark`, `networkx`, `matplotlib`, `yaml` and more.

[The Data Catalog](https://kedro.readthedocs.io/en/stable/data/data_catalog.html) allows you to work with a range of file formats on local file systems, network file systems, cloud object stores, and Hadoop.

Here is a full list of [supported data descriptions and APIs](https://docs.kedro.org/en/stable/kedro_datasets.html).

## How can I create my own `AbstractDataset` implementation?


Take a look at our [instructions on how to create your own `AbstractDataset` implementation](https://kedro.readthedocs.io/en/stable/extend_kedro/custom_datasets.html).
