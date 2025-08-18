# Kedro-Datasets

<!-- Note that the contents of this file are also used in the documentation, see docs/source/index.md -->

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/kedro-org/kedro-plugins/blob/main/LICENSE.md)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://pypi.org/project/kedro-datasets/)
[![PyPI Version](https://badge.fury.io/py/kedro-datasets.svg)](https://pypi.org/project/kedro-datasets/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/ambv/black)

Welcome to `kedro_datasets`, the home of Kedro's data connectors. Here you will find `AbstractDataset` implementations powering Kedro's DataCatalog created by QuantumBlack and external contributors.

## Installation

`kedro-datasets` is a Python plugin. To install it:

```bash
pip install kedro-datasets
```

### Install dependencies at a group-level

Datasets are organised into groups e.g. `pandas`, `spark` and `pickle`. Each group has a collection of datasets, e.g.`pandas.CSVDataset`, `pandas.ParquetDataset` and more. You can install dependencies for an entire group of dependencies as follows:

```bash
pip install "kedro-datasets[<group>]"
```

This installs Kedro-Datasets and dependencies related to the dataset group. An example of this could be a workflow that depends on the data types in `pandas`. Run `pip install 'kedro-datasets[pandas]'` to install Kedro-Datasets and the dependencies for the datasets in the [`pandas` group](https://github.com/kedro-org/kedro-plugins/tree/main/kedro-datasets/kedro_datasets/pandas).

### Install dependencies at a type-level

To limit installation to dependencies specific to a dataset:

```bash
pip install "kedro-datasets[<group>-<dataset>]"
```

For example, your workflow might require the `pandas.ExcelDataset`, so to install its dependencies, run `pip install "kedro-datasets[pandas-exceldataset]"`.

```{note}
From `kedro-datasets` version 3.0.0 onwards, the names of the optional dataset-level dependencies have been normalised to follow [PEP 685](https://peps.python.org/pep-0685/). The '.' character has been replaced with a '-' character and the names are in lowercase. For example, if you had `kedro-datasets[pandas.ExcelDataset]` in your requirements file, it would have to be changed to `kedro-datasets[pandas-exceldataset]`.
```

## What `AbstractDataset` implementations are supported?

We support a range of data connectors, including CSV, Excel, Parquet, Feather, HDF5, JSON, Pickle, SQL Tables, SQL Queries, Spark DataFrames and more. We even allow support for working with images.

These data connectors are supported with the APIs of `pandas`, `spark`, `networkx`, `matplotlib`, `yaml` and more.

[The Data Catalog](https://docs.kedro.org/en/stable/data/data_catalog.html) allows you to work with a range of file formats on local file systems, network file systems, cloud object stores, and Hadoop.

Here is a full list of [supported data connectors and APIs](https://docs.kedro.org/projects/kedro-datasets/en/stable/api/kedro_datasets.html).

## How can I create my own `AbstractDataset` implementation?
Take a look at our [instructions on how to create your own `AbstractDataset` implementation](https://docs.kedro.org/en/stable/data/how_to_create_a_custom_dataset.html).

## Can I contribute?

Yes! Want to help build Kedro-Datasets? Check out our guide to [contributing](https://github.com/kedro-org/kedro-plugins/blob/main/kedro-datasets/CONTRIBUTING.md).

## What licence do you use?

Kedro-Datasets is licensed under the [Apache 2.0](https://github.com/kedro-org/kedro-plugins/blob/main/LICENSE.md) License.

## Python version support policy
* The [Kedro-Datasets](https://github.com/kedro-org/kedro-plugins/tree/main/kedro-datasets) package follows the [NEP 29](https://numpy.org/neps/nep-0029-deprecation_policy.html) Python version support policy.
