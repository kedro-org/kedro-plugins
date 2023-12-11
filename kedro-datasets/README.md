# Kedro-Datasets

<!-- Note that the contents of this file are also used in the documentation, see docs/source/index.md -->

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11-blue.svg)](https://pypi.org/project/kedro-datasets/)
[![PyPI Version](https://badge.fury.io/py/kedro-datasets.svg)](https://pypi.org/project/kedro-datasets/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/ambv/black)

Welcome to `kedro_datasets`, the home of Kedro's data connectors. Here you will find `AbstractDataset` implementations powering Kedro's DataCatalog created by QuantumBlack and external contributors.

## Installation

`kedro-datasets` is a Python plugin. To install it:

```bash
pip install kedro-datasets
```

## What `AbstractDataset` implementations are supported?

We support a range of data connectors, including CSV, Excel, Parquet, Feather, HDF5, JSON, Pickle, SQL Tables, SQL Queries, Spark DataFrames and more. We even allow support for working with images.

These data connectors are supported with the APIs of `pandas`, `spark`, `networkx`, `matplotlib`, `yaml` and more.

[The Data Catalog](https://kedro.readthedocs.io/en/stable/data/data_catalog.html) allows you to work with a range of file formats on local file systems, network file systems, cloud object stores, and Hadoop.

Here is a full list of [supported data connectors and APIs](https://docs.kedro.org/en/stable/kedro_datasets.html).

## How can I create my own `AbstractDataset` implementation?
Take a look at our [instructions on how to create your own `AbstractDataset` implementation](https://kedro.readthedocs.io/en/stable/extend_kedro/custom_datasets.html).

## Can I contribute?

Yes! Want to help build Kedro-Datasets? Check out our guide to [contributing](https://github.com/kedro-org/kedro-plugins/blob/main/kedro-datasets/CONTRIBUTING.md).

## What licence do you use?

Kedro-Datasets is licensed under the [Apache 2.0](https://github.com/kedro-org/kedro-plugins/blob/main/LICENSE.md) License.

## Python version support policy
* The [Kedro-Datasets](https://github.com/kedro-org/kedro-plugins/tree/main/kedro-datasets) package follows the [NEP 29](https://numpy.org/neps/nep-0029-deprecation_policy.html) Python version support policy.
