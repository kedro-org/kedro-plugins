# Upcoming Release
## Major features and improvements

- Added the following new experimental datasets:

| Type                           | Description                                                   | Location                             |
|--------------------------------|---------------------------------------------------------------|--------------------------------------|
| `polars.PolarsDatabaseDataset` | A dataset to load and save data to a SQL backend using Polars | `kedro_datasets_experimental.polars` |

## Bug fixes and other changes
## Breaking Changes
## Community contributions
- [Minura Punchihewa](https://github.com/MinuraPunchihewa)

# Release 8.0.0

## Major features and improvements

- Migrated docs to mkdocs
- Make `kedro-datasets` compatible with Kedro 1.0.0.
- Added the following new datasets:

| Type                  | Description                                                                       | Location                 |
|-----------------------|-----------------------------------------------------------------------------------|--------------------------|
| `openxml.DocxDataset` | A dataset for loading and saving .docx files (Microsoft Word) using `python-docx` | `kedro_datasets.openxml` |

## Bug fixes and other changes

- Fixed `PartitionedDataset` to reliably load newly created partitions, particularly with `ParallelRunner`, by ensuring `load()` always re-scans the filesystem .
- Add a parameter `encoding` inside the dataset `SQLQueryDataset` to choose the encoding format of the query.
- Corrected the `APIDataset` docstring to clarify that request parameters should be passed via `load_args`, not as top-level arguments.

## Breaking changes

- `kedro-datasets` now requires Kedro 1.0.0 or higher.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Paul Lemonnier](https://github.com/PaulLemonnier)
- [Seohyun Park](https://github.com/soyamimi)
- [Daniel Russell-Brain](https://github.com/killerfridge)


# Release 7.0.0

## Major features and improvements

- Added a parameter to enable/disable lazy saving for `PartitionedDataset`.
- Added `ibis-athena` and `ibis-databricks` extras for the backends added in Ibis 10.0.
- Renamed `MatplotlibWriter` to `MatplotlibDataset` for consistency with other dataset naming conventions. `MatplotlibWriter` is deprecated and will be removed in a future release.
- Added the following new **experimental** datasets:

| Type                           | Description                                                               | Location                             |
| ------------------------------ | ------------------------------------------------------------------------- | ------------------------------------ |
| `optuna.StudyDataset`          | A dataset for saving and loading Optuna studies.                          | `kedro_datasets_experimental.optuna` |
| `darts.DartsTorchModelDataset` | A dataset for securely saving and loading Darts Torch Forecasting Models. | `kedro_datasets_experimental.darts`  |

## Bug fixes and other changes

- Fixed `polars.CSVDataset` `save` method on Windows using `utf-8` as default encoding.
- Made `table_name` a keyword argument in the `ibis.FileDataset` implementation to be compatible with Ibis 10.0.
- Fixed how sessions are handled in the `snowflake.SnowflakeTableDataset` implementation.
- Fixed credentials handling in `pandas.GBQQueryDataset` and `pandas.GBQTableDataset`.

## Breaking changes

- Removed `tracking.MetricsDataset` and `tracking.JSONDataset`.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Szymon Cogiel](https://github.com/SzymonCogiel)
- [Abhishek Bhatia](https://github.com/abhi8893)
- [Guillaume Tauzin](https://github.com/gtauzin)


# Release 6.0.0

## Major features and improvements

- Supported passing `database` to `ibis.TableDataset` for load and save operations.
- Added functionality to save pandas DataFrames directly to Snowflake, facilitating seamless `.csv` ingestion.
- Added Python 3.9, 3.10 and 3.11 support for `snowflake.SnowflakeTableDataset`.
- Enabled connection sharing between `ibis.FileDataset` and `ibis.TableDataset` instances, thereby allowing nodes to save data loaded by one to the other (as long as they share the same connection configuration).
- Added the following new **experimental** datasets:

| Type                              | Description                                                                | Location                                  |
| --------------------------------- | -------------------------------------------------------------------------- | ----------------------------------------- |
| `databricks.ExternalTableDataset` | A dataset for accessing external tables in Databricks.                     | `kedro_datasets_experimental.databricks`  |
| `safetensors.SafetensorsDataset`  | A dataset for securely saving and loading files in the SafeTensors format. | `kedro_datasets_experimental.safetensors` |

## Bug fixes and other changes

- Delayed backend connection for `pandas.GBQTableDataset`. In practice, this means that a dataset's connection details aren't used (or validated) until the dataset is accessed. On the plus side, the cost of connection isn't incurred regardless of when or whether the dataset is used. Furthermore, this makes the dataset object serializable (e.g. for use with `ParallelRunner`), because the unserializable client isn't part of it.
- Removed the unused BigQuery client created in `pandas.GBQQueryDataset`. This makes the dataset object serializable (e.g. for use with `ParallelRunner`) by removing the unserializable object.
- Implemented Snowflake's [local testing framework](https://docs.snowflake.com/en/developer-guide/snowpark/python/testing-locally) for testing purposes.
- Improved the dependency management for Spark-based datasets by refactoring the Spark and Databricks utility functions used across the datasets.
- Added deprecation warning for `tracking.MetricsDataset` and `tracking.JSONDataset`.
- Moved `kedro-catalog` JSON schemas from Kedro core to `kedro-datasets`.

## Breaking changes

- Demoted `video.VideoDataset` from core to experimental dataset.
- Removed file handling capabilities from `ibis.TableDataset`. Use `ibis.FileDataset` to load and save files with an Ibis backend instead.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Thomas d'Hooghe](https://github.com/tdhooghe)
- [Minura Punchihewa](https://github.com/MinuraPunchihewa)
- [Mark Druffel](https://github.com/mark-druffel)
- [Chris Schopp](https://github.com/chrisschopp)

# Release 5.1.0

## Major features and improvements

- Added the following new core datasets:

| Type               | Description                                                   | Location              |
| ------------------ | ------------------------------------------------------------- | --------------------- |
| `ibis.FileDataset` | A dataset for loading and saving files using Ibis's backends. | `kedro_datasets.ibis` |

## Bug fixes and other changes

- Changed Ibis datasets to connect to an in-memory DuckDB database if connection configuration is not provided.

# Release 5.0.0

## Major features and improvements

- Removed support for Python 3.9.
- Added the following new **experimental** datasets:

| Type                          | Description                                                     | Location                              |
| ----------------------------- | --------------------------------------------------------------- | ------------------------------------- |
| `pytorch.PyTorchDataset`      | A dataset for securely saving and loading PyTorch models.       | `kedro_datasets_experimental.pytorch` |
| `prophet.ProphetModelDataset` | A dataset for Meta's Prophet model for time series forecasting. | `kedro_datasets_experimental.prophet` |

- Added the following new core datasets:

| Type                 | Description                                     | Location                |
| -------------------- | ----------------------------------------------- | ----------------------- |
| `plotly.HTMLDataset` | A dataset for saving a `plotly` figure as HTML. | `kedro_datasets.plotly` |

## Bug fixes and other changes

- Refactored all datasets to set `fs_args` defaults in the same way as `load_args` and `save_args` and not have hardcoded values in the save methods.
- Fixed bug related to loading/saving models from/to remote storage using `TensorFlowModelDataset`.
- Fixed deprecated load and save approaches of `GBQTableDataset` and `GBQQueryDataset` by invoking save and load directly over `pandas-gbq` lib.
- Fixed incorrect `pandas` optional dependency.

## Breaking changes

- Exposed `load` and `save` publicly for each dataset. This requires Kedro version 0.19.7 or higher.
- Replaced the `geopandas.GeoJSONDataset` with `geopandas.GenericDataset` to support parquet and feather file formats.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Brandon Meek](https://github.com/bpmeek)
- [yury-fedotov](https://github.com/yury-fedotov)
- [gitgud5000](https://github.com/gitgud5000)
- [janickspirig](https://github.com/janickspirig)
- [Galen Seilis](https://github.com/galenseilis)
- [Mariusz Wojakowski](https://github.com/mariusz89016)
- [harm-matthias-harms](https://github.com/harm-matthias-harms)
- [Felix Scherz](https://github.com/felixscherz)

# Release 4.1.0

## Major features and improvements

- Improved `partitions.PartitionedDataset` representation when printing.

## Bug fixes and other changes

- Updated `ibis.TableDataset` to make sure credentials are not printed in interactive environment.

## Breaking changes

## Community contributions

# Release 4.0.0

## Major features and improvements

- Added the following new **experimental** datasets:

| Type                                | Description                                               | Location                                |
| ----------------------------------- | --------------------------------------------------------- | --------------------------------------- |
| `langchain.ChatAnthropicDataset`    | A dataset for loading a ChatAnthropic langchain model.    | `kedro_datasets_experimental.langchain` |
| `langchain.ChatCohereDataset`       | A dataset for loading a ChatCohere langchain model.       | `kedro_datasets_experimental.langchain` |
| `langchain.OpenAIEmbeddingsDataset` | A dataset for loading a OpenAIEmbeddings langchain model. | `kedro_datasets_experimental.langchain` |
| `langchain.ChatOpenAIDataset`       | A dataset for loading a ChatOpenAI langchain model.       | `kedro_datasets_experimental.langchain` |
| `rioxarray.GeoTIFFDataset`          | A dataset for loading and saving geotiff raster data      | `kedro_datasets_experimental.rioxarray` |
| `netcdf.NetCDFDataset`              | A dataset for loading and saving "\*.nc" files.           | `kedro_datasets_experimental.netcdf`    |

- Added the following new core datasets:

| Type              | Description                                    | Location              |
| ----------------- | ---------------------------------------------- | --------------------- |
| `dask.CSVDataset` | A dataset for loading a CSV files using `dask` | `kedro_datasets.dask` |

- Extended preview feature to `yaml.YAMLDataset`.

## Bug fixes and other changes

- Added `metadata` parameter for a few datasets

## Breaking changes

- `netcdf.NetCDFDataset` moved from `kedro_datasets` to `kedro_datasets_experimental`.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Ian Whalen](https://github.com/ianwhale)
- [Charles Guan](https://github.com/charlesbmi)
- [Thomas Gölles](https://github.com/tgoelles)
- [Lukas Innig](https://github.com/derluke)
- [Michael Sexton](https://github.com/michaelsexton)
- [michal-mmm](https://github.com/michal-mmm)

# Release 3.0.1

## Bug fixes and other changes

- Removed arbitrary upper bound for `s3fs`.
- Added support for NetCDF4 via `engine="netcdf4"` and `engine="h5netcdf"` to `netcdf.NetCDFDataset`.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Charles Guan](https://github.com/charlesbmi)

# Release 3.0.0

## Major features and improvements

- Added the following new datasets:

| Type                   | Description                                             | Location                |
| ---------------------- | ------------------------------------------------------- | ----------------------- |
| `netcdf.NetCDFDataset` | A dataset for loading and saving `*.nc` files.          | `kedro_datasets.netcdf` |
| `ibis.TableDataset`    | A dataset for loading and saving using Ibis's backends. | `kedro_datasets.ibis`   |

- Added support for Python 3.12.
- Normalised optional dependencies names for datasets to follow [PEP 685](https://peps.python.org/pep-0685/). The `.` characters have been replaced with `-` in the optional dependencies names. Note that this might be breaking for some users. For example, users should now install optional dependencies for `pandas.ParquetDataset` from `kedro-datasets` like this:

```bash
pip install kedro-datasets[pandas-parquetdataset]
```

- Removed `setup.py` and move to `pyproject.toml` completely for `kedro-datasets`.

## Bug fixes and other changes

- If using MSSQL, `load_args:params` will be typecasted as tuple.
- Fixed bug with loading datasets from Hugging Face. Now allows passing parameters to the load_dataset function.
- Made `connection_args` argument optional when calling `create_connection()` in `sql_dataset.py`.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Riley Brady](https://github.com/riley-brady)
- [Andrew Cao](https://github.com/andrewcao1)
- [Eduardo Romero Lopez](https://github.com/eromerobilbomatica)
- [Jerome Asselin](https://github.com/jerome-asselin-buspatrol)

# Release 2.1.0

## Major features and improvements

- Added the following new datasets:

| Type                   | Description                                                 | Location                |
| ---------------------- | ----------------------------------------------------------- | ----------------------- |
| `matlab.MatlabDataset` | A dataset which uses `scipy` to save and load `.mat` files. | `kedro_datasets.matlab` |

- Extended preview feature for matplotlib, plotly and tracking datasets.
- Allowed additional parameters for sqlalchemy engine when using sql datasets.

## Bug fixes and other changes

- Removed Windows specific conditions in `pandas.HDFDataset` extra dependencies

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Samuel Lee SJ](https://github.com/samuel-lee-sj)
- [Felipe Monroy](https://github.com/felipemonroy)
- [Manuel Spierenburg](https://github.com/mjspier)

# Release 2.0.0

## Major features and improvements

- Added the following new datasets:

| Type                                       | Description                                                                                                                     | Location                     |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| `huggingface.HFDataset`                    | A dataset to load Hugging Face datasets using the [datasets](https://pypi.org/project/datasets) library.                        | `kedro_datasets.huggingface` |
| `huggingface.HFTransformerPipelineDataset` | A dataset to load pretrained Hugging Face transformers using the [transformers](https://pypi.org/project/transformers) library. | `kedro_datasets.huggingface` |

- Removed Dataset classes ending with "DataSet", use the "Dataset" spelling instead.
- Removed support for Python 3.7 and 3.8.
- Added [databricks-connect>=13.0](https://docs.databricks.com/en/dev-tools/databricks-connect-ref.html) support for Spark- and Databricks-based datasets.
- Bumped `s3fs` to latest calendar-versioned release.
- `PartitionedDataset` and `IncrementalDataset` now both support versioning of the underlying dataset.

## Bug fixes and other changes

- Fixed bug with loading models saved with `TensorFlowModelDataset`.
- Made dataset parameters keyword-only.
- Corrected pandas-gbq as py311 dependency.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Edouard59](https://github.com/Edouard59)
- [Miguel Rodriguez Gutierrez](https://github.com/MigQ2)
- [felixscherz](https://github.com/felixscherz)
- [Onur Kuru](https://github.com/kuruonur1)

# Release 1.8.0

## Major features and improvements

- Added the following new datasets:

| Type                       | Description                                                            | Location                |
| -------------------------- | ---------------------------------------------------------------------- | ----------------------- |
| `polars.LazyPolarsDataset` | A `LazyPolarsDataset` using [polars](https://www.pola.rs/)'s Lazy API. | `kedro_datasets.polars` |

- Moved `PartitionedDataSet` and `IncrementalDataSet` from the core Kedro repo to `kedro-datasets` and renamed to `PartitionedDataset` and `IncrementalDataset`.
- Renamed `polars.GenericDataSet` to `polars.EagerPolarsDataset` to better reflect the difference between the two dataset classes.
- Added a deprecation warning when using `polars.GenericDataSet` or `polars.GenericDataset` that these have been renamed to `polars.EagerPolarsDataset`
- Delayed backend connection for `pandas.SQLTableDataset`, `pandas.SQLQueryDataset`, and `snowflake.SnowparkTableDataset`. In practice, this means that a dataset's connection details aren't used (or validated) until the dataset is accessed. On the plus side, the cost of connection isn't incurred regardless of when or whether the dataset is used.

## Bug fixes and other changes

- Fixed erroneous warning when using an cloud protocol file path with SparkDataSet on Databricks.
- Updated `PickleDataset` to explicitly mention `cloudpickle` support.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [PtrBld](https://github.com/PtrBld)
- [Alistair McKelvie](https://github.com/alamastor)
- [Felix Wittmann](https://github.com/hfwittmann)
- [Matthias Roels](https://github.com/MatthiasRoels)

# Release 1.7.1

## Bug fixes and other changes

- Pinned `tables` version on `kedro-datasets` for Python < 3.8.

## Upcoming deprecations for Kedro-Datasets 2.0.0

- Renamed dataset and error classes, in accordance with the [Kedro lexicon](https://github.com/kedro-org/kedro/wiki/Kedro-documentation-style-guide#kedro-lexicon). Dataset classes ending with "DataSet" are deprecated and will be removed in 2.0.0.

# Release 1.7.0:

## Major features and improvements

- Added the following new datasets:

| Type                    | Description                                                                                                                | Location                |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------- | ----------------------- |
| `polars.GenericDataSet` | A `GenericDataSet` backed by [polars](https://www.pola.rs/), a lightning fast dataframe package built entirely using Rust. | `kedro_datasets.polars` |

## Bug fixes and other changes

- Fixed broken links in docstrings.
- Reverted PySpark pin to <4.0.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Walber Moreira](https://github.com/wmoreiraa)

# Release 1.6.0:

## Major features and improvements

- Added support for Python 3.11.

# Release 1.5.3:

## Bug fixes and other changes

- Made `databricks.ManagedTableDataSet` read-only by default.
  - The user needs to specify `write_mode` to allow `save` on the data set.
- Fixed an issue on `api.APIDataSet` where the sent data was doubly converted to json
  string (once by us and once by the `requests` library).
- Fixed problematic `kedro-datasets` optional dependencies, revert to `setup.py`

## Community contributions

# Release 1.5.2:

## Bug fixes and other changes

- Fixed problematic `kedro-datasets` optional dependencies.

# Release 1.5.1:

## Bug fixes and other changes

- Fixed problematic docstrings in `pandas.DeltaTableDataSet` causing Read the Docs builds on Kedro to fail.

# Release 1.5.0

## Major features and improvements

- Added the following new datasets:

| Type                       | Description                          | Location                |
| -------------------------- | ------------------------------------ | ----------------------- |
| `pandas.DeltaTableDataSet` | A dataset to work with delta tables. | `kedro_datasets.pandas` |

- Implemented lazy loading of dataset subpackages and classes.
  - Suppose that SQLAlchemy, a Python SQL toolkit, is installed in your Python environment. With this change, the SQLAlchemy library will not be loaded (for `pandas.SQLQueryDataSet` or `pandas.SQLTableDataSet`) if you load a different pandas dataset (e.g. `pandas.CSVDataSet`).
- Added automatic inference of file format for `pillow.ImageDataSet` to be passed to `save()`.

## Bug fixes and other changes

- Improved error messages for missing dataset dependencies.
  - Suppose that SQLAlchemy, a Python SQL toolkit, is not installed in your Python environment. Previously, `from kedro_datasets.pandas import SQLQueryDataSet` or `from kedro_datasets.pandas import SQLTableDataSet` would result in `ImportError: cannot import name 'SQLTableDataSet' from 'kedro_datasets.pandas'`. Now, the same imports raise the more helpful and intuitive `ModuleNotFoundError: No module named 'sqlalchemy'`.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [Daniel-Falk](https://github.com/daniel-falk)
- [afaqueahmad7117](https://github.com/afaqueahmad7117)
- [everdark](https://github.com/everdark)

# Release 1.4.2

## Bug fixes and other changes

- Fixed documentations of `GeoJSONDataSet` and `SparkStreamingDataSet`.
- Fixed problematic docstrings causing Read the Docs builds on Kedro to fail.

# Release 1.4.1:

## Bug fixes and other changes

- Fixed missing `pickle.PickleDataSet` extras in `setup.py`.

# Release 1.4.0:

## Major features and improvements

- Added the following new datasets:

| Type                          | Description                                         | Location               |
| ----------------------------- | --------------------------------------------------- | ---------------------- |
| `spark.SparkStreamingDataSet` | A dataset to work with PySpark Streaming DataFrame. | `kedro_datasets.spark` |

## Bug fixes and other changes

- Fixed problematic docstrings of `APIDataSet`.

# Release 1.3.0:

## Major features and improvements

- Added the following new datasets:

| Type                             | Description                                             | Location                    |
| -------------------------------- | ------------------------------------------------------- | --------------------------- |
| `databricks.ManagedTableDataSet` | A dataset to access managed delta tables in Databricks. | `kedro_datasets.databricks` |

- Added pandas 2.0 support.
- Added SQLAlchemy 2.0 support (and dropped support for versions below 1.4).
- Added a save method to `APIDataSet`.
- Reduced constructor arguments for `APIDataSet` by replacing most arguments with a single constructor argument `load_args`. This makes it more consistent with other Kedro DataSets and the underlying `requests` API, and automatically enables the full configuration domain: stream, certificates, proxies, and more.
- Relaxed Kedro version pin to `>=0.16`.
- Added `metadata` attribute to all existing datasets. This is ignored by Kedro, but may be consumed by users or external plugins.

## Bug fixes and other changes

- Relaxed `delta-spark` upper bound to allow compatibility with Spark 3.1.x and 3.2.x.
- Upgraded required `polars` version to 0.17.
- Renamed `TensorFlowModelDataset` to `TensorFlowModelDataSet` to be consistent with all other plugins in Kedro-Datasets.

## Community contributions

Many thanks to the following Kedroids for contributing PRs to this release:

- [BrianCechmanek](https://github.com/BrianCechmanek)
- [McDonnellJoseph](https://github.com/McDonnellJoseph)
- [Danny Farah](https://github.com/dannyrfar)

# Release 1.2.0:

## Major features and improvements

- Added `fsspec` resolution in `SparkDataSet` to support more filesystems.
- Added the `_preview` method to the Pandas `ExcelDataSet` and `CSVDataSet` classes.

## Bug fixes and other changes

- Fixed a docstring in the Pandas `SQLQueryDataSet` as part of the Sphinx revamp on Kedro.

# Release 1.1.1:

## Bug fixes and other changes

- Fixed problematic docstrings causing Read the Docs builds on Kedro to fail.

# Release 1.1.0:

## Major features and improvements

- Added the following new datasets:

| Type                             | Description                                                                                                           | Location                   |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `polars.CSVDataSet`              | A `CSVDataSet` backed by [polars](https://www.pola.rs/), a lighting fast dataframe package built entirely using Rust. | `kedro_datasets.polars`    |
| `snowflake.SnowparkTableDataSet` | Work with [Snowpark](https://www.snowflake.com/en/data-cloud/snowpark/) DataFrames from tables in Snowflake.          | `kedro_datasets.snowflake` |

## Bug fixes and other changes

- Add `mssql` backend to the `SQLQueryDataSet` DataSet using `pyodbc` library.
- Added a warning when the user tries to use `SparkDataSet` on Databricks without specifying a file path with the `/dbfs/` prefix.

# Release 1.0.2:

## Bug fixes and other changes

- Change reference to `kedro.pipeline.Pipeline` object throughout test suite with `kedro.modular_pipeline.pipeline` factory.
- Relaxed PyArrow range in line with pandas.
- Fixed outdated links to the dill package documentation.

# Release 1.0.1:

## Bug fixes and other changes

- Fixed docstring formatting in `VideoDataSet` that was causing the documentation builds to fail.

# Release 1.0.0:

First official release of Kedro-Datasets.

Datasets are Kedro’s way of dealing with input and output in a data and machine-learning pipeline. [Kedro supports numerous datasets](https://kedro.readthedocs.io/en/stable/kedro.extras.datasets.html) out of the box to allow you to process different data formats including Pandas, Plotly, Spark and more.

The datasets have always been part of the core Kedro Framework project inside `kedro.extras`. In Kedro `0.19.0`, we will remove datasets from Kedro to reduce breaking changes associated with dataset dependencies. Instead, users will need to use the datasets from the `kedro-datasets` repository instead.

## Major features and improvements

- Changed `pandas.ParquetDataSet` to load data using pandas instead of parquet.

# Release 0.1.0:

The initial release of Kedro-Datasets.

## Thanks to our main contributors

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.
