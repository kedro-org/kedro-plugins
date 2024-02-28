# Upcoming Release
* Sort DAGs to make sure `kedro airflow create` is deterministic.
* Option to group MemoryDatasets in the same Airflow task (breaking change for custom template via `--jinja-file`).
* Include the environment name in the DAG file name when different from the default.

# Release 0.8.0
* Added support for Kedro 0.19.x

# Release 0.7.0
* Added support for Python 3.11
* Added the `--all` CLI argument to `kedro-airflow` to convert registered all pipelines at once.
* Simplify the output of the `kedro airflow create` command.
* Fixed compatibility of `kedro-airflow` with older versions of the config loaders (`kedro<=0.18.2`).
* Removed support for Python 3.7

## Community contributions
Many thanks to the following Kedroids for contributing PRs to this release:

* [sbrugman](https://github.com/sbrugman)

# Release 0.6.0
* Change reference to `kedro.pipeline.Pipeline` object throughout test suite with `kedro.modular_pipeline.pipeline` factory.
* Migrate all project metadata to static `pyproject.toml`.
* Configure DAG kwargs via `airflow.yml`.
* The generated DAG file now contains the pipeline name.
* Included help for CLI arguments (see `kedro airflow create --help`).
* Added additional CLI argument `--params` to pass configuration to the Jinja2 template.

## Community contributions
Many thanks to the following Kedroids for contributing PRs to this release:

* [sbrugman](https://github.com/sbrugman)

# Release 0.5.1
* Added additional CLI argument `--jinja-file` to provide a path to a custom Jinja2 template.
* Added support for Airflow>=2.3.0.
* Kept Kedro-Airflow plugin docstring from appearing in `kedro -h`.
* Prefixed Airflow plugin name with "Kedro-" in usage message

# Release 0.5.0
* Added support for Kedro 0.18.
* Added support for Python 3.9 and 3.10.
* Removed compatibility with Python 3.6.

# Release 0.4.2
* Dropped context/session usage to fetch pipelines to support Kedro>=0.17.5.

# Release 0.4.1

## Bugfix
* Dropped unnecessary dependency on `apache-airflow`.

## Thanks for supporting contributions

# Release 0.4.0

## Major features and improvements
* Added support for Python 3.8.
* Added support for Kedro 0.17.

## Thanks for supporting contributions

# Release 0.3.0

## Major features and improvements
* Dropped support for Python 3.5.
* Changed default DAG schedule interval to `None`.
* Changed default DAG catchup to be `False`.
* Fixed a bug that was logging unnecessary Kedro messages when running Airflow commands.
* Fixed a bug for processing Airflow context in DAG template.

## Thanks for supporting contributions
[@pitterb](https://github.com/pitterb)

# Release 0.2.2

## Major features and improvements
* Fixed a bug deploying when the Airflow directory does not exist.

# Release 0.2.1

## Major features and improvements
* Fixed installation issue due to dependency version conflict.

# Release 0.2.0

## Major features and improvements
* Added support for Kedro 0.15.

# Release 0.1.0:

The initial release of Kedro-Airflow.

## Thanks to our main contributors

[Gordon Wrigley](https://github.com/tolomea), [Nasef Khan](https://github.com/nakhan98), [Dmitrii Deriabin](https://github.com/DmitryDeryabin), [Yetunde Dada](https://github.com/yetudada), [Jo Stichbury](https://github.com/stichbury), [Kiyohito Kunii](https://github.com/921kiyo), [Anton Kirilenko](https://github.com/Flid)

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.
