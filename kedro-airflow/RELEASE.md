# Release 0.5.0
* Add compatibility with `kedro` 0.18.0
* Add compatibility with Python 3.9 and 3.10
* Remove compatibility with Python 3.6

# Release 0.4.2
* Drop context/session usage to fetch pipelines to support Kedro>=0.17.5.

# Release 0.4.1

## Bugfix
* Drop unnecessary dependency on apache-airflow

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
* Fix a bug deploying when the Airflow directory does not exist.

# Release 0.2.1

## Major features and improvements
* Fix installation issue due to dependency version conflict.


# Release 0.2.0

## Major features and improvements
* Compatibility with kedro==0.15.0

# Release 0.1.0:

The initial release of Kedro-Airflow.

## Thanks to our main contributors

[Gordon Wrigley](https://github.com/tolomea), [Nasef Khan](https://github.com/nakhan98), [Dmitrii Deriabin](https://github.com/DmitryDeryabin), [Yetunde Dada](https://github.com/yetudada), [Jo Stichbury](https://github.com/stichbury), [Kiyohito Kunii](https://github.com/921kiyo), [Anton Kirilenko](https://github.com/Flid)

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.
