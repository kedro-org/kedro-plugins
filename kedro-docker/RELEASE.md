# Upcoming release

## Major features and improvements

## Bug fixes and other changes

## Breaking changes to the API

# Release 0.5.0
## Major features and improvements
* Add compatibility with `kedro` 0.19

## Breaking changes
* `kedro-docker` was updated to use the `0.19` version of the Kedro starters. If you need to use `kedro-docker` for an old project, you can either use an older version or move the requirements file up from `src` to the top-level project directory.

# Release 0.4.0
* Migrate all project metadata to static `pyproject.toml`.
* Removed support for Python 3.7

## Major features and improvements
* Added support for Python 3.11

# Release 0.3.1
## Major features and improvements
* Introduced two-stage builds with separated runtime environment and project code.
* Optimized image layers that store project code.
* Redundant Spark and Hadoop libs removed from Spark's template.

## Thanks for supporting contributions
[Mariusz Strzelecki](https://github.com/szczeles)

# Release 0.3.0
## Major features and improvements
* Add compatibility with `kedro` 0.18.0
* Add compatibility with Python 3.9 and 3.10
* Remove compatibility with Python 3.6

# Release 0.2.2

## Major features and improvements

## Bug fixes and other changes
* Removed deprecated `get_project_context()` calls.
* Made `kedro-docker` compatible with Kedro `0.17.0`.

## Breaking changes to the API
* Replaced deprecated `kedro.cli` imports with `kedro.framework.cli` (incompatible with Kedro `<0.16.0`).

## Thanks for supporting contributions

# Release 0.2.1

## Major features and improvements

* Added `--base-image` CLI option for `kedro docker build` command to create an image with the given base image.
* By default `kedro docker build` pulls Debian buster base image with Python version obtained from the current environment.
* Added `kedro docker init` command to generate `Dockerfile`, `.dockerignore` and `.dive-ci` files without building the image.

## Bug fixes and other changes
* Fixed `/home/kedro` directory permissions for containers run on Linux hosts.

## Breaking changes to the API

## Thanks for supporting contributions

[Adrian Piotr Kruszewski](https://github.com/akruszewski)

# Release 0.2.0

## Major features and improvements
* Dropped support for Python 3.5.
* Added support for Python 3.8.
* `kedro docker build` by default creates an image without Spark and Hadoop.

## Bug fixes and other changes

## Breaking changes to the API
* Added `--with-spark` CLI option for `kedro docker build` command to create an image with Spark and Hadoop.

## Thanks for supporting contributions

# Release 0.1.1

## Major features and improvements
* Added `kedro docker dive` CLI command to run [Dive](https://github.com/wagoodman/dive) analyzer.

## Bug fixes and other changes
* Added legal licence header checking as part of CircleCI.
* Removed smart quotes from the legal headers.

## Breaking changes to the API

## Thanks for supporting contributions

[Lucas Engels](https://github.com/yarncraft)

# Release 0.1.0:

The initial release of Kedro-Docker.

## Thanks to our main contributors

[Dmitrii Deriabin](https://github.com/dmder), [Nikolaos Tsaousis](https://github.com/tsanikgr), [Ivan Danov](https://github.com/idanov),  [Gordon Wrigley](https://github.com/tolomea), [Yetunde Dada](https://github.com/yetudada), [Nasef Khan](https://github.com/nakhan98), [Kiyohito Kunii](https://github.com/921kiyo), [Lorena Balan](https://github.com/lorenabalan), [Zain Patel](https://github.com/mzjp2), [Andrii Ivaniuk](https://github.com/andrii-ivaniuk)

We are also grateful to everyone who advised and supported us, filed issues or helped resolve them, asked and answered questions and were part of inspiring discussions.
