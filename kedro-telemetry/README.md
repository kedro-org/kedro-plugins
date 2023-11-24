# Kedro-Telemetry

[![Python Version](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue.svg)](https://pypi.org/project/kedro-telemetry/)
[![PyPI version](https://badge.fury.io/py/kedro-telemetry.svg)](https://pypi.org/project/kedro-telemetry/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/ambv/black)

## Introduction

To find out how Kedro's features are used, the [Kedro team](https://github.com/kedro-org/kedro) have created an opt-in Kedro-Telemetry [plugin](https://kedro.readthedocs.io/en/stable/07_extend_kedro/04_plugins.html) to gather anonymised (before being sent across the internet) and aggregated usage analytics.

The data it gathers will help drive future development of Kedro because we can understand how the product is being used.

## Why is my data being collected?

Telemetry data is made available to [project maintainers of the Kedro Project](https://kedro.readthedocs.io/en/stable/faq/faq.html#who-maintains-kedro). The purpose of providing this information is to provide metrics to the maintainers on how Kedro is used. This data helps inform future product development because we can better understand if certain features are having traction with our users. We want to make sure that Kedro is as streamlined as possible and it is difficult to do this without usage analytics.


## What about my personal data?

The Kedro Project’s telemetry has been reviewed and approved under the [Telemetry Data Collection and Usage Policy](https://lfprojects.org/policies/telemetry-data-policy/) of LF Projects, LLC.


## What data is being collected?

We process your hashed hostname and hashed computer username, which both is pseudonymized information that indirectly relates to you personally. Besides the hashed host and username, we collect the following project-related information. Again, we rely on your consent to do so:

|Description|Example Input|What we receive|
|-|-|-|
|CLI command (masked arguments)|`kedro run --pipeline=ds --env=test`|`kedro run --pipeline ***** --env *****`|
|_(Hashed)_ Package name|my-project|1c7cd944c28cd888904f3efc2345198507...|
|_(Hashed)_ Project name|my_project|a6392d359362dc9827cf8688c9d634520e...|
|`kedro` project version|0.17.6|0.17.6|
|`kedro-telemetry` version|0.1.2|0.1.2|
|Python version|3.8.10 (default, Jun  2 2021, 10:49:15)|3.8.10 (default, Jun  2 2021, 10:49:15)|
|Operating system used|darwin|darwin|

## How do I consent to the use of Kedro-Telemetry?

Kedro-Telemetry is a Python plugin. To install it:

```console
pip install kedro-telemetry
```

> _Note:_ If you are using an official [Kedro project template](https://kedro.readthedocs.io/en/stable/02_get_started/06_starters.html) then `kedro-telemetry` is included in the [project-level `requirements.txt`](https://kedro.readthedocs.io/en/stable/04_kedro_project_setup/01_dependencies.html#kedro-install) of the starter. `kedro-telemetry` is activated after you have a created a new project with a [Kedro project template](https://kedro.readthedocs.io/en/stable/02_get_started/06_starters.html) and have run `kedro install` from the terminal.

When you next run the Kedro CLI you will be asked for consent to share usage analytics data for the purposes explained in the privacy notice, and a `.telemetry` YAML file will be created in the project root directory. The variable `consent` will be set according to your choice in the file, e.g. if you consent:

```yaml
consent: true
```

>*Note:* The `.telemetry` file should not be committed to `git` or packaged in deployment. In `kedro>=0.17.4` the file is git-ignored.

## How do I withdraw consent?

To withdraw consent, you can change the `consent` variable to `false` in `.telemetry` YAML by editing the file in the following way:

```yaml
consent: false
```

Or you can uninstall the plugin:

```console
pip uninstall kedro-telemetry
```

## What happens when I withdraw consent?

Data will only be collected if consent is given. Otherwise, if consent was explicitly denied or withdrawn, the message below will be printed out on every Kedro CLI invocation. If you explicitly deny consent from the beginning, no data will be collected. If you withdraw consent later, the processing of data will be stopped from that moment on.

```
Kedro-Telemetry is installed, but you have opted out of sharing usage analytics so none will be collected.
```

## How is the data collected

Kedro-Telemetry uses [`pluggy`](https://pypi.org/project/pluggy/) hooks and [`requests`](https://pypi.org/project/requests/) to send data to [Heap Analytics](https://heap.io/). Project maintainers have access to the data and can create dashboards that show adoption and feature usage.

## What licence do you use?

Kedro-Telemetry is licensed under the [Apache 2.0](https://github.com/kedro-org/kedro-plugins/blob/main/LICENSE.md) License.

## Python version support policy

* The [Kedro-Telemetry](https://github.com/kedro-org/kedro-plugins/tree/main/kedro-telemetry) supports all Python versions that are actively maintained by the CPython core team. When a [Python version reaches end of life](https://devguide.python.org/versions/#versions), support for that version is dropped from `kedro-telemetry`. This is not considered a breaking change.
