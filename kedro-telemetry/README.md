# Kedro-Telemetry

[![Python Version](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue.svg)](https://pypi.org/project/kedro-telemetry/)
[![PyPI version](https://badge.fury.io/py/kedro-telemetry.svg)](https://pypi.org/project/kedro-telemetry/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/ambv/black)

## What is kedro-telemetry?

Kedro-Telemetry is a [plugin](https://docs.kedro.org/en/stable/extend_kedro/plugins.html)
that gathers anonymised and aggregated usage analytics
to help [the Kedro team](https://docs.kedro.org/en/stable/contribution/technical_steering_committee.html)
understand how Kedro is used and prioritise improvements to the product accordingly.

## What data is being collected?

Read [our Telemetry documentation](https://docs.kedro.org/en/stable/configuration/telemetry.html)
for further information on the intent of the data collection and what data is collected.

For technical information on how the telemetry collection works, you can browse
[the source code of `kedro-telemetry`](https://github.com/kedro-org/kedro-plugins/tree/main/kedro-telemetry).

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
