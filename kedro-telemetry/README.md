# Kedro-Telemetry

[![Python Version](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://pypi.org/project/kedro-telemetry/)
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

## How is the data collected

Kedro-Telemetry uses [`pluggy`](https://pypi.org/project/pluggy/) hooks and [`requests`](https://pypi.org/project/requests/) to send data to [Heap Analytics](https://heap.io/). Project maintainers have access to the data and can create dashboards that show adoption and feature usage.

## How do I withdraw consent?

Data collection for telemetry is enabled by default. To withdraw consent, you have a few options:

1. **Set Environment Variables**:
   Set the environment variables `DO_NOT_TRACK` or `KEDRO_DISABLE_TELEMETRY` to any value. The presence of any of these environment variables will disable telemetry for all Kedro projects in that environment and will override any consent specified in the `.telemetry` file of the specific project.

2. **CLI Option When Creating a New Project**:
   When creating a new project, you can use the command:

   ```console
   kedro new --telemetry=no
   ```
   This will create a new project with a `.telemetry` file in its root folder, containing `consent: false`. This file will be used when executing Kedro commands within that project folder. Note that telemetry data about the execution of the `kedro new` command will still be sent if telemetry has not been disabled using environment variables.

   >*Note:* The `.telemetry` file should not be committed to `git` or packaged in deployment. In `kedro>=0.17.4` the file is git-ignored.

3. **Modify or Create the `.telemetry` file manually**:
   If the `.telemetry` file exists in the root folder of your Kedro project, set the `consent` variable to `false`. If the file does not exist, create it with the following content:
     ```yaml
     consent: false
     ```

4. **Uninstall the plugin**:
   Remove the `kedro-telemetry` plugin:

   ```console
   pip uninstall kedro-telemetry
   ```
   >*Note:* This is a last resort option, as it will break the dependencies of Kedro (for example, `pip check` will report issues).

## What happens when I withdraw consent?

If you explicitly deny consent from the beginning, no data will be collected. If you withdraw consent later, the processing of data will be stopped from that moment on.

## What licence do you use?

Kedro-Telemetry is licensed under the [Apache 2.0](https://github.com/kedro-org/kedro-plugins/blob/main/LICENSE.md) License.

## Python version support policy

* The [Kedro-Telemetry](https://github.com/kedro-org/kedro-plugins/tree/main/kedro-telemetry) supports all Python versions that are actively maintained by the CPython core team. When a [Python version reaches end of life](https://devguide.python.org/versions/#versions), support for that version is dropped from `kedro-telemetry`. This is not considered a breaking change.
