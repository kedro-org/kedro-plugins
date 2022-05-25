# Kedro-Telemetry!

[![Python Version](https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9%20%7C%203.10-blue.svg)](https://pypi.org/project/kedro-telemetry/)
[![PyPI version](https://badge.fury.io/py/kedro-telemetry.svg)](https://pypi.org/project/kedro-telemetry/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/ambv/black)

To find out how Kedro's features are used, the [Kedro team](https://github.com/kedro-org/kedro) have created the Kedro-Telemetry [plugin](https://kedro.readthedocs.io/en/stable/07_extend_kedro/04_plugins.html) to gather anonymised usage analytics. The data it gathers will help drive future development of Kedro because we can understand how the product is being used. Kedro-Telemetry uses [`pluggy`](https://pypi.org/project/pluggy/) hooks and [`requests`](https://pypi.org/project/requests/) to send data to [Heap Analytics](https://heap.io/).

## Privacy notice

### What about my personal data?

**McKinsey & Company Inc** (“McKinsey”, “we”) will process a minimal amount of personal data when you use **Kedro** with the Kedro-Telemetry plugin. McKinsey will only process such data under applicable data protection laws.

McKinsey will process your hashed hostname and hashed computer username, which both is pseudonymized information that indirectly relates to you personally. We use this data to better understand how users use **Kedro’s** features, involving a third-party analytics provider. The aim is to enable constant improvement to make **Kedro** as user friendly, effective, and precise as possible for you and other users. Furthermore, we compare your hashed hostname and hashed computer username with our internal list to understand the proportion of McKinsey users vs. open-source users. For EU users, we have implemented additional data protection safeguards, where these processing activities include a data transfer to a third country. **As an open-source user, please note that McKinsey or the third-party analytics provider will not, at any time, have the possibility to identify you personally behind the hashed hostname or hashed username (f. ex. your name, location, etc.). **As a McKinsey user, you, and the engagement you are using **Kedro** for will be identified in internal systems to evaluate McKinsey’s internal use of **Kedro**.

We base our processing activities on **your free consent**, and we will only store your personal data for so long as necessary for our analysis. **You can withdraw your consent at any time.** How to withdraw consent is explained below.

You have furthermore certain data subject rights under applicable laws (incl. right to access, erasure, restriction). However, as an open-source user, please note that answering to these would require us to collect and process additional personal data and identify you personally only for this reason. Depending on your location, you may also have the right to lodge a complaint with the competent supervisory authority. If you have any questions, want to learn more about this Notice or exercise your rights, or if you would like to communicate with our EU Data Protection Officer or the Data Privacy Team, please contact us at: privacy@mckinsey.com.

### What other information do you collect?

Besides the hashed host and username, we collect the following project-related information. Again, we rely on your consent to do so:

|Description|Example Input|What we receive|
|-|-|-|
|CLI command (masked arguments)|`kedro run --pipeline=ds --env=test`|`kedro run --pipeline ***** --env *****`|
|_(Hashed)_ Package name|my-project|1c7cd944c28cd888904f3efc2345198507...|
|_(Hashed)_ Project name|my_project|a6392d359362dc9827cf8688c9d634520e...|
|`kedro` project version|0.17.6|0.17.6|
|`kedro-telemetry` version|0.1.2|0.1.2|
|Python version|3.8.10 (default, Jun  2 2021, 10:49:15)|3.8.10 (default, Jun  2 2021, 10:49:15)|
|Operating system used|darwin|darwin|

### How do I consent to the use of Kedro-Telemetry?

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

### How do I withdraw consent?

To withdraw consent, you can change the `consent` variable to `false` in `.telemetry` YAML by editing the file in the following way:

```yaml
consent: false
```

Or you can uninstall the plugin:

```console
pip uninstall kedro-telemetry
```

### What happens when I have denied or withdrawn consent?

Data will only be collected if consent is given. Otherwise, if consent was explicitly denied or withdrawn, the message below will be printed out on every Kedro CLI invocation. If you explicitly deny consent from the beginning, no data will be collected. If you withdraw consent later, the processing of data will be stopped from that moment on.

```
Kedro-Telemetry is installed, but you have opted out of sharing usage analytics so none will be collected.
```

## What licence do you use?

Kedro-Telemetry is licensed under the [Apache 2.0](https://github.com/kedro-org/kedro-plugins/blob/main/LICENSE.md) License.
