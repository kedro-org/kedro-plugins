# Kedro-Airflow

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue.svg)](https://pypi.org/project/kedro-airflow/)
[![PyPI Version](https://badge.fury.io/py/kedro-airflow.svg)](https://pypi.org/project/kedro-airflow/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/ambv/black)

[Apache Airflow](https://github.com/apache/airflow) is a tool for orchestrating complex workflows and data processing pipelines. The Kedro-Airflow plugin can be used for:
- Rapid pipeline creation in the prototyping phase. You can write Python functions in Kedro without worrying about schedulers, daemons, services or having to recreate the Airflow DAG file.
- Automatic dependency resolution in Kedro. This allows you to bypass Airflow's need to specify the order of your tasks.
- Distributing Kedro tasks across many workers. You can also enable monitoring and scheduling of the tasks' runtimes.

## Installation

`kedro-airflow` is a Python plugin. To install it:

```bash
pip install kedro-airflow
```

## Usage

You can use `kedro-airflow` to deploy a Kedro pipeline as an Airflow DAG by following these steps:

### Step 1: Generate the DAG file

At the root directory of the Kedro project, run:

```bash
kedro airflow create
```

This command will generate an Airflow DAG file located in the `airflow_dags/` directory in your project.
You can pass a `--pipeline` flag to generate the DAG file for a specific Kedro pipeline and an `--env` flag to generate the DAG file for a specific Kedro environment.
Passing `--all` will convert all registered Kedro pipelines to Airflow DAGs.

### Step 2: Copy the DAG file to the Airflow DAGs folder.

For more information about the DAGs folder, please visit [Airflow documentation](https://airflow.apache.org/docs/stable/concepts.html#dags).
The Airflow DAG configuration can be customized by editing this file.

### Step 3: Package and install the Kedro pipeline in the Airflow executor's environment

After generating and deploying the DAG file, you will then need to package and install the Kedro pipeline into the Airflow executor's environment.
Please visit the guide to [deploy Kedro as a Python package](https://docs.kedro.org/en/stable/deployment/single_machine.html#package-based) for more details.

### FAQ

#### What if my DAG file is in a different directory to my project folder?

By default, the generated DAG file is configured to live in the same directory as your project as per this [template](https://github.com/kedro-org/kedro-plugins/blob/main/kedro-airflow/kedro_airflow/airflow_dag_template.j2#L44). If your DAG file is located in a different directory to your project, you will need to tweak this  manually after running the `kedro airflow create` command.

#### What if I want to use a different Jinja2 template?

You can use the additional command line argument `--jinja-file` (alias `-j`) to provide an alternative path to a Jinja2 template. Note that these files have to accept the same variables as those used in the [default Jinja2 template](https://github.com/kedro-org/kedro-plugins/blob/main/kedro-airflow/kedro_airflow/airflow_dag_template.j2).

```bash
kedro airflow create --jinja-file=./custom/template.j2
```

#### How can I pass arguments to the Airflow DAGs dynamically?

`kedro-airflow` picks up configuration from `airflow.yml` in `conf/base` or `conf/local` of your Kedro project.
Or it could be in a folder starting with `airflow`.
The [parameters](https://docs.kedro.org/en/stable/configuration/parameters.html) are read by Kedro.
Arguments can be specified globally, or per pipeline:

```yaml
# Global parameters
default:
    start_date: [2023, 1, 1]
    max_active_runs: 3
    # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    schedule_interval: "@once"
    catchup: false
    # Default settings applied to all tasks
    owner: "airflow"
    depends_on_past: false
    email_on_failure: false
    email_on_retry: false
    retries: 1
    retry_delay: 5

# Arguments specific to the pipeline (overrides the parameters above)
data_science:
    owner: "airflow-ds"
```

Arguments can also be passed via `--params` in the command line:

```bash
kedro airflow create --params "schedule_interval='@weekly'"
```

These variables are passed to the Jinja2 template that creates an Airflow DAG from your pipeline.

### What if I want to use a configuration pattern other than `airflow*` and `airflow**`?

In order to configure the config loader, update the `settings.py` file in your Kedro project.
For instance, if you would like to use the name `scheduler`, then change the file as follows:

```python
CONFIG_LOADER_ARGS = {"config_patterns": {"airflow": ["scheduler*", "scheduler/**"]}}
```

Follow Kedro's [official documentation](https://docs.kedro.org/en/stable/configuration/advanced_configuration.html#how-to-do-templating-with-the-omegaconfigloader), to see how to add templating, custom resolvers etc.

#### What if I want to pass different arguments?

In order to pass arguments other than those specified in the default template, simply pass a custom template (see: _"What if I want to use a different Jinja2 template?"_)

The syntax for arguments is:
```
{{ argument_name }}
```

In order to make arguments optional, one can use:
```
{{ argument_name | default("default_value") }}
```

For examples, please have a look at the default template (`airflow_dag_template.j2`).

### What if I want to use a configuration file other than `airflow.yml`?

The default configuration pattern is `["airflow*", "airflow/**"]`.
In order to configure the `OmegaConfigLoader`, update the `settings.py` file in your Kedro project as follows:

```python
from kedro.config import OmegaConfigLoader

CONFIG_LOADER_CLASS = OmegaConfigLoader
CONFIG_LOADER_ARGS = {
    # other args
    "config_patterns": {  # configure the pattern for configuration files
        "airflow": ["airflow*", "airflow/**"]
    }
}
```

Follow Kedro's official documentation, to see how to add templating, custom resolvers etc. (https://docs.kedro.org/en/stable/configuration/advanced_configuration.html#how-to-do-templating-with-the-omegaconfigloader)[https://docs.kedro.org/en/stable/configuration/advanced_configuration.html#how-to-do-templating-with-the-omegaconfigloader]

#### How can I use Airflow runtime parameters?

It is possible to pass parameters when triggering an Airflow DAG from the user interface.
In order to use this feature, create a custom template using the [Params syntax](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html).
See ["What if I want to use a different Jinja2 template?"](#what-if-i-want-to-use-a-different-jinja2-template) for instructions on using custom templates.

#### What if I want to use a different Airflow Operator?

Which Airflow Operator to use depends on the environment your project is running in.
You can set the operator to use by providing a custom template.
See ["What if I want to use a different Jinja2 template?"](#what-if-i-want-to-use-a-different-jinja2-template) for instructions on using custom templates.
The [rich offering](https://airflow.apache.org/docs/apache-airflow-providers/operators-and-hooks-ref/index.html) of operators means that the `kedro-airflow` plugin is providing templates for specific operators.
The default template provided by `kedro-airflow` uses the `BaseOperator`.

### Can I group nodes together?

When running Kedro nodes using Airflow, MemoryDatasets are often not shared across operators.
This will cause the DAG run to fail.

MemoryDatasets may be used to provide logical separation between nodes in Kedro, without the overhead of needing to write to disk (and in the case of distributed running needing multiple executors).

Nodes that are connected through MemoryDatasets are grouped together via the `--group-in-memory` flag.
This preserves the option to have logical separation in Kedro, with little computational overhead.

It is possible to use [task groups](https://docs.astronomer.io/learn/task-groups) by changing the template.
See ["What if I want to use a different Jinja2 template?"](#what-if-i-want-to-use-a-different-jinja2-template) for instructions on using custom templates.

## Can I contribute?

Yes! Want to help build Kedro-Airflow? Check out our guide to [contributing](https://github.com/kedro-org/kedro-plugins/blob/main/kedro-airflow/CONTRIBUTING.md).

## What licence do you use?

Kedro-Airflow is licensed under the [Apache 2.0](https://github.com/kedro-org/kedro-plugins/blob/main/LICENSE.md) License.

## Python version support policy
* The [Kedro-Airflow](https://github.com/kedro-org/kedro-plugins/tree/main/kedro-airflow) supports all Python versions that are actively maintained by the CPython core team. When a [Python version reaches end of life](https://devguide.python.org/versions/#versions), support for that version is dropped from `kedro-airflow`. This is not considered a breaking change.
