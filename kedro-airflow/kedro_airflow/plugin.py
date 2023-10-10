""" Kedro plugin for running a project with Airflow """
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

import click
import jinja2
from click import secho
from kedro.config import MissingConfigException
from kedro.framework.cli.project import PARAMS_ARG_HELP
from kedro.framework.cli.utils import ENV_HELP, KedroCliError, _split_params
from kedro.framework.context import KedroContext
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata, bootstrap_project
from slugify import slugify

PIPELINE_ARG_HELP = """Name of the registered pipeline to convert.
If not set, the '__default__' pipeline is used. This argument supports
passing multiple values using `--pipeline [p1] --pipeline [p2]`.
Use the `--all` flag to convert all registered pipelines at once."""
ALL_ARG_HELP = """Convert all registered pipelines at once."""


@click.group(name="Kedro-Airflow")
def commands():
    pass


@commands.group(name="airflow")
def airflow_commands():
    """Run project with Airflow"""
    pass


def _load_config(context: KedroContext) -> dict[str, Any]:
    # Set the default pattern for `airflow` if not provided in `settings.py`
    if "airflow" not in context.config_loader.config_patterns.keys():
        context.config_loader.config_patterns.update(  # pragma: no cover
            {"airflow": ["airflow*", "airflow/**"]}
        )

    assert "airflow" in context.config_loader.config_patterns.keys()

    # Load the config
    try:
        return context.config_loader["airflow"]
    except MissingConfigException:
        # File does not exist
        return {}


def _get_pipeline_config(config_airflow: dict, params: dict, pipeline_name: str):
    dag_config = {}
    # Load the default config if specified
    if "default" in config_airflow:
        dag_config.update(config_airflow["default"])
    # Update with pipeline-specific config if present
    if pipeline_name in config_airflow:
        dag_config.update(config_airflow[pipeline_name])

    # Update with params if provided
    dag_config.update(params)
    return dag_config


@airflow_commands.command()
@click.option(
    "-p",
    "--pipeline",
    "--pipelines",
    "pipeline_names",
    multiple=True,
    default=("__default__",),
    help=PIPELINE_ARG_HELP,
)
@click.option("--all", "convert_all", is_flag=True, help=ALL_ARG_HELP)
@click.option("-e", "--env", default="local", help=ENV_HELP)
@click.option(
    "-t",
    "--target-dir",
    "target_path",
    type=click.Path(writable=True, resolve_path=True, file_okay=False),
    default="./airflow_dags/",
    help="The directory path to store the generated Airflow dags",
)
@click.option(
    "-j",
    "--jinja-file",
    type=click.Path(
        exists=True, readable=True, resolve_path=True, file_okay=True, dir_okay=False
    ),
    default=Path(__file__).parent / "airflow_dag_template.j2",
    help="The template file for the generated Airflow dags",
)
@click.option(
    "--params",
    type=click.UNPROCESSED,
    default="",
    help=PARAMS_ARG_HELP,
    callback=_split_params,
)
@click.pass_obj
def create(  # noqa: PLR0913
    metadata: ProjectMetadata,
    pipeline_names,
    env,
    target_path,
    jinja_file,
    params,
    convert_all: bool,
):
    """Create an Airflow DAG for a project"""
    if convert_all and pipeline_names != ("__default__",):
        raise click.BadParameter(
            "The `--all` and `--pipeline` option are mutually exclusive."
        )

    project_path = Path.cwd().resolve()
    bootstrap_project(project_path)
    with KedroSession.create(project_path=project_path, env=env) as session:
        context = session.load_context()
        config_airflow = _load_config(context)

    jinja_file = Path(jinja_file).resolve()
    loader = jinja2.FileSystemLoader(jinja_file.parent)
    jinja_env = jinja2.Environment(autoescape=True, loader=loader, lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify
    template = jinja_env.get_template(jinja_file.name)

    dags_folder = Path(target_path)
    # Ensure that the DAGs folder exists
    dags_folder.mkdir(parents=True, exist_ok=True)
    secho(f"Location of the Airflow DAG folder: {target_path!s}", fg="green")

    package_name = metadata.package_name

    if convert_all:
        # Convert all pipelines
        conversion_pipelines = pipelines
    else:
        conversion_pipelines = {
            pipeline_name: pipelines.get(pipeline_name)
            for pipeline_name in pipeline_names
        }

    # Convert selected pipelines
    for name, pipeline in conversion_pipelines.items():
        dag_config = _get_pipeline_config(config_airflow, params, name)

        if pipeline is None:
            raise KedroCliError(f"Pipeline {name} not found.")

        # Obtain the file name
        dag_filename = dags_folder / (
            f"{package_name}_dag.py"
            if name == "__default__"
            else f"{package_name}_{name}_dag.py"
        )

        dependencies = defaultdict(list)
        for node, parent_nodes in pipeline.node_dependencies.items():
            for parent in parent_nodes:
                dependencies[parent].append(node)

        template.stream(
            dag_name=package_name,
            dependencies=dependencies,
            env=env,
            pipeline_name=name,
            package_name=package_name,
            pipeline=pipeline,
            **dag_config,
        ).dump(str(dag_filename))

        secho(
            f"Converted pipeline `{name}` to Airflow DAG in the file `{dag_filename.name}`",
            fg="green",
        )
