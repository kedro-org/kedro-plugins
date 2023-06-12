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
If not set, the '__default__' pipeline is used."""


@click.group(name="Kedro-Airflow")
def commands():  # pylint: disable=missing-function-docstring
    pass


@commands.group(name="airflow")
def airflow_commands():
    """Run project with Airflow"""
    pass


def _load_config(context: KedroContext, pipeline_name: str) -> dict[str, Any]:
    # Set the default pattern for `airflow` if not provided in `settings.py`
    if "airflow" not in context.config_loader.config_patterns.keys():
        context.config_loader.config_patterns.update(  # pragma: no cover
            {"airflow": ["airflow*", "airflow/**"]}
        )

    assert "airflow" in context.config_loader.config_patterns.keys()

    # Load the config
    try:
        config_airflow = context.config_loader["airflow"]
    except MissingConfigException:
        # File does not exist
        return {}

    dag_config = {}
    # Load the default config if specified
    if "default" in config_airflow:
        dag_config.update(config_airflow["default"])
    # Update with pipeline-specific config if present
    if pipeline_name in config_airflow:
        dag_config.update(config_airflow[pipeline_name])
    return dag_config


@airflow_commands.command()
@click.option(
    "-p", "--pipeline", "pipeline_name", default="__default__", help=PIPELINE_ARG_HELP
)
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
def create(
    metadata: ProjectMetadata,
    pipeline_name,
    env,
    target_path,
    jinja_file,
    params,
):  # pylint: disable=too-many-locals,too-many-arguments
    """Create an Airflow DAG for a project"""
    project_path = Path.cwd().resolve()
    bootstrap_project(project_path)
    with KedroSession.create(project_path=project_path, env=env) as session:
        context = session.load_context()
        dag_config = _load_config(context, pipeline_name)

        # Update with params if provided
        dag_config.update(params)

    jinja_file = Path(jinja_file).resolve()
    loader = jinja2.FileSystemLoader(jinja_file.parent)
    jinja_env = jinja2.Environment(autoescape=True, loader=loader, lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify
    template = jinja_env.get_template(jinja_file.name)

    package_name = metadata.package_name
    dag_filename = f"{package_name}_{pipeline_name}_dag.py"

    target_path = Path(target_path)
    target_path = target_path / dag_filename

    target_path.parent.mkdir(parents=True, exist_ok=True)

    pipeline = pipelines.get(pipeline_name)
    if pipeline is None:
        raise KedroCliError(f"Pipeline {pipeline_name} not found.")

    dependencies = defaultdict(list)
    for node, parent_nodes in pipeline.node_dependencies.items():
        for parent in parent_nodes:
            dependencies[parent].append(node)

    template.stream(
        dag_name=package_name,
        dependencies=dependencies,
        env=env,
        pipeline_name=pipeline_name,
        package_name=package_name,
        pipeline=pipeline,
        **dag_config,
    ).dump(str(target_path))

    secho("")
    secho("An Airflow DAG has been generated in:", fg="green")
    secho(str(target_path))
    secho("This file should be copied to your Airflow DAG folder.", fg="yellow")
    secho(
        "The Airflow configuration can be customized by editing this file.",
        fg="green",
    )
    secho("")
    secho(
        "This file also contains the path to the config directory, this directory will need to "
        "be available to Airflow and any workers.",
        fg="yellow",
    )
    secho("")
    secho(
        "Additionally all data sets must have an entry in the data catalog.",
        fg="yellow",
    )
    secho(
        "And all local paths in both the data catalog and log config must be absolute paths.",
        fg="yellow",
    )
