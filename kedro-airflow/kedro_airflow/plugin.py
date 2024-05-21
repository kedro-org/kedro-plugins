""" Kedro plugin for running a project with Airflow """

from __future__ import annotations

from pathlib import Path
from typing import Any

import click
import jinja2
from click import secho
from kedro.config import MissingConfigException
from kedro.framework.cli.project import PARAMS_ARG_HELP
from kedro.framework.cli.utils import (
    ENV_HELP,
    KedroCliError,
    _split_params,
    split_string,
)
from kedro.framework.context import KedroContext
from kedro.framework.project import pipelines
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata
from slugify import slugify

from kedro_airflow.grouping import group_memory_nodes

PIPELINE_ARG_HELP = """Name of the registered pipeline to convert.
If not set, the '__default__' pipeline is used. This argument supports
passing multiple values using `--pipeline [p1] --pipeline [p2]`.
Use the `--all` flag to convert all registered pipelines at once."""
ALL_ARG_HELP = """Convert all registered pipelines at once."""
TAGS_ARG_HELP = """Tags to be used for filtering pipeline nodes.
Multiple tags are supported. Use the following format:
`--tags tag1,tag2`."""
DEFAULT_RUN_ENV = "local"
DEFAULT_PIPELINE = "__default__"


@click.group(name="Kedro-Airflow")
def commands():
    pass


@commands.group(name="airflow")
def airflow_commands():
    """Run project with Airflow"""
    pass


def _load_config(context: KedroContext) -> dict[str, Any]:
    # Backwards compatibility for ConfigLoader that does not support `config_patterns`
    config_loader = context.config_loader
    if not hasattr(config_loader, "config_patterns"):
        return config_loader.get("airflow*", "airflow/**")  # pragma: no cover

    # Set the default pattern for `airflow` if not provided in `settings.py`
    if "airflow" not in config_loader.config_patterns.keys():
        config_loader.config_patterns.update(  # pragma: no cover
            {"airflow": ["airflow*", "airflow/**"]}
        )

    assert "airflow" in config_loader.config_patterns.keys()

    # Load the config
    try:
        return config_loader["airflow"]
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
    default=(DEFAULT_PIPELINE,),
    help=PIPELINE_ARG_HELP,
)
@click.option("--all", "convert_all", is_flag=True, help=ALL_ARG_HELP)
@click.option("-e", "--env", default=DEFAULT_RUN_ENV, help=ENV_HELP)
@click.option(
    "-t",
    "--target-dir",
    "target_path",
    type=click.Path(writable=True, resolve_path=False, file_okay=False),
    default="airflow_dags/",
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
    "-g",
    "--group-in-memory",
    is_flag=True,
    default=False,
    help="Group nodes with at least one MemoryDataset as input/output together, "
    "as they do not persist between Airflow operators.",
)
@click.option(
    "--tags",
    type=str,
    default="",
    help=TAGS_ARG_HELP,
    callback=split_string,
)
@click.option(
    "--params",
    type=click.UNPROCESSED,
    default="",
    help=PARAMS_ARG_HELP,
    callback=_split_params,
)
@click.pass_obj
def create(  # noqa: PLR0913, PLR0912
    metadata: ProjectMetadata,
    pipeline_names,
    env,
    target_path,
    jinja_file,
    group_in_memory,
    tags,
    params,
    convert_all: bool,
):
    """Create an Airflow DAG for a project"""
    if convert_all and pipeline_names != (DEFAULT_PIPELINE,):
        raise click.BadParameter(
            "The `--all` and `--pipeline` option are mutually exclusive."
        )
    with KedroSession.create(project_path=metadata.project_path, env=env) as session:
        context = session.load_context()
        config_airflow = _load_config(context)

    jinja_file = Path(jinja_file).resolve()
    loader = jinja2.FileSystemLoader(jinja_file.parent)
    jinja_env = jinja2.Environment(autoescape=True, loader=loader, lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify
    template = jinja_env.get_template(jinja_file.name)

    dags_folder = (
        Path(target_path)
        if Path(target_path).is_absolute()
        else metadata.project_path / Path(target_path)
    )

    # Ensure that the DAGs folder exists
    dags_folder.mkdir(parents=True, exist_ok=True)
    secho(f"Location of the Airflow DAG folder: {dags_folder!s}", fg="green")

    package_name = metadata.package_name

    if convert_all:
        # Convert all pipelines
        conversion_pipelines = dict(pipelines)
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
        dag_name = package_name
        if env != DEFAULT_RUN_ENV:
            dag_name += f"_{env}"
        if name != DEFAULT_PIPELINE:
            dag_name += f"_{name}"
        dag_name += "_dag.py"
        dag_filename = dags_folder / dag_name

        if tags:
            pipeline = pipeline.only_nodes_with_tags(*tags)  # noqa: PLW2901

        # Group memory nodes
        if group_in_memory:
            # The order of nodes and dependencies is deterministic and based on the
            # topological sort order obtained from pipeline.nodes, see group_memory_nodes()
            # implementation
            nodes, dependencies = group_memory_nodes(context.catalog, pipeline)
        else:
            # To keep the order of nodes and dependencies deterministic - nodes are
            # iterated in the topological sort order obtained from pipeline.nodes and
            # appended to the corresponding dictionaries
            nodes = {}
            dependencies = {}
            for node in pipeline.nodes:
                nodes[node.name] = [node]
                dependencies[node.name] = []
            for node, parent_nodes in pipeline.node_dependencies.items():
                for parent in parent_nodes:
                    dependencies[parent.name].append(node.name)

        template.stream(
            dag_name=package_name,
            nodes=nodes,
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
