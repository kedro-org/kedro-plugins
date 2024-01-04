"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""
from __future__ import annotations

import os
from pathlib import Path
from shutil import copyfile

from click.testing import CliRunner
from kedro import __version__ as kedro_version
from kedro.framework.cli.starters import create_cli as kedro_cli
from kedro.framework.startup import ProjectMetadata
from pytest import fixture


@fixture(name="cli_runner", scope="session")
def cli_runner():
    runner = CliRunner()
    cwd = Path.cwd()
    with runner.isolated_filesystem():
        fp = cwd / "kedro_airflow/airflow_dag_template.j2"
        copyfile(fp.resolve(), Path("./airflow_dag.j2").resolve())
        yield runner


def _create_kedro_settings_py(file_name: Path, patterns: list[str]):
    patterns = ", ".join([f'"{p}"' for p in patterns])
    content = f"""CONFIG_LOADER_ARGS = {{
    "base_env": "base",
    "default_run_env": "local",
    "config_patterns": {{
        "airflow": [{patterns}],  # configure the pattern for configuration files
    }}
}}
"""
    file_name.write_text(content)


@fixture(scope="session")
def kedro_project(cli_runner):
    CliRunner().invoke(
        # Supply name, tools, and example to skip interactive prompts
        kedro_cli,
        [
            "new",
            "-v",
            "--name",
            "Fake Project",
            "--tools",
            "none",
            "--example",
            "no",
        ],
    )
    pipeline_registry_py = """
from kedro.pipeline import Pipeline, node


def identity(arg):
    return arg


def register_pipelines():
    pipeline = Pipeline(
        [
            node(identity, ["input"], ["intermediate"], name="node0"),
            node(identity, ["intermediate"], ["output"], name="node1"),
        ],
        tags="pipeline0",
    )
    return {
        "__default__": pipeline,
        "ds": pipeline,
    }
    """

    project_path = Path().cwd() / "fake-project"
    (project_path / "src" / "fake_project" / "pipeline_registry.py").write_text(
        pipeline_registry_py
    )

    settings_file = project_path / "src" / "fake_project" / "settings.py"
    _create_kedro_settings_py(
        settings_file, ["airflow*", "airflow/**", "scheduler*", "scheduler/**"]
    )

    os.chdir(project_path)
    return project_path


@fixture(scope="session")
def metadata(kedro_project):
    # cwd() depends on ^ the isolated filesystem, created by CliRunner()
    project_path = kedro_project
    return ProjectMetadata(
        source_dir=project_path / "src",
        config_file=project_path / "pyproject.toml",
        package_name="hello_world",
        project_name="Hello world !!!",
        kedro_init_version=kedro_version,
        project_path=project_path,
        tools=["None"],
        example_pipeline="No",
    )
