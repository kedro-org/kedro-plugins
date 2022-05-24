"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""
from pathlib import Path
from platform import system
from shutil import copyfile

from click.testing import CliRunner
from kedro import __version__ as kedro_version
from kedro.framework.startup import ProjectMetadata
from pytest import fixture


@fixture(name="cli_runner")
def cli_runner():
    runner = CliRunner()
    owd = Path.cwd()
    with runner.isolated_filesystem():
        if system() == "Windows":
            fp = owd / "kedro_airflow/kedro_airflow/airflow_dag_template.j2"
        else:
            fp = owd / "kedro_airflow/airflow_dag_template.j2"
        copyfile(fp.resolve(), Path("./airflow_dag.j2").resolve())
        yield runner


@fixture
def metadata(cli_runner):  # pylint: disable=unused-argument
    # cwd() depends on ^ the isolated filesystem, created by CliRunner()
    project_path = Path.cwd()
    return ProjectMetadata(
        project_path / "pyproject.toml",
        "hello_world",
        "Hello world !!!",
        project_path,
        kedro_version,
        project_path / "src",
    )
