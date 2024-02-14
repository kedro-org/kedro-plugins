from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from kedro_airflow.plugin import commands


@pytest.mark.parametrize(
    "dag_name,pipeline_name,command",
    [
        # Test normal execution
        ("hello_world", "__default__", ["airflow", "create"]),
        # Test execution with alternate pipeline name
        ("hello_world", "ds", ["airflow", "create", "--pipeline", "ds"]),
        # Test with grouping
        ("hello_world", "__default__", ["airflow", "create", "--group-in-memory"]),
    ],
)
def test_create_airflow_dag(dag_name, pipeline_name, command, cli_runner, metadata):
    """Check the generation and validity of a simple Airflow DAG."""
    dag_file = (
        Path.cwd()
        / "airflow_dags"
        / (
            f"{dag_name}_dag.py"
            if pipeline_name == "__default__"
            else f"{dag_name}_{pipeline_name}_dag.py"
        )
    )
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()

    expected_airflow_dag = 'tasks["node0"] >> tasks["node1"]'
    with dag_file.open(encoding="utf-8") as f:
        dag_code = [line.strip() for line in f.read().splitlines()]
    assert expected_airflow_dag in dag_code
    dag_file.unlink()


def _create_kedro_airflow_yml(file_name: Path, content: dict[str, Any]):
    file_name.parent.mkdir(parents=True, exist_ok=True)
    with file_name.open("w") as fp:
        yaml.dump(content, fp)


def test_airflow_config_params(cli_runner, metadata):
    """Check if config variables are picked up"""
    dag_name = "hello_world"
    template_name = "airflow_params.j2"
    content = "{{ owner | default('hello')}}"

    _create_kedro_airflow_jinja_template(Path.cwd(), template_name, content)

    # default
    default_content = "hello"
    command = ["airflow", "create", "-j", template_name]
    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}_dag.py"
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()
    assert dag_file.read_text() == default_content
    dag_file.unlink()


def test_airflow_config_params_cli(cli_runner, metadata):
    """Check if config variables are picked up"""
    dag_name = "hello_world"
    template_name = "airflow_params.j2"
    content = "{{ owner | default('hello')}}"

    _create_kedro_airflow_jinja_template(Path.cwd(), template_name, content)

    # "--params"
    expected_content = "testme"
    command = ["airflow", "create", "--params", "owner=testme", "-j", template_name]
    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}_dag.py"
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()
    assert dag_file.read_text() == expected_content
    dag_file.unlink()


def test_airflow_config_params_from_config(cli_runner, metadata):
    """Check if config variables are picked up"""
    dag_name = "hello_world"
    template_name = "airflow_params.j2"
    content = "{{ owner | default('hello')}}"

    _create_kedro_airflow_jinja_template(Path.cwd(), template_name, content)

    # airflow.yml
    expected_content = "someone else"
    file_name = Path.cwd() / "conf" / "base" / "airflow.yml"
    _create_kedro_airflow_yml(file_name, {"default": {"owner": expected_content}})
    command = ["airflow", "create", "-j", template_name]
    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}_dag.py"
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()
    assert dag_file.read_text() == expected_content
    file_name.unlink()
    dag_file.unlink()

    # ../airflow.yml
    expected_content = "yet someone else"
    file_name = Path.cwd() / "conf" / "base" / "airflow" / "default.yml"
    _create_kedro_airflow_yml(file_name, {"default": {"owner": expected_content}})
    command = ["airflow", "create", "-j", template_name]
    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}_dag.py"
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()
    assert dag_file.read_text() == expected_content
    file_name.unlink()


def test_airflow_config_params_from_config_non_default(cli_runner, metadata):
    """Check if config variables are picked up"""
    dag_name = "hello_world"
    template_name = "airflow_params.j2"
    content = "{{ owner | default('hello')}}"
    default_content = "hello"

    _create_kedro_airflow_jinja_template(Path.cwd(), template_name, content)

    # random.yml
    expected_content = "yet someone else again"
    file_name = Path.cwd() / "conf" / "base" / "random.yml"
    _create_kedro_airflow_yml(file_name, {"default": {"owner": expected_content}})
    command = ["airflow", "create", "-j", template_name]
    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}_dag.py"
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()
    assert dag_file.read_text() == default_content
    dag_file.unlink()

    # scheduler.yml
    file_name = Path.cwd() / "conf" / "base" / "scheduler.yml"
    _create_kedro_airflow_yml(file_name, {"default": {"owner": expected_content}})
    command = ["airflow", "create", "-j", template_name]
    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}_dag.py"
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()
    assert dag_file.read_text() == expected_content
    dag_file.unlink()
    file_name.unlink()


def test_airflow_config_params_env(cli_runner, metadata):
    """Check if config variables are picked up"""
    dag_name = "hello_world"
    template_name = "airflow_params.j2"
    content = "{{ owner | default('hello')}}"

    _create_kedro_airflow_jinja_template(Path.cwd(), template_name, content)

    # env
    expected_content = "again someone else"
    file_name = Path.cwd() / "conf" / "local" / "airflow.yml"
    _create_kedro_airflow_yml(file_name, {"default": {"owner": expected_content}})
    command = ["airflow", "create", "-j", template_name, "-e", "local"]
    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}_dag.py"
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()
    assert dag_file.read_text() == expected_content
    dag_file.unlink()


def test_airflow_config_params_custom_pipeline(cli_runner, metadata):
    """Check if config variables are picked up"""
    dag_name = "hello_world"
    template_name = "airflow_params.j2"
    content = "{{ owner | default('hello')}}"

    _create_kedro_airflow_jinja_template(Path.cwd(), template_name, content)

    # custom pipeline name
    expected_content = "finally someone else"
    file_name = Path.cwd() / "conf" / "base" / "airflow.yml"
    _create_kedro_airflow_yml(
        file_name, {"default": {"owner": "foobar"}, "ds": {"owner": expected_content}}
    )
    command = ["airflow", "create", "-j", template_name, "-p", "ds"]
    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}_ds_dag.py"
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()
    assert dag_file.read_text() == expected_content
    dag_file.unlink()


def _create_kedro_airflow_jinja_template(path: Path, name: str, content: str):
    (path / name).write_text(content)


def test_custom_template_exists(cli_runner, metadata):
    """Test execution with different dir and filename for Jinja2 Template"""
    dag_name = "hello_world"
    template_name = "custom_template.j2"
    command = ["airflow", "create", "-j", template_name]
    content = "print('my custom dag')"
    # because there are no jinja variables
    expected_content = content

    _create_kedro_airflow_jinja_template(Path.cwd(), template_name, content)

    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}_dag.py"
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()
    assert dag_file.read_text() == expected_content


def test_custom_template_nonexistent(cli_runner, metadata):
    """Test execution with different dir and filename for Jinja2 Template"""
    template_name = "non_existent_custom_template.j2"
    command = ["airflow", "create", "-j", template_name]
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 2
    assert (
        f"Error: Invalid value for '-j' / '--jinja-file': File '{template_name}' does not exist."
        in result.stdout
    )


def _kedro_create_env(project_root: Path):
    (project_root / "conf" / "remote").mkdir(parents=True)


def test_create_airflow_dag_env_parameter_exists(cli_runner, metadata):
    """Test the `env` parameter"""
    dag_name = "hello_world"
    command = ["airflow", "create", "--env", "remote"]

    _kedro_create_env(Path.cwd())

    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}_remote_dag.py"
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    assert dag_file.exists()

    expected_airflow_dag = 'tasks["node0"] >> tasks["node1"]'
    with dag_file.open(encoding="utf-8") as f:
        dag_code = [line.strip() for line in f.read().splitlines()]
    assert expected_airflow_dag in dag_code


def test_create_airflow_dag_nonexistent_pipeline(cli_runner, metadata):
    """Test executing with a non-existing pipeline"""
    command = ["airflow", "create", "--pipeline", "de"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 1
    assert (
        "kedro.framework.cli.utils.KedroCliError: Pipeline de not found."
        in result.stdout
    )


def test_create_airflow_all_dags(cli_runner, metadata):
    command = ["airflow", "create", "--all"]
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0, (result.exit_code, result.stdout)
    print(result.stdout)

    for dag_name, pipeline_name in [
        ("hello_world", "__default__"),
        ("hello_world", "ds"),
    ]:
        dag_file = (
            Path.cwd()
            / "airflow_dags"
            / (
                f"{dag_name}_dag.py"
                if pipeline_name == "__default__"
                else f"{dag_name}_{pipeline_name}_dag.py"
            )
        )
        assert dag_file.exists()

        expected_airflow_dag = 'tasks["node0"] >> tasks["node1"]'
        with dag_file.open(encoding="utf-8") as f:
            dag_code = [line.strip() for line in f.read().splitlines()]
        assert expected_airflow_dag in dag_code
        dag_file.unlink()


def test_create_airflow_all_and_pipeline(cli_runner, metadata):
    command = ["airflow", "create", "--all", "-p", "ds"]
    result = cli_runner.invoke(commands, command, obj=metadata)
    assert result.exit_code == 2
    assert (
        "Error: Invalid value: The `--all` and `--pipeline` option are mutually exclusive."
        in result.stdout
    )
