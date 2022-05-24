from pathlib import Path

import pytest
from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline, node
from kedro_airflow.plugin import commands


def identity(arg):
    return arg


@pytest.mark.parametrize(
    "dag_name,pipeline_name,command",
    [
        # Test normal execution
        ("hello_world_dag", "__default__", ["airflow", "create"]),
        # Test execution with alternate pipeline and output name
        ("hello_world_dag", "ds", ["airflow", "create", "--pipeline", "ds"]),
        # Test execution with different dir and filename for Jinja2 Template
        (
            "hello_world_dag",
            "__default__",
            ["airflow", "create", "-j", "airflow_dag.j2"],
        ),
        # Test execution with different target filename
        (
            "foo_bar",
            "__default__",
            ["airflow", "create", "-t", "airflow_dags/foo_bar.py"],
        ),
    ],
)
def test_create_airflow_dag(
    dag_name, pipeline_name, command, mocker, cli_runner, metadata
):
    """Check the generation and validity of a simple Airflow DAG."""
    dag_file = Path.cwd() / "airflow_dags" / f"{dag_name}.py"
    mock_pipeline = Pipeline(
        [
            node(identity, ["input"], ["intermediate"], name="node0"),
            node(identity, ["intermediate"], ["output"], name="node1"),
        ],
        tags="pipeline0",
    )
    mocker.patch.dict(pipelines, {pipeline_name: mock_pipeline})
    result = cli_runner.invoke(commands, command, obj=metadata)

    assert result.exit_code == 0
    assert dag_file.exists()

    expected_airflow_dag = 'tasks["node0"] >> tasks["node1"]'
    with open(dag_file, "r", encoding="utf-8") as f:
        dag_code = [line.strip() for line in f.read().splitlines()]
    assert expected_airflow_dag in dag_code


def test_creating_airflow_dag_given_folder_instead_of_jinjafile_raises_error(
    mocker, cli_runner, metadata
):
    """Check the generation and validity of a simple Airflow DAG."""
    mock_pipeline = Pipeline(
        [
            node(identity, ["input"], ["intermediate"], name="node0"),
            node(identity, ["intermediate"], ["output"], name="node1"),
        ],
        tags="pipeline0",
    )
    mocker.patch.dict(pipelines, {"__default__": mock_pipeline})
    with pytest.raises(ValueError, match="jinja_template must be a file"):
        cli_runner.invoke(
            commands,
            ["airflow", "create", "-j", "."],
            obj=metadata,
            catch_exceptions=False,
        )
