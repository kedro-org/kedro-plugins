from pathlib import Path

from kedro.framework.project import pipelines
from kedro.pipeline import Pipeline, node
from kedro_airflow.plugin import commands


def identity(arg):
    return arg


def test_create_airflow_dag(mocker, cli_runner, metadata):
    """Check the generation and validity of a simple Airflow DAG."""
    dag_file = Path.cwd() / "airflow_dags" / "hello_world_dag.py"
    mock_pipeline = Pipeline(
        [
            node(identity, ["input"], ["intermediate"], name="node0"),
            node(identity, ["intermediate"], ["output"], name="node1"),
        ],
        tags="pipeline0",
    )
    mocker.patch.dict(pipelines, {"__default__": mock_pipeline})
    result = cli_runner.invoke(commands, ["airflow", "create"], obj=metadata)

    assert result.exit_code == 0
    assert str(dag_file) in result.output
    assert dag_file.exists()

    expected_airflow_dag = 'tasks["node0"] >> tasks["node1"]'
    with open(dag_file, "r", encoding="utf-8") as f:
        dag_code = [line.strip() for line in f.read().splitlines()]
    assert expected_airflow_dag in dag_code
