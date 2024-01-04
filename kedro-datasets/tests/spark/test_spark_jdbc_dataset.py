import pytest
from kedro.io.core import DatasetError

from kedro_datasets.spark import SparkJDBCDataset


@pytest.fixture
def spark_jdbc_args():
    return {"url": "dummy_url", "table": "dummy_table"}


@pytest.fixture
def spark_jdbc_args_credentials(spark_jdbc_args):
    args = spark_jdbc_args
    args.update({"credentials": {"user": "dummy_user", "password": "dummy_pw"}})
    return args


@pytest.fixture
def spark_jdbc_args_credentials_with_none_password(spark_jdbc_args):
    args = spark_jdbc_args
    args.update({"credentials": {"user": "dummy_user", "password": None}})
    return args


@pytest.fixture
def spark_jdbc_args_save_load(spark_jdbc_args):
    args = spark_jdbc_args
    connection_properties = {"properties": {"driver": "dummy_driver"}}
    args.update(
        {"save_args": connection_properties, "load_args": connection_properties}
    )
    return args


def test_missing_url():
    error_message = (
        "'url' argument cannot be empty. Please provide a JDBC"
        " URL of the form 'jdbc:subprotocol:subname'."
    )
    with pytest.raises(DatasetError, match=error_message):
        SparkJDBCDataset(url=None, table="dummy_table")


def test_missing_table():
    error_message = (
        "'table' argument cannot be empty. Please provide"
        " the name of the table to load or save data to."
    )
    with pytest.raises(DatasetError, match=error_message):
        SparkJDBCDataset(url="dummy_url", table=None)


def test_save(mocker, spark_jdbc_args):
    mock_data = mocker.Mock()
    dataset = SparkJDBCDataset(**spark_jdbc_args)
    dataset.save(mock_data)
    mock_data.write.jdbc.assert_called_with("dummy_url", "dummy_table")


def test_save_credentials(mocker, spark_jdbc_args_credentials):
    mock_data = mocker.Mock()
    dataset = SparkJDBCDataset(**spark_jdbc_args_credentials)
    dataset.save(mock_data)
    mock_data.write.jdbc.assert_called_with(
        "dummy_url",
        "dummy_table",
        properties={"user": "dummy_user", "password": "dummy_pw"},
    )


def test_save_args(mocker, spark_jdbc_args_save_load):
    mock_data = mocker.Mock()
    dataset = SparkJDBCDataset(**spark_jdbc_args_save_load)
    dataset.save(mock_data)
    mock_data.write.jdbc.assert_called_with(
        "dummy_url", "dummy_table", properties={"driver": "dummy_driver"}
    )


def test_except_bad_credentials(mocker, spark_jdbc_args_credentials_with_none_password):
    pattern = r"Credential property 'password' cannot be None(.+)"
    with pytest.raises(DatasetError, match=pattern):
        mock_data = mocker.Mock()
        dataset = SparkJDBCDataset(**spark_jdbc_args_credentials_with_none_password)
        dataset.save(mock_data)


def test_load(mocker, spark_jdbc_args):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset._get_spark"
    ).return_value
    dataset = SparkJDBCDataset(**spark_jdbc_args)
    dataset.load()
    spark.read.jdbc.assert_called_with("dummy_url", "dummy_table")


def test_load_credentials(mocker, spark_jdbc_args_credentials):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset._get_spark"
    ).return_value
    dataset = SparkJDBCDataset(**spark_jdbc_args_credentials)
    dataset.load()
    spark.read.jdbc.assert_called_with(
        "dummy_url",
        "dummy_table",
        properties={"user": "dummy_user", "password": "dummy_pw"},
    )


def test_load_args(mocker, spark_jdbc_args_save_load):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset._get_spark"
    ).return_value
    dataset = SparkJDBCDataset(**spark_jdbc_args_save_load)
    dataset.load()
    spark.read.jdbc.assert_called_with(
        "dummy_url", "dummy_table", properties={"driver": "dummy_driver"}
    )
