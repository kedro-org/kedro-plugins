import base64
import json
import os
import re
import tempfile

import pytest
from kedro.io import DatasetError
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from kedro_datasets.spark import GBQQueryDataset

SQL_QUERY = "SELECT * FROM table"
SQL_FILEPATH = "/path/to/file.sql"
MATERIALIZATION_DATASET = "dataset"
MATERIALIZATION_PROJECT = "project"
LOAD_ARGS = {"key": "value"}
REQUIRED_INIT_ARGS = {
    "sql": SQL_QUERY,
    "materialization_dataset": MATERIALIZATION_DATASET,
}


@pytest.fixture
def mock_gateway_client(mocker):
    mock_client = mocker.MagicMock()
    mock_client.send_command.return_value = (
        "ysjava.util.NoSuchElementException: viewsEnabled"
    )
    mock_client.converters = []
    mock_client.is_connected.return_value = True
    mock_client.deque = mocker.MagicMock()
    return mock_client


@pytest.fixture
def mock_java_object(mocker, mock_gateway_client):
    mock_java_object = mocker.MagicMock()
    mock_java_object._target_id = "o123"
    mock_java_object._gateway_client = mock_gateway_client
    return mock_java_object


@pytest.fixture
def mock_py4j_error_exception(mock_java_object):
    mock_errmsg = "An error occurred while calling o123.load."
    return Py4JJavaError(mock_errmsg, java_exception=mock_java_object)


@pytest.fixture
def spark_session(mocker):
    return mocker.MagicMock(spec=SparkSession)


@pytest.fixture
def dummy_save_dataset(spark_session):
    return spark_session.createDataFrame([("foo",)], ["bar"])


@pytest.fixture
def sql_file():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".sql") as temp_sql_file:
        temp_sql_file.write(SQL_QUERY.encode())
        temp_sql_file_path = temp_sql_file.name

    yield temp_sql_file_path

    os.remove(temp_sql_file_path)


@pytest.fixture
def gbq_query_dataset():
    return GBQQueryDataset(
        sql=SQL_QUERY,
        materialization_dataset=MATERIALIZATION_DATASET,
        materialization_project=MATERIALIZATION_PROJECT,
        load_args=LOAD_ARGS,
    )


def test_save_not_implemented(gbq_query_dataset, dummy_save_dataset):
    with pytest.raises(
        DatasetError,
        match=r"'save' is not supported on GBQQueryDataset",
    ):
        gbq_query_dataset.save(dummy_save_dataset)


@pytest.mark.parametrize(
    "credentials, expected_credentials",
    [
        ({"base64": "base64_creds"}, {"credentials": "base64_creds"}),
        ({"file": "/path/to/creds.json"}, {"credentialsFile": "/path/to/creds.json"}),
        (
            {"json": {"type": "service_account"}},
            {
                "credentials": base64.b64encode(
                    json.dumps({"type": "service_account"}).encode("utf-8")
                ).decode("utf-8")
            },
        ),
        ({}, {}),
    ],
)
def test_get_spark_bq_credentials(gbq_query_dataset, credentials, expected_credentials):
    gbq_query_dataset._bq_credentials = credentials
    assert gbq_query_dataset._get_spark_bq_credentials() == expected_credentials


def test_invalid_bq_credentials_key(gbq_query_dataset):
    invalid_cred_key = "invalid_cred_key"
    gbq_query_dataset._bq_credentials = {invalid_cred_key: "value"}
    with pytest.raises(
        ValueError,
        match=f"Please provide one of {gbq_query_dataset._VALID_CREDENTIALS_KEYS} key in the credentials. You provided: {invalid_cred_key}",
    ):
        gbq_query_dataset._get_spark_bq_credentials()


@pytest.mark.parametrize(
    "credentials",
    [
        {"base64": "base64_creds", "file": "/path/to/creds.json"},
        {"base64": "base64_creds", "json": {"type": "service_account"}},
        {"file": "/path/to/creds.json", "json": {"type": "service_account"}},
        {
            "base64": "base64_creds",
            "file": "/path/to/creds.json",
            "json": {"type": "service_account"},
        },
        {"base64": "base64_creds", "invalid_key": "value"},
    ],
)
def test_more_than_one_bq_credentials_key(gbq_query_dataset, credentials):
    gbq_query_dataset._bq_credentials = credentials
    pattern = re.escape(
        f"Please provide only one of {gbq_query_dataset._VALID_CREDENTIALS_KEYS} key in the credentials. You provided: {list(credentials.keys())}"
    )
    with pytest.raises(
        ValueError,
        match=pattern,
    ):
        gbq_query_dataset._get_spark_bq_credentials()


@pytest.mark.parametrize(
    "init_args, expected_load_args",
    [
        (
            REQUIRED_INIT_ARGS,
            {
                "query": REQUIRED_INIT_ARGS["sql"],
                "materializationDataset": REQUIRED_INIT_ARGS["materialization_dataset"],
                "viewsEnabled": "true",
            },
        ),
        (
            {**REQUIRED_INIT_ARGS, "materialization_project": MATERIALIZATION_PROJECT},
            {
                "query": REQUIRED_INIT_ARGS["sql"],
                "materializationDataset": REQUIRED_INIT_ARGS["materialization_dataset"],
                "materializationProject": MATERIALIZATION_PROJECT,
                "viewsEnabled": "true",
            },
        ),
    ],
)
def test_load(mocker, spark_session, init_args, expected_load_args):
    gbq_query_dataset = GBQQueryDataset(**init_args)
    mocker.patch(
        "kedro_datasets.spark.gbq_dataset.get_spark", return_value=spark_session
    )
    read_obj = mocker.MagicMock()
    spark_session.read.format.return_value = read_obj
    read_obj.load.return_value = mocker.MagicMock()

    gbq_query_dataset.load()

    spark_session.read.format.assert_called_once_with("bigquery")
    read_obj.load.assert_called_once_with(**expected_load_args)


def test_raise_error_if_both_sql_and_filepath_not_provided():
    with pytest.raises(
        DatasetError,
        match="'sql' and 'filepath' arguments cannot both be empty.",
    ):
        GBQQueryDataset(materialization_dataset=MATERIALIZATION_DATASET)


def test_raise_error_if_both_sql_and_filepath_are_provided():
    with pytest.raises(
        DatasetError,
        match="'sql' and 'filepath' arguments cannot both be provided.",
    ):
        GBQQueryDataset(
            materialization_dataset=MATERIALIZATION_DATASET,
            sql=SQL_QUERY,
            filepath=SQL_FILEPATH,
        )


def test_filepath_sql_query_load(mocker, spark_session, sql_file):
    gbq_query_dataset = GBQQueryDataset(
        filepath=sql_file,
        materialization_dataset=MATERIALIZATION_DATASET,
        materialization_project=MATERIALIZATION_PROJECT,
    )
    mocker.patch(
        "kedro_datasets.spark.gbq_dataset.get_spark", return_value=spark_session
    )
    read_obj = mocker.MagicMock()
    spark_session.read.format.return_value = read_obj
    read_obj.load.return_value = mocker.MagicMock()

    gbq_query_dataset.load()

    read_obj.load.assert_called_once_with(
        query=SQL_QUERY,
        materializationDataset=MATERIALIZATION_DATASET,
        materializationProject=MATERIALIZATION_PROJECT,
        viewsEnabled="true",
    )


@pytest.mark.parametrize(
    "viewsEnabled, expected_warning",
    [("false", True), ("true", False), (None, True)],
)
def test_warning_spark_views_enabled(
    mocker,
    spark_session,
    gbq_query_dataset,
    viewsEnabled,
    expected_warning,
    mock_py4j_error_exception,
    caplog,
):
    mocker.patch(
        "kedro_datasets.spark.gbq_dataset.get_spark", return_value=spark_session
    )
    read_obj = mocker.MagicMock()
    spark_session.read.format.return_value = read_obj
    read_obj.load.return_value = mocker.MagicMock()

    if viewsEnabled is not None:
        spark_session.conf.get.side_effect = lambda x: {"viewsEnabled": viewsEnabled}[x]
    else:
        spark_session.conf.get.side_effect = mock_py4j_error_exception

    with caplog.at_level("WARNING"):
        gbq_query_dataset.load()

    has_warn_msg = any(
        (record.levelname == "WARNING") and ("viewsEnabled" in record.message)
        for record in caplog.records
    )

    if expected_warning:
        assert has_warn_msg
    else:
        assert not has_warn_msg
