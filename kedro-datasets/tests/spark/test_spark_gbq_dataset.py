import base64
import json
import re

import pytest
from kedro.io import DatasetError
from pyspark.sql import SparkSession

from kedro_datasets.spark.spark_gbq_dataset import GBQQueryDataset

SQL_QUERY = "SELECT * FROM table"
MATERIALIZATION_DATASET = "dataset"
MATERIALIZATION_PROJECT = "project"
LOAD_ARGS = {"key": "value"}
REQUIRED_INIT_ARGS = {
    "sql": SQL_QUERY,
    "materialization_dataset": MATERIALIZATION_DATASET,
}


@pytest.fixture
def spark_session(mocker):
    return mocker.MagicMock(spec=SparkSession)


@pytest.fixture
def dummy_save_dataset(spark_session):
    return spark_session.createDataFrame([("foo",)], ["bar"])


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
        match=f"Please provide one of 'base64', 'file' or 'json' key in the credentials. You provided: {invalid_cred_key}",
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
        f"Please provide only one of 'base64', 'file' or 'json' key in the credentials. You provided: {list(credentials.keys())}"
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
        "kedro_datasets.spark.spark_gbq_dataset.get_spark", return_value=spark_session
    )
    read_obj = mocker.MagicMock()
    spark_session.read.format.return_value = read_obj
    read_obj.load.return_value = mocker.MagicMock()

    gbq_query_dataset.load()

    spark_session.read.format.assert_called_once_with("bigquery")
    read_obj.load.assert_called_once_with(**expected_load_args)
