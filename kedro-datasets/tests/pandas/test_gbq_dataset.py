from pathlib import PosixPath

import pandas as pd
import pytest
from google.cloud.exceptions import NotFound
from kedro.io.core import DatasetError
from pandas.testing import assert_frame_equal

from kedro_datasets._utils import ConnectionMixin
from kedro_datasets.pandas import GBQQueryDataset, GBQTableDataset

DATASET = "dataset"
TABLE_NAME = "table_name"
PROJECT = "project"
SQL_QUERY = "SELECT * FROM table_a"


@pytest.fixture
def dummy_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


@pytest.fixture
def mock_bigquery_client(mocker):
    mocked = mocker.patch("google.cloud.bigquery.Client", autospec=True)
    return mocked


@pytest.fixture
def gbq_dataset(load_args, save_args, mock_bigquery_client):
    return GBQTableDataset(
        dataset=DATASET,
        table_name=TABLE_NAME,
        project=PROJECT,
        credentials=None,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture(params=[{}])
def gbq_sql_dataset(load_args, mock_bigquery_client):
    return GBQQueryDataset(
        sql=SQL_QUERY,
        project=PROJECT,
        credentials=None,
        load_args=load_args,
    )


@pytest.fixture
def sql_file(tmp_path: PosixPath):
    file = tmp_path / "test.sql"
    file.write_text(SQL_QUERY)
    return file.as_posix()


@pytest.fixture(params=[{}])
def gbq_sql_file_dataset(load_args, sql_file, mock_bigquery_client):
    return GBQQueryDataset(
        filepath=sql_file,
        project=PROJECT,
        credentials=None,
        load_args=load_args,
    )


class TestGBQDataset:
    def test_exists(self, mock_bigquery_client):
        """Test `exists` method invocation."""
        mock_bigquery_client.return_value.get_table.side_effect = [
            NotFound("NotFound"),
            "exists",
        ]

        dataset = GBQTableDataset(dataset=DATASET, table_name=TABLE_NAME)
        assert not dataset.exists()
        assert dataset.exists()

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, gbq_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert gbq_dataset._load_args[key] == value

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, gbq_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert gbq_dataset._save_args[key] == value

    def test_load_missing_file(self, gbq_dataset, mocker):
        """Check the error when trying to load missing table."""
        pattern = r"Failed while loading data from dataset kedro_datasets.pandas.gbq_dataset.GBQTableDataset\(.*\)"
        mocked_read_gbq = mocker.patch(
            "kedro_datasets.pandas.gbq_dataset.pd_gbq.read_gbq"
        )
        mocked_read_gbq.side_effect = ValueError
        with pytest.raises(DatasetError, match=pattern):
            gbq_dataset.load()

    @pytest.mark.parametrize("load_args", [{"location": "l1"}], indirect=True)
    @pytest.mark.parametrize("save_args", [{"location": "l2"}], indirect=True)
    def test_invalid_location(self, save_args, load_args):
        """Check the error when initializing instance if save_args and load_args
        'location' are different."""
        pattern = r""""load_args\['location'\]" is different from "save_args\['location'\]"."""
        with pytest.raises(DatasetError, match=pattern):
            GBQTableDataset(
                dataset=DATASET,
                table_name=TABLE_NAME,
                project=PROJECT,
                credentials=None,
                load_args=load_args,
                save_args=save_args,
            )

    @pytest.mark.parametrize("save_args", [{"option1": "value1"}], indirect=True)
    @pytest.mark.parametrize("load_args", [{"option2": "value2"}], indirect=True)
    def test_str_representation(self, gbq_dataset, save_args, load_args):
        """Test string representation of the dataset instance."""
        str_repr = str(gbq_dataset)
        assert "GBQTableDataset" in str_repr
        assert TABLE_NAME in str_repr
        assert DATASET in str_repr
        for k in save_args.keys():
            assert k in str_repr
        for k in load_args.keys():
            assert k in str_repr

    def test_save_load_data(self, gbq_dataset, dummy_dataframe, mocker):
        """Test saving and reloading the dataset."""
        sql = f"select * from {DATASET}.{TABLE_NAME}"
        table_id = f"{DATASET}.{TABLE_NAME}"
        mocked_to_gbq = mocker.patch("kedro_datasets.pandas.gbq_dataset.pd_gbq.to_gbq")
        mocked_read_gbq = mocker.patch(
            "kedro_datasets.pandas.gbq_dataset.pd_gbq.read_gbq"
        )
        mocked_read_gbq.return_value = dummy_dataframe
        mocked_df = mocker.Mock()
        gbq_dataset._connection._credentials = None

        gbq_dataset.save(mocked_df)
        loaded_data = gbq_dataset.load()

        mocked_to_gbq.assert_called_once_with(
            dataframe=mocked_df,
            destination_table=table_id,
            project_id=PROJECT,
            credentials=None,
            progress_bar=False,
        )
        mocked_read_gbq.assert_called_once_with(
            project_id=PROJECT, credentials=None, query_or_table=sql
        )
        assert_frame_equal(dummy_dataframe, loaded_data)

    @pytest.mark.parametrize(
        "load_args", [{"query_or_table": "Select 1"}], indirect=True
    )
    def test_read_gbq_with_query(self, gbq_dataset, dummy_dataframe, mocker, load_args):
        """Test loading dataset with query in the argument."""
        mocked_read_gbq = mocker.patch(
            "kedro_datasets.pandas.gbq_dataset.pd_gbq.read_gbq"
        )
        mocked_read_gbq.return_value = dummy_dataframe
        loaded_data = gbq_dataset.load()

        mocked_read_gbq.assert_called_once_with(
            project_id=PROJECT,
            credentials=None,
            query_or_table=load_args["query_or_table"],
        )

        assert_frame_equal(dummy_dataframe, loaded_data)

    @pytest.mark.parametrize(
        "dataset,table_name",
        [
            ("data set", TABLE_NAME),
            ("data;set", TABLE_NAME),
            (DATASET, "table name"),
            (DATASET, "table;name"),
        ],
    )
    def test_validation_of_dataset_and_table_name(self, dataset, table_name):
        pattern = "Neither white-space nor semicolon are allowed.*"
        with pytest.raises(DatasetError, match=pattern):
            GBQTableDataset(dataset=dataset, table_name=table_name)

    # NOTE: tests for json and filepath are not DRY, keeping them separate for clarity
    def test_credentials_propagation_json(self, mocker):
        credentials = {"token": "my_token"}
        credentials_obj = "credentials"
        mocked_credentials = mocker.patch(
            "kedro_datasets.pandas.gbq_dataset.ServiceAccountCredentials.from_service_account_info",
            return_value=credentials_obj,
        )
        mocked_bigquery = mocker.patch("kedro_datasets.pandas.gbq_dataset.bigquery")

        dataset = GBQTableDataset(
            dataset=DATASET,
            table_name=TABLE_NAME,
            credentials=credentials,
            project=PROJECT,
        )
        dataset.exists()  # Do something to trigger the client creation.

        mocked_credentials.assert_called_once_with(credentials)
        mocked_bigquery.Client.assert_called_once_with(
            project=PROJECT, credentials=credentials_obj, location=None
        )

        # Clear connections
        ConnectionMixin._connections = {}

    def test_credentials_propagation_filepath(self, mocker):
        credentials = "path/to/credentials.json"
        credentials_obj = "credentials"
        mocked_credentials = mocker.patch(
            "kedro_datasets.pandas.gbq_dataset.ServiceAccountCredentials.from_service_account_file",
            return_value=credentials_obj,
        )
        mocked_bigquery = mocker.patch("kedro_datasets.pandas.gbq_dataset.bigquery")

        dataset = GBQTableDataset(
            dataset=DATASET,
            table_name=TABLE_NAME,
            credentials=credentials,
            project=PROJECT,
        )
        dataset.exists()  # Do something to trigger the client creation.

        mocked_credentials.assert_called_once_with(credentials)
        mocked_bigquery.Client.assert_called_once_with(
            project=PROJECT, credentials=credentials_obj, location=None
        )

        # Clear connections
        ConnectionMixin._connections = {}


class TestGBQQueryDataset:
    def test_empty_query_error(self):
        """Check the error when instantiating with empty query or file"""
        pattern = (
            r"'sql' and 'filepath' arguments cannot both be empty\."
            r"Please provide a sql query or path to a sql query file\."
        )
        with pytest.raises(DatasetError, match=pattern):
            GBQQueryDataset(sql="", filepath="", credentials=None)

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, gbq_sql_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert gbq_sql_dataset._load_args[key] == value

    # NOTE: tests for json and filepath are not DRY, keeping them separate for clarity
    def test_credentials_propagation_json(self, mocker):
        credentials = {"token": "my_token"}
        credentials_obj = "credentials"
        mocked_credentials = mocker.patch(
            "kedro_datasets.pandas.gbq_dataset.ServiceAccountCredentials.from_service_account_info",
            return_value=credentials_obj,
        )

        dataset = GBQQueryDataset(
            sql=SQL_QUERY,
            credentials=credentials,
            project=PROJECT,
        )
        dataset.exists()  # Do something to trigger the client creation.

        assert dataset._credentials == credentials_obj
        mocked_credentials.assert_called_once_with(credentials)

    def test_credentials_propagation_filepath(self, mocker):
        credentials = "path/to/credentials.json"
        credentials_obj = "credentials"
        mocked_credentials = mocker.patch(
            "kedro_datasets.pandas.gbq_dataset.ServiceAccountCredentials.from_service_account_file",
            return_value=credentials_obj,
        )

        dataset = GBQQueryDataset(
            sql=SQL_QUERY,
            credentials=credentials,
            project=PROJECT,
        )
        dataset.exists()  # Do something to trigger the client creation.

        assert dataset._credentials == credentials_obj
        mocked_credentials.assert_called_once_with(credentials)

    def test_load(self, mocker, gbq_sql_dataset, dummy_dataframe):
        """Test `load` method invocation"""
        mocked_read_gbq = mocker.patch(
            "kedro_datasets.pandas.gbq_dataset.pd_gbq.read_gbq"
        )
        mocked_read_gbq.return_value = dummy_dataframe

        loaded_data = gbq_sql_dataset.load()

        mocked_read_gbq.assert_called_once_with(
            project_id=PROJECT, credentials=None, query_or_table=SQL_QUERY
        )

        assert_frame_equal(dummy_dataframe, loaded_data)

    def test_load_query_file(self, mocker, gbq_sql_file_dataset, dummy_dataframe):
        """Test `load` method invocation using a file as input query"""
        mocked_read_gbq = mocker.patch(
            "kedro_datasets.pandas.gbq_dataset.pd_gbq.read_gbq"
        )
        mocked_read_gbq.return_value = dummy_dataframe

        loaded_data = gbq_sql_file_dataset.load()

        mocked_read_gbq.assert_called_once_with(
            project_id=PROJECT, credentials=None, query_or_table=SQL_QUERY
        )

        assert_frame_equal(dummy_dataframe, loaded_data)

    def test_save_error(self, gbq_sql_dataset, dummy_dataframe):
        """Check the error when trying to save to the dataset"""
        pattern = r"'save' is not supported on GBQQueryDataset"
        with pytest.raises(DatasetError, match=pattern):
            gbq_sql_dataset.save(dummy_dataframe)

    def test_str_representation_sql(self, gbq_sql_dataset, sql_file):
        """Test the dataset instance string representation"""
        str_repr = str(gbq_sql_dataset)
        assert (
            f"kedro_datasets.pandas.gbq_dataset.GBQQueryDataset(sql='{SQL_QUERY}', filepath='None', load_args='{{}}')"
            in str_repr
        )
        assert sql_file not in str_repr

    def test_str_representation_filepath(self, gbq_sql_file_dataset, sql_file):
        """Test the dataset instance string representation with filepath arg."""
        str_repr = str(gbq_sql_file_dataset)
        assert (
            f"kedro_datasets.pandas.gbq_dataset.GBQQueryDataset(sql='None', filepath='{str(sql_file)}', load_args='{{}}')"
            in str_repr
        )
        assert SQL_QUERY not in str_repr

    def test_sql_and_filepath_args(self, sql_file):
        """Test that an error is raised when both `sql` and `filepath` args are given."""
        pattern = (
            r"'sql' and 'filepath' arguments cannot both be provided."
            r"Please only provide one."
        )
        with pytest.raises(DatasetError, match=pattern):
            GBQQueryDataset(sql=SQL_QUERY, filepath=sql_file)
