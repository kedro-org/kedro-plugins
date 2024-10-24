import datetime
from unittest.mock import MagicMock, patch

import pytest

snoflake = pytest.importorskip("snowflake")

import pandas as pd
import pytest
from kedro.io.core import DatasetError
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kedro_datasets.snowflake.snowpark_dataset import SnowparkTableDataset

# example dummy configuration for local testing
DUMMY_CREDENTIALS = {
    "account": "DUMMY_ACCOUNT",
    "warehouse": "DUMMY_WAREHOUSE",
    "database": "DUMMY_DATABASE",
    "schema": "DUMMY_SCHEMA",
    "user": "DUMMY_USER",
    "password": "DUMMY_PASSWORD",
}


@pytest.fixture(scope="module")
def local_snowpark_session() -> Session:
    """
    Creates a local Snowflake session for testing purposes.
    See

    Returns:
        Session: Snowflake session object configured for local testing.
    """
    return Session.builder.config("local_testing", True).create()


@pytest.fixture(scope="module")
def snowflake_dataset(local_snowpark_session: Session) -> SnowparkTableDataset:
    """
    Provides a SnowparkTableDataset fixture for testing.

    Args:
        snowflake_session (Session): The Snowflake session used for this dataset.

    Returns:
        SnowparkTableDataset: Dataset configuration for a Snowflake table.
    """
    return SnowparkTableDataset(
        table_name="DUMMY_TABLE",
        credentials=DUMMY_CREDENTIALS,
        session=local_snowpark_session,
        save_args={"mode": "overwrite"},
    )


@pytest.fixture(scope="module")
def sample_sp_df(local_snowpark_session: Session) -> DataFrame:
    """
    Creates a sample Snowpark DataFrame for testing.

    Args:
        snowflake_session (Session): Session to create the DataFrame.

    Returns:
        snowpark.DataFrame: DataFrame with sample data and schema.
    """
    return local_snowpark_session.create_dataframe(
        [
            (
                "John",
                23,
                datetime.date(1999, 12, 2),
                6.5,
                datetime.datetime(2022, 12, 2, 13, 20, 1),
            ),
            (
                "Jane",
                41,
                datetime.date(1981, 1, 3),
                5.7,
                datetime.datetime(2022, 12, 2, 13, 21, 11),
            ),
        ],
        schema=StructType(
            [
                StructField("name", StringType()),
                StructField("age", IntegerType()),
                StructField("bday", DateType()),
                StructField("height", FloatType()),
                StructField("insert_dttm", TimestampType()),
            ]
        ),
    )


@pytest.fixture(scope="module")
def sample_pd_df() -> pd.DataFrame:
    """
    Creates a sample Pandas DataFrame for testing.

    Returns:
        pd.DataFrame: DataFrame with sample data.
    """
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [30, 40],
            "bday": [datetime.date(1993, 1, 1), datetime.date(1983, 2, 2)],
            "height": [5.5, 6.0],
            "insert_dttm": [
                datetime.datetime(2023, 1, 1, 10, 0),
                datetime.datetime(2023, 1, 1, 12, 0),
            ],
        }
    )


class TestSnowparkTableDataset:
    """Tests for the SnowparkTableDataset functionality."""

    def test_save_with_snowpark(
        self, sample_sp_df: DataFrame, snowflake_dataset: SnowparkTableDataset
    ) -> None:
        """Tests saving a Snowpark DataFrame to a Snowflake table.

        Args:
            sample_sp_df (snowpark.DataFrame): Sample data to save.
            snowflake_dataset (SnowparkTableDataset): Dataset to test.

        Asserts:
            The count of the loaded DataFrame matches the saved DataFrame.
        """
        snowflake_dataset.save(sample_sp_df)
        loaded_df = snowflake_dataset.load()
        assert loaded_df.count() == sample_sp_df.count()

    def test_save_with_pandas(
        self, sample_pd_df: pd.DataFrame, snowflake_dataset: SnowparkTableDataset
    ) -> None:
        """
        Tests saving a Pandas DataFrame to a Snowflake table.

        Args:
            sample_pd_df (pd.DataFrame): Sample data to save.
            snowflake_dataset (SnowparkTableDataset): Dataset to test.

        Asserts:
            The count of the loaded DataFrame matches the number of rows in the Pandas DataFrame.
        """
        snowflake_dataset.save(sample_pd_df)
        loaded_df = snowflake_dataset.load()
        assert loaded_df.count() == len(sample_pd_df)

    def test_load(
        self, snowflake_dataset: SnowparkTableDataset, sample_sp_df: DataFrame
    ) -> None:
        """
        Tests loading data from a Snowflake table.

        Args:
            snowflake_dataset (SnowparkTableDataset): Dataset to load data from.
            sample_sp_df (snowpark.DataFrame): Sample data for reference.

        Asserts:
            The count of the loaded DataFrame matches the reference sample DataFrame.
        """
        loaded_df = snowflake_dataset.load()
        assert loaded_df.count() == sample_sp_df.count()

    def test_exists(self, snowflake_dataset: SnowparkTableDataset) -> None:
        """
        Tests if a Snowflake table exists.

        Args:
            snowflake_dataset (SnowparkTableDataset): Dataset to check existence.

        Asserts:
            The dataset table exists in the Snowflake environment.
        """
        exists = snowflake_dataset._exists()
        assert exists

    def test_not_exists(self, snowflake_dataset: SnowparkTableDataset) -> None:
        """
        Tests if a non-existent Snowflake table is detected.
        Args:
            snowflake_dataset (SnowparkTableDataset): Dataset to check existence.

        Asserts:
            The dataset table does not exist in the Snowflake environment.
        """
        snowflake_dataset._table_name = "NON_EXISTENT_TABLE"
        exists = snowflake_dataset._exists()
        assert not exists

    def test_get_session(self, snowflake_dataset: SnowparkTableDataset) -> None:
        """
        Tests getting the Snowflake session from the dataset.

        Args:
            snowflake_dataset (SnowparkTableDataset): Dataset to get the session from.

        Asserts:
            The session is the same as the one used to create the dataset.
        """
        assert (
            snowflake_dataset._get_session(snowflake_dataset._connection_parameters)
            == snowflake_dataset._session
        )

    def test_missing_table_name(self):
        with pytest.raises(
            DatasetError, match="'table_name' argument cannot be empty."
        ):
            SnowparkTableDataset(table_name="", credentials=DUMMY_CREDENTIALS)

    def test_missing_credentials(self):
        with pytest.raises(
            DatasetError, match="'credentials' argument cannot be empty."
        ):
            SnowparkTableDataset(table_name="weather_data", credentials=None)

    def test_missing_database_in_both_parameters_and_credentials(self):
        credentials = DUMMY_CREDENTIALS.copy()
        credentials.pop("database")
        with pytest.raises(
            DatasetError, match="'database' must be provided by credentials or dataset."
        ):
            SnowparkTableDataset(table_name="DUMMY_TABLE", credentials=credentials)

    def test_missing_schema_in_both_parameters_and_credentials(self):
        credentials = DUMMY_CREDENTIALS.copy()
        credentials.pop("schema")
        with pytest.raises(
            DatasetError, match="'schema' must be provided by credentials or dataset."
        ):
            SnowparkTableDataset(table_name="DUMMY_TABLE", credentials=credentials)

    def test_validate_and_get_table_name_success(self, snowflake_dataset):
        """
        Test that the `_validate_and_get_table_name` method returns the correct table name.

        This test calls the `_validate_and_get_table_name` method with a valid table name
        and verifies that the method returns the correct table name.

        Args:
            self: The test case instance.
            snowflake_dataset: The dataset instance being tested.

        Asserts:
            The method returns the correct table name.
        """
        snowflake_dataset._table_name = "DUMMY_TABLE"
        expected_table_name = "DUMMY_DATABASE.DUMMY_SCHEMA.DUMMY_TABLE"

        table_name = snowflake_dataset._validate_and_get_table_name()
        assert table_name == expected_table_name

    @pytest.mark.parametrize(
        "table_name, database, schema",
        [
            ("", "DUMMY_DATABASE", "DUMMY_SCHEMA"),  # Invalid table name (empty string)
            ("DUMMY_TABLE", "", "DUMMY_SCHEMA"),  # Invalid database (empty string)
            ("DUMMY_TABLE", "DUMMY_DATABASE", ""),  # Invalid schema (empty string)
            (None, "DUMMY_DATABASE", "DUMMY_SCHEMA"),  # Invalid table name (None)
            ("DUMMY_TABLE", None, "DUMMY_SCHEMA"),  # Invalid database (None)
            ("DUMMY_TABLE", "DUMMY_DATABASE", None),  # Invalid schema (None)
        ],
    )
    def test_validate_and_get_table_name_error(
        self, snowflake_dataset, table_name, database, schema
    ):
        """
        Test that the `_validate_and_get_table_name` method raises an error for invalid table name, database, and schema.

        This test calls the `_validate_and_get_table_name` method with invalid table name, database, and schema
        and verifies that the method raises a `DatasetError`.

        Args:
            self: The test case instance.
            snowflake_dataset: The dataset instance being tested.
            table_name: The table name to test.
            database: The database name to test.
            schema: The schema name to test.

        Asserts:
            A `DatasetError` is raised.
        """
        snowflake_dataset._table_name = table_name
        snowflake_dataset._database = database
        snowflake_dataset._schema = schema

        with pytest.raises(
            DatasetError, match="Database, schema or table name cannot be None or empty"
        ):
            snowflake_dataset._validate_and_get_table_name()

    def test_get_session_existing_session(self, mocker, snowflake_dataset):
        """
        Test that `snowflake_dataset._get_session` returns the existing active session.

        Args:
            mocker: A fixture for mocking objects.
            snowflake_dataset: An instance of the Snowflake dataset.

        Asserts:
            The session returned by `_get_session` is the same as the active session.
            The `get_active_session` method is called exactly once.
        """
        mock_active_session = MagicMock()
        mock_get_active_session = mocker.patch(
            "snowflake.snowpark.context.get_active_session",
            return_value=mock_active_session,
        )

        session = snowflake_dataset._get_session(
            snowflake_dataset._connection_parameters
        )

        assert session == mock_active_session
        mock_get_active_session.assert_called_once()

    @patch("snowflake.snowpark.Session.builder")
    def test_get_session_no_existing_session(self, mock_builder, snowflake_dataset):
        """
        Test the `_get_session` method of `SnowparkTableDataset` when there is no existing session.

        This test ensures that the `_get_session` method correctly initializes a new session
        using the Snowflake Snowpark `Session.builder` when there is no existing session.

        Args:
            mock_builder (MagicMock): Mocked `Session.builder` object.
            snowflake_dataset (SnowparkTableDataset): Instance of the dataset being tested.

        Steps:
            1. Close the existing session and set the private session attribute to `None`.
            2. Mock the `builder`, `configs`, and `create` methods to simulate session creation.
            3. Call the `_get_session` method with the dataset's connection parameters.
            4. Assert that the `configs` method was called once with the correct parameters.
            5. Assert that the `create` method was called once.
            6. Assert that the returned session is the mocked `create` instance.

        Asserts:
            - `mock_builder.configs` is called once with the dataset's connection parameters.
            - `mock_configs_instance.create` is called once.
            - The returned session is the `mock_create_instance` object.
        """
        snowflake_dataset._session.close()
        snowflake_dataset._SnowparkTableDataset__session = (
            None  # Accessing the mangled private attribute
        )

        # mock the builder, configs, and create methods since we cannot create a real session
        mock_configs_instance = MagicMock()
        mock_create_instance = MagicMock()
        mock_builder.configs.return_value = mock_configs_instance
        mock_configs_instance.create.return_value = mock_create_instance

        session = snowflake_dataset._get_session(
            snowflake_dataset._connection_parameters
        )

        # assert that each part of the chain was called correctly
        mock_builder.configs.assert_called_once_with(
            snowflake_dataset._connection_parameters
        )
        mock_configs_instance.create.assert_called_once()

        # Assert the returned session is the mock_create_instance object
        assert session == mock_create_instance
