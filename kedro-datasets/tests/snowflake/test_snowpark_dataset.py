import datetime
import sys

import pandas as pd
import pytest

SNOWPARK_AVAILABLE = False

try:
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

    SNOWPARK_AVAILABLE = True
except ImportError:
    print(f"Snowpark not supported in Python version {sys.version_info}")

if SNOWPARK_AVAILABLE and sys.version_info < (3, 12):
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

    @pytest.fixture
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

    @pytest.fixture
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

elif sys.version_info >= (3, 12):

    @pytest.mark.skip(
        reason="Snowpark not supported in this Python versions higher than 3.11"
    )
    class TestSnowparkTableDataset:
        def test_skip(self):
            pass
