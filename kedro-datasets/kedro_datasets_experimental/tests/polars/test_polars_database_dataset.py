"""Unit tests for ``PolarsDatabaseDataset``."""

import polars as pl
import pytest
from kedro.io.core import DatasetError
from polars.testing import assert_frame_equal

from kedro_datasets_experimental.polars import PolarsDatabaseDataset

TABLE_NAME = "table_a"
SQL_QUERY = "SELECT * FROM table_a"
CONNECTION = "sqlite:///:memory:"


@pytest.fixture(autouse=True)
def cleanup_engines():
    yield
    PolarsDatabaseDataset.engines = {}


@pytest.fixture
def sqlite_credentials(tmp_path):
    return {"con": f"sqlite:///{tmp_path / 'test.db'}"}


@pytest.fixture
def sql_file(tmp_path):
    file = tmp_path / "query.sql"
    file.write_text(SQL_QUERY)
    return file.as_posix()


@pytest.fixture
def dummy_dataframe():
    return pl.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


class TestPolarsDatabaseDataset:
    def test_no_query_or_table_name(self):
        """Check the error when none of ``sql``, ``filepath``, ``table_name`` is given."""
        pattern = r"Provide at least one of 'sql', 'filepath', or 'table_name'\."
        with pytest.raises(DatasetError, match=pattern):
            PolarsDatabaseDataset(credentials={"con": CONNECTION})

    def test_sql_and_filepath_args(self, sql_file):
        """Check the error when both ``sql`` and ``filepath`` are provided."""
        pattern = r"'sql' and 'filepath' arguments cannot both be provided\."
        with pytest.raises(DatasetError, match=pattern):
            PolarsDatabaseDataset(
                sql=SQL_QUERY,
                filepath=sql_file,
                table_name=TABLE_NAME,
                credentials={"con": CONNECTION},
            )

    def test_empty_connection(self):
        """Check the error when instantiating with an empty connection string."""
        pattern = r"'con' argument cannot be empty\."
        with pytest.raises(DatasetError, match=pattern):
            PolarsDatabaseDataset(
                sql=SQL_QUERY, table_name=TABLE_NAME, credentials={"con": ""}
            )

    def test_load_with_sql(self, mocker):
        """Test ``load`` invokes ``polars.read_database`` with the cached engine."""
        mock_read = mocker.patch("polars.read_database")
        dataset = PolarsDatabaseDataset(
            sql=SQL_QUERY,
            table_name=TABLE_NAME,
            credentials={"con": CONNECTION},
        )
        dataset.load()

        mock_read.assert_called_once()
        kwargs = mock_read.call_args.kwargs
        assert kwargs["query"] == SQL_QUERY
        assert kwargs["connection"] is dataset.engine

    def test_load_query_file(self, mocker, sql_file):
        """Test ``load`` reads the SQL query from a file when ``filepath`` is set."""
        mock_read = mocker.patch("polars.read_database")
        dataset = PolarsDatabaseDataset(
            filepath=sql_file,
            table_name=TABLE_NAME,
            credentials={"con": CONNECTION},
        )
        dataset.load()

        mock_read.assert_called_once()
        kwargs = mock_read.call_args.kwargs
        assert kwargs["query"] == SQL_QUERY

    def test_load_table_name_only(self, mocker):
        """Test ``load`` builds ``SELECT * FROM <table_name>`` when no sql/filepath."""
        mock_read = mocker.patch("polars.read_database")
        dataset = PolarsDatabaseDataset(
            table_name=TABLE_NAME, credentials={"con": CONNECTION}
        )
        dataset.load()

        mock_read.assert_called_once()
        kwargs = mock_read.call_args.kwargs
        assert kwargs["query"] == f"SELECT * FROM {TABLE_NAME}"
        assert kwargs["connection"] is dataset.engine

    def test_save(self, mocker, dummy_dataframe):
        """Test ``save`` calls ``DataFrame.write_database`` with the default save_args."""
        dataset = PolarsDatabaseDataset(
            sql=SQL_QUERY,
            table_name=TABLE_NAME,
            credentials={"con": CONNECTION},
        )
        mocker.patch.object(dummy_dataframe, "write_database")
        dataset.save(dummy_dataframe)

        dummy_dataframe.write_database.assert_called_once_with(
            table_name=TABLE_NAME,
            connection=CONNECTION,
            if_table_exists="replace",
        )

    def test_save_no_table_name(self, dummy_dataframe):
        """Test ``save`` raises when no ``table_name`` was configured."""
        dataset = PolarsDatabaseDataset(
            sql=SQL_QUERY,
            table_name=TABLE_NAME,
            credentials={"con": CONNECTION},
        )
        dataset.table_name = None
        pattern = r"'table_name' argument is required to save datasets\."
        with pytest.raises(DatasetError, match=pattern):
            dataset.save(dummy_dataframe)

    def test_str_representation_sql(self):
        """Test the dataset instance string representation with the ``sql`` arg."""
        dataset = PolarsDatabaseDataset(
            sql=SQL_QUERY,
            table_name=TABLE_NAME,
            credentials={"con": CONNECTION},
        )
        str_repr = str(dataset)
        assert f"sql='{SQL_QUERY}'" in str_repr
        assert f"table_name='{TABLE_NAME}'" in str_repr
        assert CONNECTION not in str_repr

    def test_str_representation_filepath(self, sql_file):
        """Test the dataset instance string representation with the ``filepath`` arg."""
        dataset = PolarsDatabaseDataset(
            filepath=sql_file,
            table_name=TABLE_NAME,
            credentials={"con": CONNECTION},
        )
        str_repr = str(dataset)
        assert f"filepath='{sql_file}'" in str_repr
        assert f"table_name='{TABLE_NAME}'" in str_repr
        assert CONNECTION not in str_repr

    def test_save_and_load_roundtrip(self, sqlite_credentials, dummy_dataframe):
        """Regression test for the docstring example (issue #1384)."""
        dataset = PolarsDatabaseDataset(
            sql=SQL_QUERY,
            table_name=TABLE_NAME,
            credentials=sqlite_credentials,
        )
        dataset.save(dummy_dataframe)
        reloaded = dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded)

    def test_load_table_name_only_roundtrip(self, sqlite_credentials, dummy_dataframe):
        """End-to-end test for table-name-only mode: write with sql + load with table_name."""
        writer = PolarsDatabaseDataset(
            sql=SQL_QUERY,
            table_name=TABLE_NAME,
            credentials=sqlite_credentials,
        )
        writer.save(dummy_dataframe)

        reader = PolarsDatabaseDataset(
            table_name=TABLE_NAME, credentials=sqlite_credentials
        )
        assert_frame_equal(dummy_dataframe, reader.load())
