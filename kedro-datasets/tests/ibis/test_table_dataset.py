import duckdb
import ibis
import pytest
from kedro.io import DatasetError
from pandas.testing import assert_frame_equal

from kedro_datasets.ibis import TableDataset


@pytest.fixture(scope="session")
def filepath_csv(tmp_path_factory):
    path = (tmp_path_factory.mktemp("data") / "test.csv").as_posix()
    ibis.memtable({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]}).to_csv(path)
    return path


@pytest.fixture
def database(tmp_path):
    return (tmp_path / "file.db").as_posix()


@pytest.fixture(params=[None])
def connection_config(request, database):
    return request.param or {"backend": "duckdb", "database": database}


@pytest.fixture
def table_dataset(connection_config, load_args, save_args):
    return TableDataset(
        table_name="test",
        connection=connection_config,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture
def table_dataset_from_csv(filepath_csv, connection_config, load_args, save_args):
    return TableDataset(
        filepath=filepath_csv,
        file_format="csv",
        connection=connection_config,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture
def dummy_table(table_dataset_from_csv):
    return table_dataset_from_csv.load()


class TestTableDataset:
    def test_save_and_load(self, table_dataset, dummy_table, database):
        """Test saving and reloading the dataset."""
        table_dataset.save(dummy_table)
        reloaded = table_dataset.load()
        assert_frame_equal(dummy_table.execute(), reloaded.execute())

        # Verify that the appropriate materialization strategy was used.
        con = duckdb.connect(database)
        assert not con.sql("SELECT * FROM duckdb_tables").fetchnumpy()["table_name"]
        assert "test" in con.sql("SELECT * FROM duckdb_views").fetchnumpy()["view_name"]

    def test_exists(self, table_dataset, dummy_table):
        """Test `exists` method invocation for both existing and
        nonexistent dataset."""
        assert not table_dataset.exists()
        table_dataset.save(dummy_table)
        assert table_dataset.exists()

    @pytest.mark.parametrize("load_args", [{"filename": True}], indirect=True)
    def test_load_extra_params(self, table_dataset_from_csv, load_args):
        """Test overriding the default load arguments."""
        assert "filename" in table_dataset_from_csv.load()

    @pytest.mark.parametrize("save_args", [{"materialized": "table"}], indirect=True)
    def test_save_extra_params(self, table_dataset, save_args, dummy_table, database):
        """Test overriding the default save arguments."""
        table_dataset.save(dummy_table)

        # Verify that the appropriate materialization strategy was used.
        con = duckdb.connect(database)
        assert (
            "test" in con.sql("SELECT * FROM duckdb_tables").fetchnumpy()["table_name"]
        )
        assert not con.sql("SELECT * FROM duckdb_views").fetchnumpy()["view_name"]

    def test_no_filepath_or_table_name(connection_config):
        pattern = r"Must provide at least one of `filepath` or `table_name`\."
        with pytest.raises(DatasetError, match=pattern):
            TableDataset(connection=connection_config)

    def test_save_no_table_name(self, table_dataset_from_csv, dummy_table):
        pattern = r"Must provide `table_name` for materialization\."
        with pytest.raises(DatasetError, match=pattern):
            table_dataset_from_csv.save(dummy_table)

    @pytest.mark.parametrize(
        ("connection_config", "key"),
        [
            (
                {"backend": "duckdb", "database": "file.db", "extensions": ["spatial"]},
                (
                    ("backend", "duckdb"),
                    ("database", "file.db"),
                    ("extensions", ("spatial",)),
                ),
            ),
            # https://github.com/kedro-org/kedro-plugins/pull/560#discussion_r1536083525
            (
                {
                    "host": "xxx.sql.azuresynapse.net",
                    "database": "xxx",
                    "query": {"driver": "ODBC Driver 17 for SQL Server"},
                    "backend": "mssql",
                },
                (
                    ("backend", "mssql"),
                    ("database", "xxx"),
                    ("host", "xxx.sql.azuresynapse.net"),
                    ("query", (("driver", "ODBC Driver 17 for SQL Server"),)),
                ),
            ),
        ],
        indirect=["connection_config"],
    )
    def test_connection_config(self, mocker, table_dataset, connection_config, key):
        """Test hashing of more complicated connection configuration."""
        mocker.patch(f"ibis.{connection_config['backend']}")
        table_dataset.load()
        assert key in table_dataset._connections
