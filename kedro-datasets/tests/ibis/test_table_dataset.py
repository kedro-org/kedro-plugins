import duckdb
import ibis
import pytest
from kedro.io import DatasetError
from pandas.testing import assert_frame_equal

from kedro_datasets.ibis import TableDataset

_SENTINEL = object()


@pytest.fixture
def database(tmp_path):
    return (tmp_path / "file.db").as_posix()


@pytest.fixture(params=[_SENTINEL])
def connection_config(request, database):
    return (
        {"backend": "duckdb", "database": database}
        if request.param is _SENTINEL  # `None` is a valid value to test
        else request.param
    )


@pytest.fixture
def table_dataset(connection_config, save_args):
    return TableDataset(
        table_name="test",
        connection=connection_config,
        save_args=save_args,
    )


@pytest.fixture
def dummy_table():
    return ibis.memtable({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


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

    def test_no_table_name(connection_config):
        pattern = r"Must provide `table_name`\."
        with pytest.raises(DatasetError, match=pattern):
            TableDataset(connection=connection_config)

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
            # https://github.com/kedro-org/kedro-plugins/pull/893#discussion_r1804632435
            (
                None,
                (
                    ("backend", "duckdb"),
                    ("database", ":memory:"),
                ),
            ),
        ],
        indirect=["connection_config"],
    )
    def test_connection_config(self, mocker, table_dataset, connection_config, key):
        """Test hashing of more complicated connection configuration."""
        backend = (
            connection_config["backend"] if connection_config is not None else "duckdb"
        )
        mocker.patch(f"ibis.{backend}")
        table_dataset.load()
        assert key in table_dataset._connections
