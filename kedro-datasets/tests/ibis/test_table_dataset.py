import duckdb
import ibis
import pytest
from packaging.version import Version
from pandas.testing import assert_frame_equal

from kedro_datasets.ibis import FileDataset, TableDataset

_SENTINEL = object()


@pytest.fixture(scope="session")
def filepath_csv(tmp_path_factory):
    path = (tmp_path_factory.mktemp("data") / "test.csv").as_posix()
    ibis.memtable({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]}).to_csv(path)
    return path


@pytest.fixture
def database(tmp_path):
    return (tmp_path / "file.db").as_posix()


@pytest.fixture(params=[None])
def database_name(request):
    return request.param


@pytest.fixture(params=[_SENTINEL])
def connection_config(request, database):
    return (
        {"backend": "duckdb", "database": database}
        if request.param is _SENTINEL  # `None` is a valid value to test
        else request.param
    )


@pytest.fixture
def table_dataset(database_name, connection_config, load_args, save_args):
    return TableDataset(
        table_name="test",
        database=database_name,
        connection=connection_config,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture
def dummy_table():
    return ibis.memtable({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


@pytest.fixture
def file_dataset(filepath_csv, connection_config, load_args, save_args):
    return FileDataset(
        filepath=filepath_csv,
        file_format="csv",
        connection=connection_config,
        load_args=load_args,
        save_args=save_args,
    )


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

    @pytest.mark.parametrize(
        "connection_config", [{"backend": "polars"}], indirect=True
    )
    @pytest.mark.parametrize("save_args", [{"materialized": "table"}], indirect=True)
    def test_save_and_load_polars(
        self, table_dataset, connection_config, save_args, dummy_table
    ):
        """Test saving and reloading the dataset configured with Polars.

        If and when the Polars backend handles the `database` parameter,
        this test can be removed. Additionally, the `create_view` method
        is supported since Ibis 9.1.0, so `save_args` doesn't need to be
        overridden.

        """
        table_dataset.save(dummy_table)
        reloaded = table_dataset.load()
        assert_frame_equal(dummy_table.execute(), reloaded.execute())

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

    @pytest.mark.parametrize("database_name", ["test"], indirect=True)
    def test_external_database(
        self, tmp_path, table_dataset, database_name, dummy_table, database
    ):
        """Test passing the database name to read from and create in."""
        # Attach another DuckDB database to the existing DuckDB session.
        table_dataset.connection.attach(tmp_path / f"{database_name}.db")

        table_dataset.save(dummy_table)
        reloaded = table_dataset.load()
        assert_frame_equal(dummy_table.execute(), reloaded.execute())

        # Verify that the attached database file was the one written to.
        con = duckdb.connect(database)
        assert (
            "test"
            in con.sql("SELECT * FROM duckdb_views").fetchnumpy()["database_name"]
        )

    @pytest.mark.skipif(
        Version(ibis.__version__) < Version("9.0.0"),
        reason='Ibis 9.0 standardised use of "database" to mean a collection of tables',
    )
    @pytest.mark.parametrize("database_name", ["test"], indirect=True)
    def test_database(
        self, tmp_path, table_dataset, database_name, dummy_table, database
    ):
        """Test passing the database name to read from and create in."""
        # Create a database (meaning a collection of tables, or schema).
        # To learn more about why Ibis uses "database" in this way, read
        # https://ibis-project.org/posts/ibis-version-9.0.0-release/#what-does-schema-mean
        table_dataset.connection.create_database(database_name)

        table_dataset.save(dummy_table)
        reloaded = table_dataset.load()
        assert_frame_equal(dummy_table.execute(), reloaded.execute())

        # Verify that the attached database file was the one written to.
        con = duckdb.connect(database)
        assert (
            "test" in con.sql("SELECT * FROM duckdb_views").fetchnumpy()["schema_name"]
        )

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
        assert ("ibis", key) in table_dataset._connections

    def test_save_data_loaded_using_file_dataset(self, file_dataset, table_dataset):
        """Test interoperability of Ibis datasets sharing a database."""
        dummy_table = file_dataset.load()
        assert not table_dataset.exists()
        table_dataset.save(dummy_table)
        assert table_dataset.exists()
