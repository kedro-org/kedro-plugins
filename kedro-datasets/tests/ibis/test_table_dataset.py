import duckdb
import ibis
import pandas as pd
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

    @pytest.mark.parametrize(
        "save_args",
        [{"materialized": "table", "mode": "append"}],
        indirect=True,
    )
    def test_save_mode_append(self, table_dataset):
        """Saving with mode=append should add rows to an existing table."""
        df1 = pd.DataFrame({"col1": [1], "col2": [2], "col3": [3]})
        df2 = pd.DataFrame({"col1": [4], "col2": [5], "col3": [6]})

        table_dataset.save(df1)
        table_dataset.save(df2)

        reloaded = table_dataset.load().execute()
        assert len(reloaded) == len(df1) + len(df2)

    @pytest.mark.parametrize(
        "save_args",
        [
            {"materialized": "table", "mode": "error"},
            {"materialized": "table", "mode": "errorifexists"},
        ],
        indirect=True,
    )
    def test_save_mode_error_variants(self, table_dataset):
        """Saving with error/errorifexists should raise when table exists."""
        df = pd.DataFrame({"col1": [1], "col2": [2], "col3": [3]})
        table_dataset.save(df)
        with pytest.raises(Exception):
            table_dataset.save(df)

    @pytest.mark.parametrize(
        "save_args",
        [{"materialized": "table", "mode": "ignore"}],
        indirect=True,
    )
    def test_save_mode_ignore(self, table_dataset):
        """Saving with ignore should not change existing table."""
        df1 = pd.DataFrame({"col1": [1], "col2": [2], "col3": [3]})
        df2 = pd.DataFrame({"col1": [10], "col2": [20], "col3": [30]})

        table_dataset.save(df1)
        table_dataset.save(df2)

        reloaded = table_dataset.load().execute()
        # Should remain as first save only
        assert_frame_equal(reloaded.reset_index(drop=True), df1.reset_index(drop=True))

    def test_legacy_overwrite_conflict_raises(self, database):
        """Providing both mode and overwrite should raise a ValueError."""
        with pytest.raises(ValueError):
            TableDataset(
                table_name="conflict",
                connection={"backend": "duckdb", "database": database},
                save_args={"materialized": "table", "mode": "append", "overwrite": True},
            )

    @pytest.mark.parametrize("legacy_overwrite", [True, False])
    def test_legacy_overwrite_behavior(self, database, legacy_overwrite):
        """Legacy overwrite should map to overwrite or error behavior."""
        ds = TableDataset(
            table_name="legacy_overwrite",
            connection={"backend": "duckdb", "database": database},
            save_args={"materialized": "table", "overwrite": legacy_overwrite},
        )
        df1 = pd.DataFrame({"col1": [1], "col2": [2], "col3": [3]})
        df2 = pd.DataFrame({"col1": [7], "col2": [8], "col3": [9]})

        ds.save(df1)
        if legacy_overwrite:
            # Should overwrite existing table with new contents
            ds.save(df2)
            out = ds.load().execute().reset_index(drop=True)
            assert_frame_equal(out, df2.reset_index(drop=True))
        else:
            # Should raise on second save when table exists
            with pytest.raises(Exception):
                ds.save(df2)

    def test_credentials_precedence_and_warning(self, database):
        """When both are given, credentials take precedence and warn."""
        with pytest.warns(DeprecationWarning):
            ds = TableDataset(
                table_name="cred_pref",
                database=None,
                credentials={"backend": "duckdb", "database": database},
                connection={"backend": "polars"},
                save_args={"materialized": "table"},
            )

        # Uses backend from credentials
        desc = ds._describe()
        assert desc["backend"] == "duckdb"
        # Smoke test load path by creating then reloading
        df = pd.DataFrame({"col1": [1], "col2": [2], "col3": [3]})
        ds.save(df)
        assert_frame_equal(ds.load().execute().reset_index(drop=True), df.reset_index(drop=True))

    def test_describe_includes_backend_mode_and_materialized(self, table_dataset):
        """_describe should expose backend, mode and materialized; nested args exclude database."""
        desc = table_dataset._describe()
        assert {"backend", "mode", "materialized"}.issubset(desc.keys())
        assert "database" in desc
        # database key should not be duplicated inside nested args
        assert "database" not in desc["load_args"]
        assert "database" not in desc["save_args"]

    def test_save_empty_dataframe_is_noop(self, database):
        """Saving an empty DataFrame should be a no-op (no table created)."""
        ds = TableDataset(
            table_name="empty_noop",
            connection={"backend": "duckdb", "database": database},
            save_args={"materialized": "table"},
        )
        empty_df = pd.DataFrame({"col1": [], "col2": [], "col3": []})
        ds.save(empty_df)
        assert not ds.exists()

    @pytest.mark.parametrize("load_args", [{"database": "test"}], indirect=True)
    def test_load_extra_params(self, table_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert table_dataset._load_args[key] == value

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
