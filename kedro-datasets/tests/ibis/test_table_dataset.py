import duckdb
import ibis
import pytest
from kedro.io import DatasetError
from pandas.testing import assert_frame_equal

from kedro_datasets.ibis import TableDataset


@pytest.fixture(scope="session")
def filepath_csv(tmp_path_factory):
    path = (tmp_path_factory.mktemp("data") / "penguins.csv").as_posix()
    ibis.examples.penguins.fetch().to_csv(path)
    return path


@pytest.fixture
def database(tmp_path):
    return (tmp_path / "penguins.ddb").as_posix()


@pytest.fixture
def connection_config(database):
    return {"backend": "duckdb", "database": database}


@pytest.fixture
def table_dataset(connection_config, load_args, save_args):
    return TableDataset(
        table_name="penguins",
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
        """Test saving and reloading the data set."""
        table_dataset.save(dummy_table)
        reloaded = table_dataset.load()
        assert_frame_equal(dummy_table.execute(), reloaded.execute())

        # Verify that the appropriate materialization strategy was used.
        con = duckdb.connect(database)
        assert not con.sql("SELECT * FROM duckdb_tables").fetchnumpy()["database_name"]
        assert (
            "penguins"
            in con.sql("SELECT * FROM duckdb_views").fetchnumpy()["database_name"]
        )

    def test_exists(self, table_dataset, dummy_table):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
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
            "penguins"
            in con.sql("SELECT * FROM duckdb_tables").fetchnumpy()["database_name"]
        )
        assert not con.sql("SELECT * FROM duckdb_views").fetchnumpy()["database_name"]

    def test_no_filepath_or_table_name(connection_config):
        pattern = r"Must provide at least one of `filepath` or `table_name`\."
        with pytest.raises(DatasetError, match=pattern):
            TableDataset(connection=connection_config)

    def test_save_no_table_name(self, table_dataset_from_csv, dummy_table):
        pattern = r"Must provide `table_name` for materialization\."
        with pytest.raises(DatasetError, match=pattern):
            table_dataset_from_csv.save(dummy_table)
