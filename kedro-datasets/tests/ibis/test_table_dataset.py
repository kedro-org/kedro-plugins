import ibis
import pytest
from pandas.testing import assert_frame_equal

from kedro_datasets.ibis import TableDataset


@pytest.fixture(scope="session")
def filepath_csv(tmp_path_factory):
    path = (tmp_path_factory.mktemp("data") / "penguins.csv").as_posix()
    ibis.examples.penguins.fetch().to_csv(path)
    return path


@pytest.fixture
def connection_config(tmp_path):
    return {"backend": "duckdb", "database": (tmp_path / "penguins.ddb").as_posix()}


@pytest.fixture
def table_dataset(connection_config, load_args, save_args):
    return TableDataset(
        table_name="penguins",
        connection=connection_config,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture
def dummy_table(filepath_csv, connection_config):
    return TableDataset(
        filepath=filepath_csv, file_format="csv", connection=connection_config
    ).load()


class TestTableDataset:
    def test_save_and_load(self, table_dataset, dummy_table):
        """Test saving and reloading the data set."""
        table_dataset.save(dummy_table)
        reloaded = table_dataset.load()
        assert_frame_equal(dummy_table.execute(), reloaded.execute())

    def test_exists(self, table_dataset, dummy_table):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not table_dataset.exists()
        table_dataset.save(dummy_table)
        assert table_dataset.exists()

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, table_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert table_dataset._load_args[key] == value

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, table_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert table_dataset._save_args[key] == value
