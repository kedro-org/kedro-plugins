import ibis
import pytest
from pandas.testing import assert_frame_equal

from kedro_datasets.ibis import FileDataset


@pytest.fixture
def filepath_csv(tmp_path):
    return (tmp_path / "test.csv").as_posix()


@pytest.fixture
def database(tmp_path):
    return (tmp_path / "file.db").as_posix()


@pytest.fixture(params=[None])
def connection_config(request, database):
    return request.param or {"backend": "duckdb", "database": database}


@pytest.fixture
def file_dataset(filepath_csv, connection_config, load_args, save_args):
    return FileDataset(
        filepath=filepath_csv,
        file_format="csv",
        connection=connection_config,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture
def dummy_table():
    return ibis.memtable({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


class TestFileDataset:
    def test_save_and_load(self, file_dataset, dummy_table, database):
        """Test saving and reloading the data set."""
        file_dataset.save(dummy_table)
        reloaded = file_dataset.load()
        assert_frame_equal(dummy_table.execute(), reloaded.execute())

    @pytest.mark.parametrize("load_args", [{"filename": True}], indirect=True)
    def test_load_extra_params(self, file_dataset, load_args, dummy_table):
        """Test overriding the default load arguments."""
        file_dataset.save(dummy_table)
        assert "filename" in file_dataset.load()

    @pytest.mark.parametrize("save_args", [{"sep": "|"}], indirect=True)
    def test_save_extra_params(
        self, file_dataset, save_args, dummy_table, filepath_csv
    ):
        """Test overriding the default save arguments."""
        file_dataset.save(dummy_table)

        # Verify that the delimiter character from `save_args` was used.
        with open(filepath_csv) as f:
            for line in f:
                assert save_args["sep"] in line

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
    def test_connection_config(self, mocker, file_dataset, connection_config, key):
        """Test hashing of more complicated connection configuration."""
        mocker.patch(f"ibis.{connection_config['backend']}")
        file_dataset.load()
        assert key in file_dataset._connections
