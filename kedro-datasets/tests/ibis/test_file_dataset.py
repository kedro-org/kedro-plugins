import sys
from pathlib import Path
from time import sleep

import ibis
import pytest
from kedro.io import DatasetError, Version
from kedro.io.core import generate_timestamp
from pandas.testing import assert_frame_equal

from kedro_datasets.ibis import FileDataset

_SENTINEL = object()


@pytest.fixture
def filepath_csv(tmp_path):
    return (tmp_path / "test.csv").as_posix()


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
def file_dataset(filepath_csv, connection_config, load_args, save_args):
    return FileDataset(
        filepath=filepath_csv,
        file_format="csv",
        connection=connection_config,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture
def versioned_file_dataset(filepath_csv, connection_config, load_version, save_version):
    return FileDataset(
        filepath=filepath_csv,
        file_format="csv",
        connection=connection_config,
        version=Version(load_version, save_version),
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
    def test_connection_config(self, mocker, file_dataset, connection_config, key):
        """Test hashing of more complicated connection configuration."""
        backend = (
            connection_config["backend"] if connection_config is not None else "duckdb"
        )
        mocker.patch(f"ibis.{backend}")
        file_dataset.load()
        assert key in file_dataset._connections


class TestFileDatasetVersioned:
    def test_version_str_repr(self, connection_config, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.csv"
        ds = FileDataset(
            filepath=filepath, file_format="csv", connection=connection_config
        )
        ds_versioned = FileDataset(
            filepath=filepath,
            file_format="csv",
            connection=connection_config,
            version=Version(load_version, save_version),
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "FileDataset" in str(ds_versioned)
        assert "FileDataset" in str(ds)
        # Default save_args
        assert "save_args={}" in str(ds)
        assert "save_args={}" in str(ds_versioned)

    def test_save_and_load(self, versioned_file_dataset, dummy_table):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_file_dataset.save(dummy_table)
        reloaded = versioned_file_dataset.load()
        assert_frame_equal(dummy_table.execute(), reloaded.execute())

    def test_multiple_loads(
        self, versioned_file_dataset, dummy_table, filepath_csv, connection_config
    ):
        """Test that if a new version is created mid-run, by an
        external system, it won't be loaded in the current run."""
        versioned_file_dataset.save(dummy_table)
        versioned_file_dataset.load()
        v1 = versioned_file_dataset.resolve_load_version()

        sleep(0.5)
        # force-drop a newer version into the same location
        v_new = generate_timestamp()
        FileDataset(
            filepath=filepath_csv,
            file_format="csv",
            connection=connection_config,
            version=Version(v_new, v_new),
        ).save(dummy_table)

        versioned_file_dataset.load()
        v2 = versioned_file_dataset.resolve_load_version()

        assert v2 == v1  # v2 should not be v_new!
        ds_new = FileDataset(
            filepath=filepath_csv,
            file_format="csv",
            connection=connection_config,
            version=Version(None, None),
        )
        assert (
            ds_new.resolve_load_version() == v_new
        )  # new version is discoverable by a new instance

    def test_multiple_saves(self, dummy_table, filepath_csv, connection_config):
        """Test multiple cycles of save followed by load for the same dataset"""
        ds_versioned = FileDataset(
            filepath=filepath_csv,
            file_format="csv",
            connection=connection_config,
            version=Version(None, None),
        )

        # first save
        ds_versioned.save(dummy_table)
        first_save_version = ds_versioned.resolve_save_version()
        first_load_version = ds_versioned.resolve_load_version()
        assert first_load_version == first_save_version

        # second save
        sleep(0.5)
        ds_versioned.save(dummy_table)
        second_save_version = ds_versioned.resolve_save_version()
        second_load_version = ds_versioned.resolve_load_version()
        assert second_load_version == second_save_version
        assert second_load_version > first_load_version

        # another dataset
        ds_new = FileDataset(
            filepath=filepath_csv,
            file_format="csv",
            connection=connection_config,
            version=Version(None, None),
        )
        assert ds_new.resolve_load_version() == second_load_version

    def test_no_versions(self, versioned_file_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for FileDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_file_dataset.load()

    def test_exists(self, versioned_file_dataset, dummy_table):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_file_dataset.exists()
        versioned_file_dataset.save(dummy_table)
        assert versioned_file_dataset.exists()

    def test_prevent_overwrite(self, versioned_file_dataset, dummy_table):
        """Check the error when attempting to override the data set if the
        corresponding CSV file for a given save version already exists."""
        versioned_file_dataset.save(dummy_table)
        pattern = (
            r"Save path \'.+\' for FileDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_file_dataset.save(dummy_table)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_file_dataset, load_version, save_version, dummy_table
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for FileDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_file_dataset.save(dummy_table)

    @pytest.mark.skipif(sys.platform == "win32", reason="different error on windows")
    def test_versioning_existing_dataset(
        self, file_dataset, versioned_file_dataset, dummy_table
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        file_dataset.save(dummy_table)
        assert file_dataset.exists()
        assert file_dataset._filepath == versioned_file_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_file_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_file_dataset.save(dummy_table)

        # Remove non-versioned dataset and try again
        Path(file_dataset._filepath.as_posix()).unlink()
        versioned_file_dataset.save(dummy_table)
        assert versioned_file_dataset.exists()
