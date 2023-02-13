from pathlib import Path, PurePosixPath
from time import sleep

import pandas as pd
import polars as pl
import pytest
from adlfs import AzureBlobFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import DataSetError, Version
from kedro.io.core import PROTOCOL_DELIMITER, generate_timestamp
from polars.testing import assert_frame_equal
from s3fs import S3FileSystem

from kedro_datasets.polars import GenericDataSet


@pytest.fixture
def filepath_csv(tmp_path):
    return tmp_path / "test.csv"


@pytest.fixture
def filepath_ipc(tmp_path):
    return tmp_path / "test.arrow"


@pytest.fixture
def filepath_parquet(tmp_path):
    return tmp_path / "test.parquet"


@pytest.fixture
def versioned_csv_data_set(filepath_csv, load_version, save_version):
    return GenericDataSet(
        filepath=filepath_csv.as_posix(),
        file_format="csv",
        version=Version(load_version, save_version),
        save_args={},
    )


@pytest.fixture
def versioned_ipc_data_set(filepath_ipc, load_version, save_version):
    return GenericDataSet(
        filepath=filepath_ipc.as_posix(),
        file_format="ipc",
        version=Version(load_version, save_version),
        save_args={},
    )


@pytest.fixture
def versioned_parquet_data_set(filepath_parquet, load_version, save_version):
    return GenericDataSet(
        filepath=filepath_parquet.as_posix(),
        file_format="parquet",
        version=Version(load_version, save_version),
        save_args={},
    )


@pytest.fixture
def csv_data_set(filepath_csv):
    return GenericDataSet(
        filepath=filepath_csv.as_posix(),
        file_format="csv",
    )


@pytest.fixture
def dummy_dataframe():
    return pl.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


@pytest.fixture
def filepath_excel(tmp_path):
    return tmp_path / "test.xlsx"


@pytest.fixture
def excel_data_set_ignore(dummy_dataframe: pl.DataFrame, filepath_excel):
    pd_df = dummy_dataframe.to_pandas()
    pd_df.to_excel(filepath_excel, index=False)

    return GenericDataSet(
        filepath=filepath_excel.as_posix(),
        file_format="excel",
        write_mode="ignore",
    )


@pytest.fixture
def excel_data_set_overwrite(dummy_dataframe: pl.DataFrame, filepath_excel):
    pd_df = dummy_dataframe.to_pandas()
    pd_df.to_excel(filepath_excel, index=False)

    return GenericDataSet(
        filepath=filepath_excel.as_posix(),
        file_format="excel",
        write_mode="overwrite",
    )


class TestGenericExcelDataSet:
    def test_load(self, excel_data_set_ignore):
        df = excel_data_set_ignore.load()
        assert df.shape == (2, 3)

    def test_save_fail_write_mode(self, excel_data_set_ignore, dummy_dataframe):
        pattern = "Write mode 'ignore' is read-only"
        with pytest.raises(DataSetError, match=pattern):
            excel_data_set_ignore.save(dummy_dataframe)

    def test_save_fail_format(self, excel_data_set_overwrite, dummy_dataframe):
        pattern = (
            "This file format is read-only: 'excel' "
            "If you want only to read, change write_mode to 'ignore'"
        )
        with pytest.raises(DataSetError, match=pattern):
            excel_data_set_overwrite.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "filepath,instance_type,credentials",
        [
            ("s3://bucket/file.xlsx", S3FileSystem, {}),
            ("file:///tmp/test.xlsx", LocalFileSystem, {}),
            ("/tmp/test.xlsx", LocalFileSystem, {}),
            ("gcs://bucket/file.xlsx", GCSFileSystem, {}),
            ("https://example.com/file.xlsx", HTTPFileSystem, {}),
            (
                "abfs://bucket/file.xlsx",
                AzureBlobFileSystem,
                {"account_name": "test", "account_key": "test"},
            ),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type, credentials):
        data_set = GenericDataSet(
            filepath=filepath,
            file_format="excel",
            credentials=credentials,
            write_mode="ignore",
        )
        assert isinstance(data_set._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(data_set._filepath) == path
        assert isinstance(data_set._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.csv"
        data_set = GenericDataSet(filepath=filepath, file_format="excel")
        assert data_set._version_cache.currsize == 0  # no cache if unversioned
        data_set.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)
        assert data_set._version_cache.currsize == 0


class TestGenericParquetDataSetVersioned:
    def test_save_and_load(self, versioned_parquet_data_set, dummy_dataframe):
        """Test saving and reloading the data set."""
        versioned_parquet_data_set.save(dummy_dataframe)
        reloaded_df = versioned_parquet_data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_version_str_repr(self, filepath_parquet, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = filepath_parquet.as_posix()
        ds = GenericDataSet(filepath=filepath, file_format="parquet")
        ds_versioned = GenericDataSet(
            filepath=filepath,
            file_format="parquet",
            version=Version(load_version, save_version),
        )
        assert filepath in str(ds)
        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "GenericDataSet" in str(ds_versioned)
        assert "GenericDataSet" in str(ds)

    def test_multiple_loads(
        self, versioned_parquet_data_set, dummy_dataframe, filepath_parquet
    ):
        """Test that if a new version is created mid-run, by an
        external system, it won't be loaded in the current run."""
        versioned_parquet_data_set.save(dummy_dataframe)
        versioned_parquet_data_set.load()
        v1 = versioned_parquet_data_set.resolve_load_version()

        sleep(0.5)
        # force-drop a newer version into the same location
        v_new = generate_timestamp()
        GenericDataSet(
            filepath=filepath_parquet.as_posix(),
            file_format="parquet",
            version=Version(v_new, v_new),
        ).save(dummy_dataframe)

        versioned_parquet_data_set.load()
        v2 = versioned_parquet_data_set.resolve_load_version()

        assert v2 == v1  # v2 should not be v_new!
        ds_new = GenericDataSet(
            filepath=filepath_parquet.as_posix(),
            file_format="parquet",
            version=Version(None, None),
        )
        assert (
            ds_new.resolve_load_version() == v_new
        )  # new version is discoverable by a new instance

    def test_multiple_saves(self, dummy_dataframe, filepath_parquet):
        """Test multiple cycles of save followed by load for the same dataset"""
        ds_versioned = GenericDataSet(
            filepath=filepath_parquet.as_posix(),
            file_format="parquet",
            version=Version(None, None),
        )

        # first save
        ds_versioned.save(dummy_dataframe)
        first_save_version = ds_versioned.resolve_save_version()
        first_load_version = ds_versioned.resolve_load_version()
        assert first_load_version == first_save_version

        # second save
        sleep(0.5)
        ds_versioned.save(dummy_dataframe)
        second_save_version = ds_versioned.resolve_save_version()
        second_load_version = ds_versioned.resolve_load_version()
        assert second_load_version == second_save_version
        assert second_load_version > first_load_version

        # another dataset
        ds_new = GenericDataSet(
            filepath=filepath_parquet.as_posix(),
            file_format="parquet",
            version=Version(None, None),
        )
        assert ds_new.resolve_load_version() == second_load_version


class TestGenericIPCDataSetVersioned:
    def test_save_and_load(self, versioned_ipc_data_set, dummy_dataframe):
        """Test saving and reloading the data set."""
        versioned_ipc_data_set.save(dummy_dataframe)
        reloaded_df = versioned_ipc_data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_version_str_repr(self, filepath_ipc, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = filepath_ipc.as_posix()
        ds = GenericDataSet(filepath=filepath, file_format="ipc")
        ds_versioned = GenericDataSet(
            filepath=filepath,
            file_format="ipc",
            version=Version(load_version, save_version),
        )
        assert filepath in str(ds)
        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "GenericDataSet" in str(ds_versioned)
        assert "GenericDataSet" in str(ds)

    def test_multiple_loads(
        self, versioned_ipc_data_set, dummy_dataframe, filepath_ipc
    ):
        """Test that if a new version is created mid-run, by an
        external system, it won't be loaded in the current run."""
        versioned_ipc_data_set.save(dummy_dataframe)
        versioned_ipc_data_set.load()
        v1 = versioned_ipc_data_set.resolve_load_version()

        sleep(0.5)
        # force-drop a newer version into the same location
        v_new = generate_timestamp()
        GenericDataSet(
            filepath=filepath_ipc.as_posix(),
            file_format="ipc",
            version=Version(v_new, v_new),
        ).save(dummy_dataframe)

        versioned_ipc_data_set.load()
        v2 = versioned_ipc_data_set.resolve_load_version()

        assert v2 == v1  # v2 should not be v_new!
        ds_new = GenericDataSet(
            filepath=filepath_ipc.as_posix(),
            file_format="ipc",
            version=Version(None, None),
        )
        assert (
            ds_new.resolve_load_version() == v_new
        )  # new version is discoverable by a new instance

    def test_multiple_saves(self, dummy_dataframe, filepath_ipc):
        """Test multiple cycles of save followed by load for the same dataset"""
        ds_versioned = GenericDataSet(
            filepath=filepath_ipc.as_posix(),
            file_format="ipc",
            version=Version(None, None),
        )

        # first save
        ds_versioned.save(dummy_dataframe)
        first_save_version = ds_versioned.resolve_save_version()
        first_load_version = ds_versioned.resolve_load_version()
        assert first_load_version == first_save_version

        # second save
        sleep(0.5)
        ds_versioned.save(dummy_dataframe)
        second_save_version = ds_versioned.resolve_save_version()
        second_load_version = ds_versioned.resolve_load_version()
        assert second_load_version == second_save_version
        assert second_load_version > first_load_version

        # another dataset
        ds_new = GenericDataSet(
            filepath=filepath_ipc.as_posix(),
            file_format="ipc",
            version=Version(None, None),
        )
        assert ds_new.resolve_load_version() == second_load_version


class TestGenericCSVDataSetVersioned:
    def test_version_str_repr(self, filepath_csv, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = filepath_csv.as_posix()
        ds = GenericDataSet(filepath=filepath, file_format="csv")
        ds_versioned = GenericDataSet(
            filepath=filepath,
            file_format="csv",
            version=Version(load_version, save_version),
        )
        assert filepath in str(ds)
        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "GenericDataSet" in str(ds_versioned)
        assert "GenericDataSet" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_save_and_load(self, versioned_csv_data_set, dummy_dataframe):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_csv_data_set.save(dummy_dataframe)
        reloaded_df = versioned_csv_data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_multiple_loads(
        self, versioned_csv_data_set, dummy_dataframe, filepath_csv
    ):
        """Test that if a new version is created mid-run, by an
        external system, it won't be loaded in the current run."""
        versioned_csv_data_set.save(dummy_dataframe)
        versioned_csv_data_set.load()
        v1 = versioned_csv_data_set.resolve_load_version()

        sleep(0.5)
        # force-drop a newer version into the same location
        v_new = generate_timestamp()
        GenericDataSet(
            filepath=filepath_csv.as_posix(),
            file_format="csv",
            version=Version(v_new, v_new),
        ).save(dummy_dataframe)

        versioned_csv_data_set.load()
        v2 = versioned_csv_data_set.resolve_load_version()

        assert v2 == v1  # v2 should not be v_new!
        ds_new = GenericDataSet(
            filepath=filepath_csv.as_posix(),
            file_format="csv",
            version=Version(None, None),
        )
        assert (
            ds_new.resolve_load_version() == v_new
        )  # new version is discoverable by a new instance

    def test_multiple_saves(self, dummy_dataframe, filepath_csv):
        """Test multiple cycles of save followed by load for the same dataset"""
        ds_versioned = GenericDataSet(
            filepath=filepath_csv.as_posix(),
            file_format="csv",
            version=Version(None, None),
        )

        # first save
        ds_versioned.save(dummy_dataframe)
        first_save_version = ds_versioned.resolve_save_version()
        first_load_version = ds_versioned.resolve_load_version()
        assert first_load_version == first_save_version

        # second save
        sleep(0.5)
        ds_versioned.save(dummy_dataframe)
        second_save_version = ds_versioned.resolve_save_version()
        second_load_version = ds_versioned.resolve_load_version()
        assert second_load_version == second_save_version
        assert second_load_version > first_load_version

        # another dataset
        ds_new = GenericDataSet(
            filepath=filepath_csv.as_posix(),
            file_format="csv",
            version=Version(None, None),
        )
        assert ds_new.resolve_load_version() == second_load_version

    def test_release_instance_cache(self, dummy_dataframe, filepath_csv):
        """Test that cache invalidation does not affect other instances"""
        ds_a = GenericDataSet(
            filepath=filepath_csv.as_posix(),
            file_format="csv",
            version=Version(None, None),
        )
        assert ds_a._version_cache.currsize == 0
        ds_a.save(dummy_dataframe)  # create a version
        assert ds_a._version_cache.currsize == 2

        ds_b = GenericDataSet(
            filepath=filepath_csv.as_posix(),
            file_format="csv",
            version=Version(None, None),
        )
        assert ds_b._version_cache.currsize == 0
        ds_b.resolve_save_version()
        assert ds_b._version_cache.currsize == 1
        ds_b.resolve_load_version()
        assert ds_b._version_cache.currsize == 2

        ds_a.release()

        # dataset A cache is cleared
        assert ds_a._version_cache.currsize == 0

        # dataset B cache is unaffected
        assert ds_b._version_cache.currsize == 2

    def test_no_versions(self, versioned_csv_data_set):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for GenericDataSet\(.+\)"
        with pytest.raises(DataSetError, match=pattern):
            versioned_csv_data_set.load()

    def test_exists(self, versioned_csv_data_set, dummy_dataframe):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_csv_data_set.exists()
        versioned_csv_data_set.save(dummy_dataframe)
        assert versioned_csv_data_set.exists()

    def test_prevent_overwrite(self, versioned_csv_data_set, dummy_dataframe):
        """Check the error when attempting to override the data set if the
        corresponding Generic (csv) file for a given save version already exists."""
        versioned_csv_data_set.save(dummy_dataframe)
        pattern = (
            r"Save path \'.+\' for GenericDataSet\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_csv_data_set.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_csv_data_set, load_version, save_version, dummy_dataframe
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for GenericDataSet\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_csv_data_set.save(dummy_dataframe)

    def test_versioning_existing_dataset(
        self, csv_data_set, versioned_csv_data_set, dummy_dataframe
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        csv_data_set.save(dummy_dataframe)
        assert csv_data_set.exists()
        assert csv_data_set._filepath == versioned_csv_data_set._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_csv_data_set._filepath.parent.as_posix()})"
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_csv_data_set.save(dummy_dataframe)

        # Remove non-versioned dataset and try again
        Path(csv_data_set._filepath.as_posix()).unlink()
        versioned_csv_data_set.save(dummy_dataframe)
        assert versioned_csv_data_set.exists()


class TestBadGenericDataSet:
    def test_bad_file_format_argument(self):
        ds = GenericDataSet(filepath="test.kedro", file_format="kedro")

        pattern = (
            "Cannot create a dataset of file_format 'kedro' as"
            " it does not support a filepath target/source."
        )

        with pytest.raises(DataSetError, match=pattern):
            _ = ds.load()

        pattern2 = (
            "Unable to retrieve 'polars.DataFrame.write_kedro' method, please "
            "ensure that your 'file_format' parameter has been defined correctly as "
            "per the Polars API "
            "https://pola-rs.github.io/polars/py-polars/html/reference/io.html"
        )
        with pytest.raises(DataSetError, match=pattern2):
            ds.save(pd.DataFrame([1]))
