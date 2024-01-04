from pathlib import Path, PurePosixPath
from time import sleep

import pandas as pd
import polars as pl
import pytest
from adlfs import AzureBlobFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import Version
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, generate_timestamp
from polars.testing import assert_frame_equal
from s3fs import S3FileSystem

from kedro_datasets.polars import EagerPolarsDataset


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
def versioned_csv_dataset(filepath_csv, load_version, save_version):
    return EagerPolarsDataset(
        filepath=filepath_csv.as_posix(),
        file_format="csv",
        version=Version(load_version, save_version),
        save_args={},
    )


@pytest.fixture
def versioned_ipc_dataset(filepath_ipc, load_version, save_version):
    return EagerPolarsDataset(
        filepath=filepath_ipc.as_posix(),
        file_format="ipc",
        version=Version(load_version, save_version),
        save_args={},
    )


@pytest.fixture
def versioned_parquet_dataset(filepath_parquet, load_version, save_version):
    return EagerPolarsDataset(
        filepath=filepath_parquet.as_posix(),
        file_format="parquet",
        version=Version(load_version, save_version),
        save_args={},
    )


@pytest.fixture
def csv_dataset(filepath_csv):
    return EagerPolarsDataset(
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
def parquet_dataset_ignore(dummy_dataframe: pl.DataFrame, filepath_parquet):
    dummy_dataframe.write_parquet(filepath_parquet)

    return EagerPolarsDataset(
        filepath=filepath_parquet.as_posix(),
        file_format="parquet",
        load_args={"low_memory": True},
    )


@pytest.fixture
def excel_dataset(dummy_dataframe: pl.DataFrame, filepath_excel):
    pd_df = dummy_dataframe.to_pandas()
    pd_df.to_excel(filepath_excel, index=False)

    return EagerPolarsDataset(
        filepath=filepath_excel.as_posix(),
        file_format="excel",
    )


class TestEagerExcelDataset:
    def test_load(self, excel_dataset):
        df = excel_dataset.load()
        assert df.shape == (2, 3)

    def test_save_and_load(self, excel_dataset, dummy_dataframe):
        excel_dataset.save(dummy_dataframe)
        reloaded_df = excel_dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

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
        dataset = EagerPolarsDataset(
            filepath=filepath,
            file_format="excel",
            credentials=credentials,
        )
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.csv"
        dataset = EagerPolarsDataset(filepath=filepath, file_format="excel")
        assert dataset._version_cache.currsize == 0  # no cache if unversioned
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)
        assert dataset._version_cache.currsize == 0


class TestEagerParquetDatasetVersioned:
    def test_load_args(self, parquet_dataset_ignore):
        df = parquet_dataset_ignore.load()
        assert df.shape == (2, 3)

    def test_save_and_load(self, versioned_parquet_dataset, dummy_dataframe):
        """Test saving and reloading the data set."""
        versioned_parquet_dataset.save(dummy_dataframe)
        reloaded_df = versioned_parquet_dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_version_str_repr(self, filepath_parquet, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = filepath_parquet.as_posix()
        ds = EagerPolarsDataset(filepath=filepath, file_format="parquet")
        ds_versioned = EagerPolarsDataset(
            filepath=filepath,
            file_format="parquet",
            version=Version(load_version, save_version),
        )
        assert filepath in str(ds)
        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "EagerPolarsDataset" in str(ds_versioned)
        assert "EagerPolarsDataset" in str(ds)

    def test_multiple_loads(
        self, versioned_parquet_dataset, dummy_dataframe, filepath_parquet
    ):
        """Test that if a new version is created mid-run, by an
        external system, it won't be loaded in the current run."""
        versioned_parquet_dataset.save(dummy_dataframe)
        versioned_parquet_dataset.load()
        v1 = versioned_parquet_dataset.resolve_load_version()

        sleep(0.5)
        # force-drop a newer version into the same location
        v_new = generate_timestamp()
        EagerPolarsDataset(
            filepath=filepath_parquet.as_posix(),
            file_format="parquet",
            version=Version(v_new, v_new),
        ).save(dummy_dataframe)

        versioned_parquet_dataset.load()
        v2 = versioned_parquet_dataset.resolve_load_version()

        assert v2 == v1  # v2 should not be v_new!
        ds_new = EagerPolarsDataset(
            filepath=filepath_parquet.as_posix(),
            file_format="parquet",
            version=Version(None, None),
        )
        assert (
            ds_new.resolve_load_version() == v_new
        )  # new version is discoverable by a new instance

    def test_multiple_saves(self, dummy_dataframe, filepath_parquet):
        """Test multiple cycles of save followed by load for the same dataset"""
        ds_versioned = EagerPolarsDataset(
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
        ds_new = EagerPolarsDataset(
            filepath=filepath_parquet.as_posix(),
            file_format="parquet",
            version=Version(None, None),
        )
        assert ds_new.resolve_load_version() == second_load_version


class TestEagerIPCDatasetVersioned:
    def test_save_and_load(self, versioned_ipc_dataset, dummy_dataframe):
        """Test saving and reloading the data set."""
        versioned_ipc_dataset.save(dummy_dataframe)
        reloaded_df = versioned_ipc_dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_version_str_repr(self, filepath_ipc, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = filepath_ipc.as_posix()
        ds = EagerPolarsDataset(filepath=filepath, file_format="ipc")
        ds_versioned = EagerPolarsDataset(
            filepath=filepath,
            file_format="ipc",
            version=Version(load_version, save_version),
        )
        assert filepath in str(ds)
        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "EagerPolarsDataset" in str(ds_versioned)
        assert "EagerPolarsDataset" in str(ds)

    def test_multiple_loads(self, versioned_ipc_dataset, dummy_dataframe, filepath_ipc):
        """Test that if a new version is created mid-run, by an
        external system, it won't be loaded in the current run."""
        versioned_ipc_dataset.save(dummy_dataframe)
        versioned_ipc_dataset.load()
        v1 = versioned_ipc_dataset.resolve_load_version()

        sleep(0.5)
        # force-drop a newer version into the same location
        v_new = generate_timestamp()
        EagerPolarsDataset(
            filepath=filepath_ipc.as_posix(),
            file_format="ipc",
            version=Version(v_new, v_new),
        ).save(dummy_dataframe)

        versioned_ipc_dataset.load()
        v2 = versioned_ipc_dataset.resolve_load_version()

        assert v2 == v1  # v2 should not be v_new!
        ds_new = EagerPolarsDataset(
            filepath=filepath_ipc.as_posix(),
            file_format="ipc",
            version=Version(None, None),
        )
        assert (
            ds_new.resolve_load_version() == v_new
        )  # new version is discoverable by a new instance

    def test_multiple_saves(self, dummy_dataframe, filepath_ipc):
        """Test multiple cycles of save followed by load for the same dataset"""
        ds_versioned = EagerPolarsDataset(
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
        ds_new = EagerPolarsDataset(
            filepath=filepath_ipc.as_posix(),
            file_format="ipc",
            version=Version(None, None),
        )
        assert ds_new.resolve_load_version() == second_load_version


class TestEagerCSVDatasetVersioned:
    def test_version_str_repr(self, filepath_csv, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = filepath_csv.as_posix()
        ds = EagerPolarsDataset(filepath=filepath, file_format="csv")
        ds_versioned = EagerPolarsDataset(
            filepath=filepath,
            file_format="csv",
            version=Version(load_version, save_version),
        )
        assert filepath in str(ds)
        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "EagerPolarsDataset" in str(ds_versioned)
        assert "EagerPolarsDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_save_and_load(self, versioned_csv_dataset, dummy_dataframe):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_csv_dataset.save(dummy_dataframe)
        reloaded_df = versioned_csv_dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_multiple_loads(self, versioned_csv_dataset, dummy_dataframe, filepath_csv):
        """Test that if a new version is created mid-run, by an
        external system, it won't be loaded in the current run."""
        versioned_csv_dataset.save(dummy_dataframe)
        versioned_csv_dataset.load()
        v1 = versioned_csv_dataset.resolve_load_version()

        sleep(0.5)
        # force-drop a newer version into the same location
        v_new = generate_timestamp()
        EagerPolarsDataset(
            filepath=filepath_csv.as_posix(),
            file_format="csv",
            version=Version(v_new, v_new),
        ).save(dummy_dataframe)

        versioned_csv_dataset.load()
        v2 = versioned_csv_dataset.resolve_load_version()

        assert v2 == v1  # v2 should not be v_new!
        ds_new = EagerPolarsDataset(
            filepath=filepath_csv.as_posix(),
            file_format="csv",
            version=Version(None, None),
        )
        assert (
            ds_new.resolve_load_version() == v_new
        )  # new version is discoverable by a new instance

    def test_multiple_saves(self, dummy_dataframe, filepath_csv):
        """Test multiple cycles of save followed by load for the same dataset"""
        ds_versioned = EagerPolarsDataset(
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
        ds_new = EagerPolarsDataset(
            filepath=filepath_csv.as_posix(),
            file_format="csv",
            version=Version(None, None),
        )
        assert ds_new.resolve_load_version() == second_load_version

    def test_release_instance_cache(self, dummy_dataframe, filepath_csv):
        """Test that cache invalidation does not affect other instances"""
        ds_a = EagerPolarsDataset(
            filepath=filepath_csv.as_posix(),
            file_format="csv",
            version=Version(None, None),
        )
        assert ds_a._version_cache.currsize == 0
        ds_a.save(dummy_dataframe)  # create a version
        assert ds_a._version_cache.currsize == 2

        ds_b = EagerPolarsDataset(
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

    def test_no_versions(self, versioned_csv_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for EagerPolarsDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_csv_dataset.load()

    def test_exists(self, versioned_csv_dataset, dummy_dataframe):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_csv_dataset.exists()
        versioned_csv_dataset.save(dummy_dataframe)
        assert versioned_csv_dataset.exists()

    def test_prevent_overwrite(self, versioned_csv_dataset, dummy_dataframe):
        """Check the error when attempting to override the data set if the
        corresponding Generic (csv) file for a given save version already exists."""
        versioned_csv_dataset.save(dummy_dataframe)
        pattern = (
            r"Save path \'.+\' for EagerPolarsDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_csv_dataset.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_csv_dataset, load_version, save_version, dummy_dataframe
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for EagerPolarsDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_csv_dataset.save(dummy_dataframe)

    def test_versioning_existing_dataset(
        self, csv_dataset, versioned_csv_dataset, dummy_dataframe
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        csv_dataset.save(dummy_dataframe)
        assert csv_dataset.exists()
        assert csv_dataset._filepath == versioned_csv_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_csv_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_csv_dataset.save(dummy_dataframe)

        # Remove non-versioned dataset and try again
        Path(csv_dataset._filepath.as_posix()).unlink()
        versioned_csv_dataset.save(dummy_dataframe)
        assert versioned_csv_dataset.exists()


class TestBadEagerPolarsDataset:
    def test_bad_file_format_argument(self):
        ds = EagerPolarsDataset(filepath="test.kedro", file_format="kedro")

        pattern = (
            "Unable to retrieve 'polars.DataFrame.write_kedro' method, please "
            "ensure that your 'file_format' parameter has been defined correctly as "
            "per the Polars API "
            "https://pola-rs.github.io/polars/py-polars/html/reference/io.html"
        )
        with pytest.raises(DatasetError, match=pattern):
            ds.save(pd.DataFrame([1]))

        pattern2 = (
            "Unable to retrieve 'polars.read_kedro' method, please ensure that your "
            "'file_format' parameter has been defined correctly as per the Polars API "
            "https://pola-rs.github.io/polars/py-polars/html/reference/io.html"
        )
        with pytest.raises(DatasetError, match=pattern2):
            ds.load()
