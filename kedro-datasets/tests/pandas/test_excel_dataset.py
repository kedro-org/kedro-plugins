import inspect
from pathlib import Path, PurePosixPath

import pandas as pd
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from pandas.testing import assert_frame_equal
from s3fs.core import S3FileSystem

from kedro_datasets.pandas import ExcelDataset


@pytest.fixture
def filepath_excel(tmp_path):
    return (tmp_path / "test.xlsx").as_posix()


@pytest.fixture
def excel_dataset(filepath_excel, load_args, save_args, fs_args):
    return ExcelDataset(
        filepath=filepath_excel,
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def excel_multisheet_dataset(filepath_excel, save_args, fs_args):
    load_args = {"sheet_name": None}
    return ExcelDataset(
        filepath=filepath_excel,
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def versioned_excel_dataset(filepath_excel, load_version, save_version):
    return ExcelDataset(
        filepath=filepath_excel, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


@pytest.fixture
def another_dummy_dataframe():
    return pd.DataFrame({"x": [10, 20], "y": ["hello", "world"]})


class TestExcelDataset:
    def test_save_and_load(self, excel_dataset, dummy_dataframe):
        """Test saving and reloading the data set."""
        excel_dataset.save(dummy_dataframe)
        reloaded = excel_dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded)

    def test_save_and_load_multiple_sheets(
        self, excel_multisheet_dataset, dummy_dataframe, another_dummy_dataframe
    ):
        """Test saving and reloading the data set with multiple sheets."""
        dummy_multisheet = {
            "sheet 1": dummy_dataframe,
            "sheet 2": another_dummy_dataframe,
        }
        excel_multisheet_dataset.save(dummy_multisheet)
        reloaded = excel_multisheet_dataset.load()
        assert_frame_equal(dummy_multisheet["sheet 1"], reloaded["sheet 1"])
        assert_frame_equal(dummy_multisheet["sheet 2"], reloaded["sheet 2"])

    def test_exists(self, excel_dataset, dummy_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not excel_dataset.exists()
        excel_dataset.save(dummy_dataframe)
        assert excel_dataset.exists()

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, excel_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert excel_dataset._load_args[key] == value

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, excel_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert excel_dataset._save_args[key] == value

    @pytest.mark.parametrize(
        "load_args,save_args",
        [
            ({"storage_options": {"a": "b"}}, {}),
            ({}, {"storage_options": {"a": "b"}}),
            ({"storage_options": {"a": "b"}}, {"storage_options": {"x": "y"}}),
        ],
    )
    def test_storage_options_dropped(self, load_args, save_args, caplog, tmp_path):
        filepath = str(tmp_path / "test.csv")

        ds = ExcelDataset(filepath=filepath, load_args=load_args, save_args=save_args)

        records = [r for r in caplog.records if r.levelname == "WARNING"]
        expected_log_message = (
            f"Dropping 'storage_options' for {filepath}, "
            f"please specify them under 'fs_args' or 'credentials'."
        )
        assert records[0].getMessage() == expected_log_message
        assert "storage_options" not in ds._save_args
        assert "storage_options" not in ds._load_args

    @pytest.mark.parametrize(
        "nrows,expected",
        [
            (
                0,
                {
                    "index": [],
                    "columns": ["col1", "col2", "col3"],
                    "data": [],
                },
            ),
            (
                1,
                {
                    "index": [0],
                    "columns": ["col1", "col2", "col3"],
                    "data": [[1, 4, 5]],
                },
            ),
            (
                None,
                {
                    "index": [0, 1],
                    "columns": ["col1", "col2", "col3"],
                    "data": [[1, 4, 5], [2, 5, 6]],
                },
            ),
            (
                10,
                {
                    "index": [0, 1],
                    "columns": ["col1", "col2", "col3"],
                    "data": [[1, 4, 5], [2, 5, 6]],
                },
            ),
        ],
    )
    def test_preview(self, excel_dataset, dummy_dataframe, nrows, expected):
        """Test preview returns the correct data structure."""
        excel_dataset.save(dummy_dataframe)
        previewed = excel_dataset.preview(nrows=nrows)
        assert previewed == expected
        assert (
            inspect.signature(excel_dataset.preview).return_annotation == "TablePreview"
        )

    def test_load_missing_file(self, excel_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set ExcelDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            excel_dataset.load()

    @pytest.mark.parametrize(
        "filepath,instance_type,load_path",
        [
            ("s3://bucket/file.xlsx", S3FileSystem, "s3://bucket/file.xlsx"),
            ("file:///tmp/test.xlsx", LocalFileSystem, "/tmp/test.xlsx"),
            ("/tmp/test.xlsx", LocalFileSystem, "/tmp/test.xlsx"),
            ("gcs://bucket/file.xlsx", GCSFileSystem, "gcs://bucket/file.xlsx"),
            (
                "https://example.com/file.xlsx",
                HTTPFileSystem,
                "https://example.com/file.xlsx",
            ),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type, load_path, mocker):
        dataset = ExcelDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

        mock_pandas_call = mocker.patch("pandas.read_excel")
        dataset.load()
        assert mock_pandas_call.call_count == 1
        assert mock_pandas_call.call_args_list[0][0][0] == load_path

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.xlsx"
        dataset = ExcelDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestExcelDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.xlsx"
        ds = ExcelDataset(filepath=filepath)
        ds_versioned = ExcelDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "ExcelDataset" in str(ds_versioned)
        assert "ExcelDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)
        assert "writer_args" in str(ds_versioned)
        assert "writer_args" in str(ds)
        # Default save_args and load_args
        assert "save_args={'index': False}" in str(ds)
        assert "save_args={'index': False}" in str(ds_versioned)
        assert "load_args={'engine': openpyxl}" in str(ds_versioned)
        assert "load_args={'engine': openpyxl}" in str(ds)

    def test_save_and_load(self, versioned_excel_dataset, dummy_dataframe):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_excel_dataset.save(dummy_dataframe)
        reloaded_df = versioned_excel_dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_no_versions(self, versioned_excel_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for ExcelDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_excel_dataset.load()

    def test_versioning_not_supported_in_append_mode(
        self, tmp_path, load_version, save_version
    ):
        filepath = str(tmp_path / "test.xlsx")
        save_args = {"writer": {"mode": "a"}}

        pattern = "'ExcelDataset' doesn't support versioning in append mode."
        with pytest.raises(DatasetError, match=pattern):
            ExcelDataset(
                filepath=filepath,
                version=Version(load_version, save_version),
                save_args=save_args,
            )

    def test_exists(self, versioned_excel_dataset, dummy_dataframe):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_excel_dataset.exists()
        versioned_excel_dataset.save(dummy_dataframe)
        assert versioned_excel_dataset.exists()

    def test_prevent_overwrite(self, versioned_excel_dataset, dummy_dataframe):
        """Check the error when attempting to override the data set if the
        corresponding Excel file for a given save version already exists."""
        versioned_excel_dataset.save(dummy_dataframe)
        pattern = (
            r"Save path \'.+\' for ExcelDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_excel_dataset.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_excel_dataset, load_version, save_version, dummy_dataframe
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for ExcelDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_excel_dataset.save(dummy_dataframe)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            ExcelDataset(
                filepath="https://example.com/file.xlsx", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, excel_dataset, versioned_excel_dataset, dummy_dataframe
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        excel_dataset.save(dummy_dataframe)
        assert excel_dataset.exists()
        assert excel_dataset._filepath == versioned_excel_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_excel_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_excel_dataset.save(dummy_dataframe)

        # Remove non-versioned dataset and try again
        Path(excel_dataset._filepath.as_posix()).unlink()
        versioned_excel_dataset.save(dummy_dataframe)
        assert versioned_excel_dataset.exists()
