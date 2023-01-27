from pathlib import Path, PurePosixPath

import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import DataSetError
from kedro.io.core import PROTOCOL_DELIMITER, Version
from s3fs.core import S3FileSystem

from kedro_datasets.text import TextDataSet

STRING = "Write to text file."


@pytest.fixture
def filepath_txt(tmp_path):
    return (tmp_path / "test.txt").as_posix()


@pytest.fixture
def txt_data_set(filepath_txt, fs_args):
    return TextDataSet(filepath=filepath_txt, fs_args=fs_args)


@pytest.fixture
def versioned_txt_data_set(filepath_txt, load_version, save_version):
    return TextDataSet(
        filepath=filepath_txt, version=Version(load_version, save_version)
    )


class TestTextDataSet:
    def test_save_and_load(self, txt_data_set):
        """Test saving and reloading the data set."""
        txt_data_set.save(STRING)
        reloaded = txt_data_set.load()
        assert STRING == reloaded
        assert txt_data_set._fs_open_args_load == {"mode": "r"}
        assert txt_data_set._fs_open_args_save == {"mode": "w"}

    def test_exists(self, txt_data_set):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not txt_data_set.exists()
        txt_data_set.save(STRING)
        assert txt_data_set.exists()

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, txt_data_set, fs_args):
        assert txt_data_set._fs_open_args_load == fs_args["open_args_load"]
        assert txt_data_set._fs_open_args_save == {"mode": "w"}  # default unchanged

    def test_load_missing_file(self, txt_data_set):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set TextDataSet\(.*\)"
        with pytest.raises(DataSetError, match=pattern):
            txt_data_set.load()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.txt", S3FileSystem),
            ("file:///tmp/test.txt", LocalFileSystem),
            ("/tmp/test.txt", LocalFileSystem),
            ("gcs://bucket/file.txt", GCSFileSystem),
            ("https://example.com/file.txt", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        data_set = TextDataSet(filepath=filepath)
        assert isinstance(data_set._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(data_set._filepath) == path
        assert isinstance(data_set._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.txt"
        data_set = TextDataSet(filepath=filepath)
        data_set.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestTextDataSetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.txt"
        ds = TextDataSet(filepath=filepath)
        ds_versioned = TextDataSet(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "TextDataSet" in str(ds_versioned)
        assert "TextDataSet" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_save_and_load(self, versioned_txt_data_set):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_txt_data_set.save(STRING)
        reloaded_df = versioned_txt_data_set.load()
        assert STRING == reloaded_df

    def test_no_versions(self, versioned_txt_data_set):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for TextDataSet\(.+\)"
        with pytest.raises(DataSetError, match=pattern):
            versioned_txt_data_set.load()

    def test_exists(self, versioned_txt_data_set):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_txt_data_set.exists()
        versioned_txt_data_set.save(STRING)
        assert versioned_txt_data_set.exists()

    def test_prevent_overwrite(self, versioned_txt_data_set):
        """Check the error when attempting to override the data set if the
        corresponding text file for a given save version already exists."""
        versioned_txt_data_set.save(STRING)
        pattern = (
            r"Save path \'.+\' for TextDataSet\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_txt_data_set.save(STRING)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_txt_data_set, load_version, save_version
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for TextDataSet\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_txt_data_set.save(STRING)

    def test_http_filesystem_no_versioning(self):
        pattern = r"HTTP\(s\) DataSet doesn't support versioning\."

        with pytest.raises(DataSetError, match=pattern):
            TextDataSet(
                filepath="https://example.com/file.txt", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self,
        txt_data_set,
        versioned_txt_data_set,
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        txt_data_set.save(STRING)
        assert txt_data_set.exists()
        assert txt_data_set._filepath == versioned_txt_data_set._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_txt_data_set._filepath.parent.as_posix()})"
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_txt_data_set.save(STRING)

        # Remove non-versioned dataset and try again
        Path(txt_data_set._filepath.as_posix()).unlink()
        versioned_txt_data_set.save(STRING)
        assert versioned_txt_data_set.exists()
