from pathlib import PurePosixPath

import plotly.express as px
import pytest
from adlfs import AzureBlobFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError
from s3fs.core import S3FileSystem

from kedro_datasets.plotly import HTMLDataset


@pytest.fixture
def filepath_html(tmp_path):
    return (tmp_path / "test.html").as_posix()


@pytest.fixture
def html_dataset(filepath_html, save_args, fs_args):
    return HTMLDataset(
        filepath=filepath_html,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def dummy_plot():
    return px.scatter(x=[1, 2, 3], y=[1, 3, 2], title="Test")


class TestHTMLDataset:
    def test_save(self, html_dataset, dummy_plot):
        """Test saving and reloading the data set."""
        html_dataset.save(dummy_plot)
        assert html_dataset._fs_open_args_save == {"mode": "w"}

    def test_exists(self, html_dataset, dummy_plot):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not html_dataset.exists()
        html_dataset.save(dummy_plot)
        assert html_dataset.exists()

    def test_load_is_impossible(self, html_dataset):
        """Check the error when trying to load a dataset."""
        pattern = "Loading not supported"
        with pytest.raises(DatasetError, match=pattern):
            html_dataset.load()

    @pytest.mark.parametrize("save_args", [{"auto_play": False}])
    def test_save_extra_params(self, html_dataset, save_args):
        """Test overriding default save args"""
        for k, v in save_args.items():
            assert html_dataset._save_args[k] == v

    @pytest.mark.parametrize(
        "filepath,instance_type,credentials",
        [
            ("s3://bucket/file.html", S3FileSystem, {}),
            ("file:///tmp/test.html", LocalFileSystem, {}),
            ("/tmp/test.html", LocalFileSystem, {}),
            ("gcs://bucket/file.html", GCSFileSystem, {}),
            ("https://example.com/file.html", HTTPFileSystem, {}),
            (
                "abfs://bucket/file.csv",
                AzureBlobFileSystem,
                {"account_name": "test", "account_key": "test"},
            ),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type, credentials):
        dataset = HTMLDataset(filepath=filepath, credentials=credentials)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.html"
        dataset = HTMLDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)
