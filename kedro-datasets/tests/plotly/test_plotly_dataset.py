import inspect
from pathlib import PurePosixPath

import pandas as pd
import pytest
from adlfs import AzureBlobFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError
from plotly import graph_objects
from plotly.graph_objs import Scatter
from s3fs.core import S3FileSystem

from kedro_datasets.plotly import PlotlyDataset


@pytest.fixture
def filepath_json(tmp_path):
    return (tmp_path / "test.json").as_posix()


@pytest.fixture
def plotly_dataset(filepath_json, load_args, save_args, fs_args, plotly_args):
    return PlotlyDataset(
        filepath=filepath_json,
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
        plotly_args=plotly_args,
    )


@pytest.fixture
def plotly_args():
    return {
        "fig": {"orientation": "h", "x": "col1", "y": "col2"},
        "layout": {"title": "Test", "xaxis_title": "x", "yaxis_title": "y"},
        "type": "scatter",
    }


@pytest.fixture
def dummy_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


class TestPlotlyDataset:
    def test_save_and_load(self, plotly_dataset, dummy_dataframe):
        """Test saving and reloading the data set."""
        plotly_dataset.save(dummy_dataframe)
        reloaded = plotly_dataset.load()
        assert isinstance(reloaded, graph_objects.Figure)
        assert "Test" in str(reloaded["layout"]["title"])
        assert isinstance(reloaded["data"][0], Scatter)

    def test_exists(self, plotly_dataset, dummy_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not plotly_dataset.exists()
        plotly_dataset.save(dummy_dataframe)
        assert plotly_dataset.exists()

    def test_load_missing_file(self, plotly_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set PlotlyDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            plotly_dataset.load()

    @pytest.mark.parametrize(
        "filepath,instance_type,credentials",
        [
            ("s3://bucket/file.json", S3FileSystem, {}),
            ("file:///tmp/test.json", LocalFileSystem, {}),
            ("/tmp/test.json", LocalFileSystem, {}),
            ("gcs://bucket/file.json", GCSFileSystem, {}),
            ("https://example.com/file.json", HTTPFileSystem, {}),
            (
                "abfs://bucket/file.csv",
                AzureBlobFileSystem,
                {"account_name": "test", "account_key": "test"},
            ),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type, credentials, plotly_args):
        dataset = PlotlyDataset(
            filepath=filepath, credentials=credentials, plotly_args=plotly_args
        )
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker, plotly_args):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.json"
        dataset = PlotlyDataset(filepath=filepath, plotly_args=plotly_args)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)

    def test_fail_if_invalid_plotly_args_provided(self):
        plotly_args = []
        filepath = "test.json"
        dataset = PlotlyDataset(filepath=filepath, plotly_args=plotly_args)
        with pytest.raises(DatasetError):
            dataset.save(dummy_dataframe)

    def test_preview(self, plotly_dataset, dummy_dataframe):
        plotly_dataset.save(dummy_dataframe)
        preview = plotly_dataset.preview()
        assert (
            inspect.signature(plotly_dataset.preview).return_annotation
            == "PlotlyPreview"
        )
        assert "data" in preview
        assert "layout" in preview
