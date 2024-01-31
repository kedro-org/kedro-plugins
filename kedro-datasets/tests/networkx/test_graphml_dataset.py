from pathlib import Path, PurePosixPath

import networkx
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import Version
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError
from s3fs.core import S3FileSystem

from kedro_datasets.networkx import GraphMLDataset

ATTRS = {
    "source": "from",
    "target": "to",
    "name": "fake_id",
    "key": "fake_key",
    "link": "fake_link",
}


@pytest.fixture
def filepath_graphml(tmp_path):
    return (tmp_path / "some_dir" / "test.graphml").as_posix()


@pytest.fixture
def graphml_dataset(filepath_graphml):
    return GraphMLDataset(
        filepath=filepath_graphml,
        load_args={"node_type": int},
        save_args={},
    )


@pytest.fixture
def versioned_graphml_dataset(filepath_graphml, load_version, save_version):
    return GraphMLDataset(
        filepath=filepath_graphml,
        version=Version(load_version, save_version),
        load_args={"node_type": int},
        save_args={},
    )


@pytest.fixture()
def dummy_graph_data():
    return networkx.complete_graph(3)


class TestGraphMLDataset:
    def test_save_and_load(self, graphml_dataset, dummy_graph_data):
        """Test saving and reloading the data set."""
        graphml_dataset.save(dummy_graph_data)
        reloaded = graphml_dataset.load()
        assert dummy_graph_data.nodes(data=True) == reloaded.nodes(data=True)
        assert graphml_dataset._fs_open_args_load == {"mode": "rb"}
        assert graphml_dataset._fs_open_args_save == {"mode": "wb"}

    def test_load_missing_file(self, graphml_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set GraphMLDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            assert graphml_dataset.load()

    def test_exists(self, graphml_dataset, dummy_graph_data):
        """Test `exists` method invocation."""
        assert not graphml_dataset.exists()
        graphml_dataset.save(dummy_graph_data)
        assert graphml_dataset.exists()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.graphml", S3FileSystem),
            ("file:///tmp/test.graphml", LocalFileSystem),
            ("/tmp/test.graphml", LocalFileSystem),
            ("gcs://bucket/file.graphml", GCSFileSystem),
            ("https://example.com/file.graphml", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = GraphMLDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.graphml"
        dataset = GraphMLDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestGraphMLDatasetVersioned:
    def test_save_and_load(self, versioned_graphml_dataset, dummy_graph_data):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_graphml_dataset.save(dummy_graph_data)
        reloaded = versioned_graphml_dataset.load()
        assert dummy_graph_data.nodes(data=True) == reloaded.nodes(data=True)
        assert versioned_graphml_dataset._fs_open_args_load == {"mode": "rb"}
        assert versioned_graphml_dataset._fs_open_args_save == {"mode": "wb"}

    def test_no_versions(self, versioned_graphml_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for GraphMLDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_graphml_dataset.load()

    def test_exists(self, versioned_graphml_dataset, dummy_graph_data):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_graphml_dataset.exists()
        versioned_graphml_dataset.save(dummy_graph_data)
        assert versioned_graphml_dataset.exists()

    def test_prevent_override(self, versioned_graphml_dataset, dummy_graph_data):
        """Check the error when attempt to override the same data set
        version."""
        versioned_graphml_dataset.save(dummy_graph_data)
        pattern = (
            r"Save path \'.+\' for GraphMLDataset\(.+\) must not "
            r"exist if versioning is enabled"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_graphml_dataset.save(dummy_graph_data)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_graphml_dataset, load_version, save_version, dummy_graph_data
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match "
            rf"load version '{load_version}' for GraphMLDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_graphml_dataset.save(dummy_graph_data)

    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.graphml"
        ds = GraphMLDataset(filepath=filepath)
        ds_versioned = GraphMLDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "GraphMLDataset" in str(ds_versioned)
        assert "GraphMLDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_versioning_existing_dataset(
        self, graphml_dataset, versioned_graphml_dataset, dummy_graph_data
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        graphml_dataset.save(dummy_graph_data)
        assert graphml_dataset.exists()
        assert graphml_dataset._filepath == versioned_graphml_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_graphml_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_graphml_dataset.save(dummy_graph_data)

        # Remove non-versioned dataset and try again
        Path(graphml_dataset._filepath.as_posix()).unlink()
        versioned_graphml_dataset.save(dummy_graph_data)
        assert versioned_graphml_dataset.exists()
