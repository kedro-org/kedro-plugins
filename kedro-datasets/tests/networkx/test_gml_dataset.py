from pathlib import Path, PurePosixPath

import networkx
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import Version
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError
from s3fs.core import S3FileSystem

from kedro_datasets.networkx import GMLDataset

ATTRS = {
    "source": "from",
    "target": "to",
    "name": "fake_id",
    "key": "fake_key",
    "link": "fake_link",
}


@pytest.fixture
def filepath_gml(tmp_path):
    return (tmp_path / "some_dir" / "test.gml").as_posix()


@pytest.fixture
def gml_dataset(filepath_gml):
    return GMLDataset(
        filepath=filepath_gml,
        load_args={"destringizer": int},
        save_args={"stringizer": str},
    )


@pytest.fixture
def versioned_gml_dataset(filepath_gml, load_version, save_version):
    return GMLDataset(
        filepath=filepath_gml,
        version=Version(load_version, save_version),
        load_args={"destringizer": int},
        save_args={"stringizer": str},
    )


@pytest.fixture()
def dummy_graph_data():
    return networkx.complete_graph(3)


class TestGMLDataset:
    def test_save_and_load(self, gml_dataset, dummy_graph_data):
        """Test saving and reloading the data set."""
        gml_dataset.save(dummy_graph_data)
        reloaded = gml_dataset.load()
        assert dummy_graph_data.nodes(data=True) == reloaded.nodes(data=True)
        assert gml_dataset._fs_open_args_load == {"mode": "rb"}
        assert gml_dataset._fs_open_args_save == {"mode": "wb"}

    def test_load_missing_file(self, gml_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set GMLDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            assert gml_dataset.load()

    def test_exists(self, gml_dataset, dummy_graph_data):
        """Test `exists` method invocation."""
        assert not gml_dataset.exists()
        gml_dataset.save(dummy_graph_data)
        assert gml_dataset.exists()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.gml", S3FileSystem),
            ("file:///tmp/test.gml", LocalFileSystem),
            ("/tmp/test.gml", LocalFileSystem),
            ("gcs://bucket/file.gml", GCSFileSystem),
            ("https://example.com/file.gml", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = GMLDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.gml"
        dataset = GMLDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestGMLDatasetVersioned:
    def test_save_and_load(self, versioned_gml_dataset, dummy_graph_data):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_gml_dataset.save(dummy_graph_data)
        reloaded = versioned_gml_dataset.load()
        assert dummy_graph_data.nodes(data=True) == reloaded.nodes(data=True)
        assert versioned_gml_dataset._fs_open_args_load == {"mode": "rb"}
        assert versioned_gml_dataset._fs_open_args_save == {"mode": "wb"}

    def test_no_versions(self, versioned_gml_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for GMLDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_gml_dataset.load()

    def test_exists(self, versioned_gml_dataset, dummy_graph_data):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_gml_dataset.exists()
        versioned_gml_dataset.save(dummy_graph_data)
        assert versioned_gml_dataset.exists()

    def test_prevent_override(self, versioned_gml_dataset, dummy_graph_data):
        """Check the error when attempt to override the same data set
        version."""
        versioned_gml_dataset.save(dummy_graph_data)
        pattern = (
            r"Save path \'.+\' for GMLDataset\(.+\) must not "
            r"exist if versioning is enabled"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_gml_dataset.save(dummy_graph_data)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_gml_dataset, load_version, save_version, dummy_graph_data
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match "
            rf"load version '{load_version}' for GMLDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_gml_dataset.save(dummy_graph_data)

    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.gml"
        ds = GMLDataset(filepath=filepath)
        ds_versioned = GMLDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "GMLDataset" in str(ds_versioned)
        assert "GMLDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_versioning_existing_dataset(
        self, gml_dataset, versioned_gml_dataset, dummy_graph_data
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        gml_dataset.save(dummy_graph_data)
        assert gml_dataset.exists()
        assert gml_dataset._filepath == versioned_gml_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_gml_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_gml_dataset.save(dummy_graph_data)

        # Remove non-versioned dataset and try again
        Path(gml_dataset._filepath.as_posix()).unlink()
        versioned_gml_dataset.save(dummy_graph_data)
        assert versioned_gml_dataset.exists()
