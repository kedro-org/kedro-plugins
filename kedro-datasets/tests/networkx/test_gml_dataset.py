from pathlib import Path, PurePosixPath

import networkx
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import DataSetError, Version
from kedro.io.core import PROTOCOL_DELIMITER
from s3fs.core import S3FileSystem

from kedro_datasets.networkx import GMLDataSet

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
def gml_data_set(filepath_gml):
    return GMLDataSet(
        filepath=filepath_gml,
        load_args={"destringizer": int},
        save_args={"stringizer": str},
    )


@pytest.fixture
def versioned_gml_data_set(filepath_gml, load_version, save_version):
    return GMLDataSet(
        filepath=filepath_gml,
        version=Version(load_version, save_version),
        load_args={"destringizer": int},
        save_args={"stringizer": str},
    )


@pytest.fixture()
def dummy_graph_data():
    return networkx.complete_graph(3)


class TestGMLDataSet:
    def test_save_and_load(self, gml_data_set, dummy_graph_data):
        """Test saving and reloading the data set."""
        gml_data_set.save(dummy_graph_data)
        reloaded = gml_data_set.load()
        assert dummy_graph_data.nodes(data=True) == reloaded.nodes(data=True)
        assert gml_data_set._fs_open_args_load == {"mode": "rb"}
        assert gml_data_set._fs_open_args_save == {"mode": "wb"}

    def test_load_missing_file(self, gml_data_set):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set GMLDataSet\(.*\)"
        with pytest.raises(DataSetError, match=pattern):
            assert gml_data_set.load()

    def test_exists(self, gml_data_set, dummy_graph_data):
        """Test `exists` method invocation."""
        assert not gml_data_set.exists()
        gml_data_set.save(dummy_graph_data)
        assert gml_data_set.exists()

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
        data_set = GMLDataSet(filepath=filepath)
        assert isinstance(data_set._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(data_set._filepath) == path
        assert isinstance(data_set._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.gml"
        data_set = GMLDataSet(filepath=filepath)
        data_set.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestGMLDataSetVersioned:
    def test_save_and_load(self, versioned_gml_data_set, dummy_graph_data):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_gml_data_set.save(dummy_graph_data)
        reloaded = versioned_gml_data_set.load()
        assert dummy_graph_data.nodes(data=True) == reloaded.nodes(data=True)
        assert versioned_gml_data_set._fs_open_args_load == {"mode": "rb"}
        assert versioned_gml_data_set._fs_open_args_save == {"mode": "wb"}

    def test_no_versions(self, versioned_gml_data_set):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for GMLDataSet\(.+\)"
        with pytest.raises(DataSetError, match=pattern):
            versioned_gml_data_set.load()

    def test_exists(self, versioned_gml_data_set, dummy_graph_data):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_gml_data_set.exists()
        versioned_gml_data_set.save(dummy_graph_data)
        assert versioned_gml_data_set.exists()

    def test_prevent_override(self, versioned_gml_data_set, dummy_graph_data):
        """Check the error when attempt to override the same data set
        version."""
        versioned_gml_data_set.save(dummy_graph_data)
        pattern = (
            r"Save path \'.+\' for GMLDataSet\(.+\) must not "
            r"exist if versioning is enabled"
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_gml_data_set.save(dummy_graph_data)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_gml_data_set, load_version, save_version, dummy_graph_data
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match "
            rf"load version '{load_version}' for GMLDataSet\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_gml_data_set.save(dummy_graph_data)

    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.gml"
        ds = GMLDataSet(filepath=filepath)
        ds_versioned = GMLDataSet(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "GMLDataSet" in str(ds_versioned)
        assert "GMLDataSet" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_versioning_existing_dataset(
        self, gml_data_set, versioned_gml_data_set, dummy_graph_data
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        gml_data_set.save(dummy_graph_data)
        assert gml_data_set.exists()
        assert gml_data_set._filepath == versioned_gml_data_set._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_gml_data_set._filepath.parent.as_posix()})"
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_gml_data_set.save(dummy_graph_data)

        # Remove non-versioned dataset and try again
        Path(gml_data_set._filepath.as_posix()).unlink()
        versioned_gml_data_set.save(dummy_graph_data)
        assert versioned_gml_data_set.exists()
