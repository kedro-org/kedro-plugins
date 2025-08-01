from pathlib import Path, PurePosixPath

import networkx
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import Version
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError
from s3fs.core import S3FileSystem

from kedro_datasets.networkx import JSONDataset

ATTRS = {
    "source": "from",
    "target": "to",
    "name": "fake_id",
    "key": "fake_key",
    "link": "fake_link",
}


@pytest.fixture
def filepath_json(tmp_path):
    return (tmp_path / "some_dir" / "test.json").as_posix()


@pytest.fixture
def json_dataset(filepath_json, fs_args):
    return JSONDataset(filepath=filepath_json, fs_args=fs_args)


@pytest.fixture
def versioned_json_dataset(filepath_json, load_version, save_version):
    return JSONDataset(
        filepath=filepath_json, version=Version(load_version, save_version)
    )


@pytest.fixture
def json_dataset_args(filepath_json):
    return JSONDataset(filepath=filepath_json, load_args=ATTRS, save_args=ATTRS)


@pytest.fixture()
def dummy_graph_data():
    return networkx.complete_graph(3)


class TestJSONDataset:
    def test_save_and_load(self, json_dataset, dummy_graph_data):
        """Test saving and reloading the dataset."""
        json_dataset.save(dummy_graph_data)
        reloaded = json_dataset.load()
        assert dummy_graph_data.nodes(data=True) == reloaded.nodes(data=True)
        assert json_dataset._fs_open_args_load == {}
        assert json_dataset._fs_open_args_save == {"mode": "w"}

    def test_load_missing_file(self, json_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from dataset kedro_datasets.networkx.json_dataset.JSONDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            assert json_dataset.load()

    def test_load_args_save_args(self, mocker, json_dataset_args, dummy_graph_data):
        """Test saving and reloading with save and load arguments."""
        patched_save = mocker.patch(
            "networkx.node_link_data", wraps=networkx.node_link_data
        )
        json_dataset_args.save(dummy_graph_data)
        patched_save.assert_called_once_with(
            dummy_graph_data,
            source="from",
            target="to",
            name="fake_id",
            key="fake_key",
            link="fake_link",
        )

        patched_load = mocker.patch(
            "networkx.node_link_graph", wraps=networkx.node_link_graph
        )
        # load args need to be the same attrs as the ones used for saving
        # in order to successfully retrieve data
        reloaded = json_dataset_args.load()

        patched_load.assert_called_once_with(
            {
                "directed": False,
                "multigraph": False,
                "graph": {},
                "nodes": [{"fake_id": 0}, {"fake_id": 1}, {"fake_id": 2}],
                "fake_link": [
                    {"from": 0, "to": 1},
                    {"from": 0, "to": 2},
                    {"from": 1, "to": 2},
                ],
            },
            source="from",
            target="to",
            name="fake_id",
            key="fake_key",
            link="fake_link",
        )
        assert dummy_graph_data.nodes(data=True) == reloaded.nodes(data=True)

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, json_dataset, fs_args):
        assert json_dataset._fs_open_args_load == fs_args["open_args_load"]
        assert json_dataset._fs_open_args_save == {"mode": "w"}  # default unchanged

    def test_exists(self, json_dataset, dummy_graph_data):
        """Test `exists` method invocation."""
        assert not json_dataset.exists()
        json_dataset.save(dummy_graph_data)
        assert json_dataset.exists()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.json", S3FileSystem),
            ("file:///tmp/test.json", LocalFileSystem),
            ("/tmp/test.json", LocalFileSystem),
            ("gcs://bucket/file.json", GCSFileSystem),
            ("https://example.com/file.json", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = JSONDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.json"
        dataset = JSONDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestJSONDatasetVersioned:
    def test_save_and_load(self, versioned_json_dataset, dummy_graph_data):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        versioned_json_dataset.save(dummy_graph_data)
        reloaded = versioned_json_dataset.load()
        assert dummy_graph_data.nodes(data=True) == reloaded.nodes(data=True)

    def test_no_versions(self, versioned_json_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for kedro_datasets.networkx.json_dataset.JSONDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_json_dataset.load()

    def test_exists(self, versioned_json_dataset, dummy_graph_data):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_json_dataset.exists()
        versioned_json_dataset.save(dummy_graph_data)
        assert versioned_json_dataset.exists()

    def test_prevent_override(self, versioned_json_dataset, dummy_graph_data):
        """Check the error when attempt to override the same dataset
        version."""
        versioned_json_dataset.save(dummy_graph_data)
        pattern = (
            r"Save path \'.+\' for kedro_datasets.networkx.json_dataset.JSONDataset\(.+\) must not "
            r"exist if versioning is enabled"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_json_dataset.save(dummy_graph_data)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_json_dataset, load_version, save_version, dummy_graph_data
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for kedro_datasets.networkx.json_dataset.JSONDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_json_dataset.save(dummy_graph_data)

    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.json"
        ds = JSONDataset(filepath=filepath)
        ds_versioned = JSONDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "JSONDataset" in str(ds_versioned)
        assert "JSONDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_versioning_existing_dataset(
        self, json_dataset, versioned_json_dataset, dummy_graph_data
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        json_dataset.save(dummy_graph_data)
        assert json_dataset.exists()
        assert json_dataset._filepath == versioned_json_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_json_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_json_dataset.save(dummy_graph_data)

        # Remove non-versioned dataset and try again
        Path(json_dataset._filepath.as_posix()).unlink()
        versioned_json_dataset.save(dummy_graph_data)
        assert versioned_json_dataset.exists()
