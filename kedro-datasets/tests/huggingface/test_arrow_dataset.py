from pathlib import PurePosixPath

import pytest
from datasets import Dataset, DatasetDict
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets.huggingface.arrow_dataset import ArrowDataset


@pytest.fixture
def path_arrow(tmp_path):
    return (tmp_path / "test_hf_dataset").as_posix()


@pytest.fixture
def arrow_dataset(path_arrow, save_args, load_args, fs_args):
    return ArrowDataset(
        path=path_arrow,
        save_args=save_args,
        load_args=load_args,
        fs_args=fs_args,
    )


@pytest.fixture
def versioned_arrow_dataset(path_arrow, load_version, save_version):
    return ArrowDataset(path=path_arrow, version=Version(load_version, save_version))


@pytest.fixture
def load_version(request):
    return getattr(request, "param", "2019-01-01T23.59.59.999Z")


@pytest.fixture
def save_version(request):
    return getattr(request, "param", "2019-01-01T23.59.59.999Z")


class TestArrowDataset:
    def test_save_and_load_dataset(self, arrow_dataset, hf_dataset):
        """Test saving and reloading a Dataset."""
        arrow_dataset.save(hf_dataset)
        reloaded = arrow_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == hf_dataset.to_dict()

    def test_save_and_load_dataset_dict(self, arrow_dataset, dataset_dict):
        """Test saving and reloading a DatasetDict."""
        arrow_dataset.save(dataset_dict)
        reloaded = arrow_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"data", "labels"}
        for split in dataset_dict:
            assert reloaded[split].to_dict() == dataset_dict[split].to_dict()

    def test_exists(self, arrow_dataset, hf_dataset):
        """Test `exists` method for both existing and nonexistent dataset."""
        assert not arrow_dataset.exists()
        arrow_dataset.save(hf_dataset)
        assert arrow_dataset.exists()

    def test_exists_dataset_dict(self, arrow_dataset, dataset_dict):
        """Test `exists` method for DatasetDict (checks dataset_dict.json marker)."""
        assert not arrow_dataset.exists()
        arrow_dataset.save(dataset_dict)
        assert arrow_dataset.exists()

    def test_save_and_load_iterable_dataset(self, arrow_dataset, iterable_dataset):
        """Test that saving an IterableDataset raises an error with a helpful message."""
        pattern = r"got iterable dataset"
        with pytest.raises(DatasetError, match=pattern):
            arrow_dataset.save(iterable_dataset)

    def test_save_and_load_iterable_dataset_dict(
        self, arrow_dataset, iterable_dataset_dict
    ):
        """Test that saving an IterableDatasetDict raises an error with a helpful message."""
        pattern = r"got iterable dataset"
        with pytest.raises(DatasetError, match=pattern):
            arrow_dataset.save(iterable_dataset_dict)

    @pytest.mark.parametrize("save_args", [{"num_shards": 2}], indirect=True)
    def test_save_extra_params(self, arrow_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert arrow_dataset._save_args[key] == value

    @pytest.mark.parametrize("load_args", [{"keep_in_memory": True}], indirect=True)
    def test_load_extra_params(self, arrow_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert arrow_dataset._load_args[key] == value

    def test_load_missing_dataset(self, arrow_dataset):
        """Check the error when trying to load missing dataset."""
        pattern = r"Failed while loading data from dataset kedro_datasets.huggingface.arrow_dataset.ArrowDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            arrow_dataset.load()

    def test_save_invalid_type(self, arrow_dataset):
        """Check the error when saving an unsupported type."""
        pattern = (
            r"ArrowDataset only supports .datasets.Dataset., .datasets.DatasetDict."
        )
        with pytest.raises(DatasetError, match=pattern):
            arrow_dataset.save({"not": "a dataset"})

    @pytest.mark.parametrize(
        "path,instance_type",
        [
            ("s3://bucket/hf_data", S3FileSystem),
            ("file:///tmp/hf_data", LocalFileSystem),
            ("/tmp/hf_data", LocalFileSystem),
            ("gcs://bucket/hf_data", GCSFileSystem),
            ("https://example.com/hf_data", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, path, instance_type):
        dataset = ArrowDataset(path=path)
        assert isinstance(dataset._fs, instance_type)

        resolved = path.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == resolved
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_pathlike_path(self, tmp_path, hf_dataset):
        """Test that os.PathLike paths are supported."""
        path = tmp_path / "test_hf_pathlike"
        ds = ArrowDataset(path=path)
        ds.save(hf_dataset)
        reloaded = ds.load()
        assert reloaded.to_dict() == hf_dataset.to_dict()

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        path = "test_hf"
        dataset = ArrowDataset(path=path)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(path)


class TestArrowDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        path = "test_hf_dataset"
        ds = ArrowDataset(path=path)
        ds_versioned = ArrowDataset(
            path=path, version=Version(load_version, save_version)
        )
        assert path in str(ds)
        assert "version" not in str(ds)

        assert path in str(ds_versioned)
        ver_str = f"version=Version(load='{load_version}', save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "ArrowDataset" in str(ds_versioned)
        assert "ArrowDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_save_and_load(self, versioned_arrow_dataset, hf_dataset):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        versioned_arrow_dataset.save(hf_dataset)
        reloaded = versioned_arrow_dataset.load()
        assert reloaded.to_dict() == hf_dataset.to_dict()

    def test_save_and_load_dataset_dict(self, versioned_arrow_dataset, dataset_dict):
        """Test versioned save and reload with DatasetDict."""
        versioned_arrow_dataset.save(dataset_dict)
        reloaded = versioned_arrow_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        for split in dataset_dict:
            assert reloaded[split].to_dict() == dataset_dict[split].to_dict()

    def test_save_and_load_iterable_dataset(
        self, versioned_arrow_dataset, iterable_dataset
    ):
        """Test that versioned save of IterableDataset raises an error with a helpful message."""
        pattern = r"got iterable dataset"
        with pytest.raises(DatasetError, match=pattern):
            versioned_arrow_dataset.save(iterable_dataset)

    def test_save_and_load_iterable_dataset_dict(
        self, versioned_arrow_dataset, iterable_dataset_dict
    ):
        """Test that versioned save of IterableDatasetDict raises an error with a helpful message."""
        pattern = r"got iterable dataset"
        with pytest.raises(DatasetError, match=pattern):
            versioned_arrow_dataset.save(iterable_dataset_dict)

    def test_exists(self, versioned_arrow_dataset, hf_dataset):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_arrow_dataset.exists()
        versioned_arrow_dataset.save(hf_dataset)
        assert versioned_arrow_dataset.exists()

    def test_prevent_overwrite(self, versioned_arrow_dataset, hf_dataset):
        """Check the error when attempting to override the dataset if the
        corresponding version already exists."""
        versioned_arrow_dataset.save(hf_dataset)
        pattern = (
            r"Save path \'.+\' for kedro_datasets.huggingface.arrow_dataset.ArrowDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_arrow_dataset.save(hf_dataset)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_arrow_dataset, load_version, save_version, hf_dataset
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            f"Save version '{save_version}' did not match "
            f"load version '{load_version}' for "
            r"kedro_datasets.huggingface.arrow_dataset.ArrowDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_arrow_dataset.save(hf_dataset)

    def test_save_invalid_type_versioned(self, versioned_arrow_dataset):
        """Check the error when saving an unsupported type through versioned dataset."""
        pattern = (
            r"ArrowDataset only supports .datasets.Dataset., .datasets.DatasetDict."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_arrow_dataset.save("not a dataset")
