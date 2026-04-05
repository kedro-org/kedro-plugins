from pathlib import PurePosixPath

import pytest
from datasets import Dataset, DatasetDict, IterableDataset, IterableDatasetDict
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from huggingface_hub import HfApi
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets.huggingface import HFDataset
from kedro_datasets.huggingface.hugging_face_dataset import LocalHFDataset


@pytest.fixture
def dataset_name():
    return "yelp_review_full"


class TestHFDataset:
    def test_simple_dataset_load(self, dataset_name, mocker):
        mocked_load_dataset = mocker.patch(
            "kedro_datasets.huggingface.hugging_face_dataset.load_dataset"
        )

        dataset = HFDataset(
            dataset_name=dataset_name,
        )
        hf_ds = dataset.load()

        mocked_load_dataset.assert_called_once_with(dataset_name)
        assert hf_ds is mocked_load_dataset.return_value

    def test_list_datasets(self, mocker):
        expected_datasets = ["dataset_1", "dataset_2"]
        mocked_hf_list_datasets = mocker.patch.object(HfApi, "list_datasets")
        mocked_hf_list_datasets.return_value = expected_datasets

        datasets = HFDataset.list_datasets()

        assert datasets == expected_datasets


@pytest.fixture
def path_local_hf(tmp_path):
    return (tmp_path / "test_hf_dataset").as_posix()


@pytest.fixture
def local_hf_dataset(path_local_hf, save_args, load_args, fs_args):
    return LocalHFDataset(
        path=path_local_hf,
        save_args=save_args,
        load_args=load_args,
        fs_args=fs_args,
    )


@pytest.fixture
def versioned_local_hf_dataset(path_local_hf, load_version, save_version):
    return LocalHFDataset(
        path=path_local_hf, version=Version(load_version, save_version)
    )


@pytest.fixture
def dataset():
    return Dataset.from_dict({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})


@pytest.fixture
def dataset_dict():
    return DatasetDict(
        {
            "train": Dataset.from_dict({"col1": [1, 2], "col2": ["a", "b"]}),
            "test": Dataset.from_dict({"col1": [3], "col2": ["c"]}),
        }
    )


@pytest.fixture
def iterable_dataset():
    return Dataset.from_dict(
        {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    ).to_iterable_dataset()


@pytest.fixture
def iterable_dataset_dict():
    return IterableDatasetDict(
        {
            "train": Dataset.from_dict(
                {"col1": [1, 2], "col2": ["a", "b"]}
            ).to_iterable_dataset(),
            "test": Dataset.from_dict(
                {"col1": [3], "col2": ["c"]}
            ).to_iterable_dataset(),
        }
    )


@pytest.fixture
def parquet_local_hf_dataset(path_local_hf):
    return LocalHFDataset(path=path_local_hf, file_format="parquet")


class TestLocalHFDataset:
    def test_save_and_load_dataset(self, local_hf_dataset, dataset):
        """Test saving and reloading a Dataset."""
        local_hf_dataset.save(dataset)
        reloaded = local_hf_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_dataset_dict(self, local_hf_dataset, dataset_dict):
        """Test saving and reloading a DatasetDict."""
        local_hf_dataset.save(dataset_dict)
        reloaded = local_hf_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        for split in dataset_dict:
            assert reloaded[split].to_dict() == dataset_dict[split].to_dict()

    def test_exists(self, local_hf_dataset, dataset):
        """Test `exists` method for both existing and nonexistent dataset."""
        assert not local_hf_dataset.exists()
        local_hf_dataset.save(dataset)
        assert local_hf_dataset.exists()

    def test_exists_dataset_dict(self, local_hf_dataset, dataset_dict):
        """Test `exists` method for DatasetDict (checks dataset_dict.json marker)."""
        assert not local_hf_dataset.exists()
        local_hf_dataset.save(dataset_dict)
        assert local_hf_dataset.exists()

    def test_save_and_load_iterable_dataset(self, local_hf_dataset, iterable_dataset):
        """Test saving an IterableDataset materializes and round-trips."""
        local_hf_dataset.save(iterable_dataset)
        reloaded = local_hf_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == {
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        }

    def test_save_and_load_iterable_dataset_dict(
        self, local_hf_dataset, iterable_dataset_dict
    ):
        """Test saving an IterableDatasetDict materializes and round-trips."""
        local_hf_dataset.save(iterable_dataset_dict)
        reloaded = local_hf_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        assert reloaded["train"].to_dict() == {
            "col1": [1, 2],
            "col2": ["a", "b"],
        }

    def test_save_and_load_parquet(self, parquet_local_hf_dataset, dataset):
        """Test saving and reloading a Dataset as parquet."""
        parquet_local_hf_dataset.save(dataset)
        reloaded = parquet_local_hf_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_parquet_dataset_dict(
        self, parquet_local_hf_dataset, dataset_dict
    ):
        """Test saving and reloading a DatasetDict as parquet."""
        parquet_local_hf_dataset.save(dataset_dict)
        reloaded = parquet_local_hf_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        for split in dataset_dict:
            assert reloaded[split].to_dict() == dataset_dict[split].to_dict()

    def test_exists_parquet(self, parquet_local_hf_dataset, dataset):
        """Test `exists` for parquet format."""
        assert not parquet_local_hf_dataset.exists()
        parquet_local_hf_dataset.save(dataset)
        assert parquet_local_hf_dataset.exists()

    def test_save_and_load_json_dataset_dict(self, tmp_path, dataset_dict):
        """Test saving and reloading a DatasetDict as JSON."""
        path = (tmp_path / "test_json_dd").as_posix()
        ds = LocalHFDataset(path=path, file_format="json")
        ds.save(dataset_dict)
        reloaded = ds.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        for split in dataset_dict:
            assert reloaded[split].to_dict() == dataset_dict[split].to_dict()

    def test_invalid_file_format(self):
        """Test that an unsupported file_format raises DatasetError."""
        pattern = r"Unsupported file_format"
        with pytest.raises(DatasetError, match=pattern):
            LocalHFDataset(path="test", file_format="xml")

    @pytest.mark.parametrize("save_args", [{"num_shards": 2}], indirect=True)
    def test_save_extra_params(self, local_hf_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert local_hf_dataset._save_args[key] == value

    @pytest.mark.parametrize("load_args", [{"keep_in_memory": True}], indirect=True)
    def test_load_extra_params(self, local_hf_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert local_hf_dataset._load_args[key] == value

    def test_load_missing_dataset(self, local_hf_dataset):
        """Check the error when trying to load missing dataset."""
        pattern = r"Failed while loading data from dataset kedro_datasets.huggingface.hugging_face_dataset.LocalHFDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            local_hf_dataset.load()

    def test_save_invalid_type(self, local_hf_dataset):
        """Check the error when saving an unsupported type."""
        pattern = r"LocalHFDataset only supports .datasets.Dataset., .datasets.DatasetDict., .datasets.IterableDataset., and .datasets.IterableDatasetDict. instances."
        with pytest.raises(DatasetError, match=pattern):
            local_hf_dataset.save({"not": "a dataset"})

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
        dataset = LocalHFDataset(path=path)
        assert isinstance(dataset._fs, instance_type)

        resolved = path.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == resolved
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_pathlike_path(self, tmp_path, dataset):
        """Test that os.PathLike paths are supported."""
        path = tmp_path / "test_hf_pathlike"
        ds = LocalHFDataset(path=path)
        ds.save(dataset)
        reloaded = ds.load()
        assert reloaded.to_dict() == dataset.to_dict()

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        path = "test_hf"
        dataset = LocalHFDataset(path=path)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(path)


class TestLocalHFDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        path = "test_hf_dataset"
        ds = LocalHFDataset(path=path)
        ds_versioned = LocalHFDataset(
            path=path, version=Version(load_version, save_version)
        )
        assert path in str(ds)
        assert "version" not in str(ds)

        assert path in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "LocalHFDataset" in str(ds_versioned)
        assert "LocalHFDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_save_and_load(self, versioned_local_hf_dataset, dataset):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        versioned_local_hf_dataset.save(dataset)
        reloaded = versioned_local_hf_dataset.load()
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_dataset_dict(self, versioned_local_hf_dataset, dataset_dict):
        """Test versioned save and reload with DatasetDict."""
        versioned_local_hf_dataset.save(dataset_dict)
        reloaded = versioned_local_hf_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        for split in dataset_dict:
            assert reloaded[split].to_dict() == dataset_dict[split].to_dict()

    def test_save_and_load_iterable_dataset(
        self, versioned_local_hf_dataset, iterable_dataset
    ):
        """Test versioned save with IterableDataset."""
        versioned_local_hf_dataset.save(iterable_dataset)
        reloaded = versioned_local_hf_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == {
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        }

    def test_save_and_load_iterable_dataset_dict(
        self, versioned_local_hf_dataset, iterable_dataset_dict
    ):
        """Test versioned save with IterableDatasetDict."""
        versioned_local_hf_dataset.save(iterable_dataset_dict)
        reloaded = versioned_local_hf_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}

    def test_no_versions(self, versioned_local_hf_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for kedro_datasets.huggingface.hugging_face_dataset.LocalHFDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_local_hf_dataset.load()

    def test_exists(self, versioned_local_hf_dataset, dataset):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_local_hf_dataset.exists()
        versioned_local_hf_dataset.save(dataset)
        assert versioned_local_hf_dataset.exists()

    def test_prevent_overwrite(self, versioned_local_hf_dataset, dataset):
        """Check the error when attempting to override the dataset if the
        corresponding version already exists."""
        versioned_local_hf_dataset.save(dataset)
        pattern = (
            r"Save path \'.+\' for kedro_datasets.huggingface.hugging_face_dataset.LocalHFDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_local_hf_dataset.save(dataset)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_local_hf_dataset, load_version, save_version, dataset
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            f"Save version '{save_version}' did not match "
            f"load version '{load_version}' for "
            r"kedro_datasets.huggingface.hugging_face_dataset.LocalHFDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_local_hf_dataset.save(dataset)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            LocalHFDataset(
                path="https://example.com/hf_data",
                version=Version(None, None),
            )

    def test_save_invalid_type_versioned(self, versioned_local_hf_dataset):
        """Check the error when saving an unsupported type through versioned dataset."""
        pattern = r"LocalHFDataset only supports .datasets.Dataset., .datasets.DatasetDict., .datasets.IterableDataset., and .datasets.IterableDatasetDict. instances."
        with pytest.raises(DatasetError, match=pattern):
            versioned_local_hf_dataset.save("not a dataset")
