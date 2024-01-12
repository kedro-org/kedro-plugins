import logging
import os
import re
from pathlib import Path

import boto3
import pandas as pd
import pytest
import s3fs
from kedro.io import DatasetError
from kedro.io.data_catalog import CREDENTIALS_KEY
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

from kedro_datasets.pandas import CSVDataset, ParquetDataset
from kedro_datasets.partitions import PartitionedDataset
from kedro_datasets.partitions.partitioned_dataset import KEY_PROPAGATION_WARNING


@pytest.fixture
def partitioned_data_pandas():
    keys = ("p1/data1.csv", "p2.csv", "p1/data2.csv", "p3", "_p4")
    return {
        k: pd.DataFrame({"part": k, "counter": list(range(counter))})
        for counter, k in enumerate(keys, 1)
    }


@pytest.fixture
def local_csvs(tmp_path, partitioned_data_pandas):
    local_dir = tmp_path / "csvs"
    local_dir.mkdir()

    for k, data in partitioned_data_pandas.items():
        path = local_dir / k
        path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(str(path), index=False)
    return local_dir


@pytest.fixture
def filepath_csvs(tmp_path):
    return str(tmp_path / "csvs")


LOCAL_DATASET_DEFINITION = [
    "pandas.CSVDataset",
    "kedro_datasets.pandas.CSVDataset",
    CSVDataset,
    {"type": "kedro_datasets.pandas.CSVDataset", "save_args": {"index": False}},
    {"type": CSVDataset},
]


class FakeDataset:  # pylint: disable=too-few-public-methods
    pass


class TestPartitionedDatasetLocal:
    @pytest.mark.parametrize("dataset", LOCAL_DATASET_DEFINITION)
    @pytest.mark.parametrize(
        "suffix,expected_num_parts", [("", 5), (".csv", 3), ("p4", 1)]
    )
    def test_load(
        self, dataset, local_csvs, partitioned_data_pandas, suffix, expected_num_parts
    ):
        pds = PartitionedDataset(
            path=str(local_csvs), dataset=dataset, filename_suffix=suffix
        )
        loaded_partitions = pds.load()

        assert len(loaded_partitions.keys()) == expected_num_parts
        for partition_id, load_func in loaded_partitions.items():
            df = load_func()
            assert_frame_equal(df, partitioned_data_pandas[partition_id + suffix])
            if suffix:
                assert not partition_id.endswith(suffix)

    @pytest.mark.parametrize("dataset", LOCAL_DATASET_DEFINITION)
    @pytest.mark.parametrize("suffix", ["", ".csv"])
    def test_save(self, dataset, local_csvs, suffix):
        pds = PartitionedDataset(
            path=str(local_csvs), dataset=dataset, filename_suffix=suffix
        )
        original_data = pd.DataFrame({"foo": 42, "bar": ["a", "b", None]})
        part_id = "new/data"
        pds.save({part_id: original_data})

        assert (local_csvs / "new" / ("data" + suffix)).is_file()
        loaded_partitions = pds.load()
        assert part_id in loaded_partitions
        reloaded_data = loaded_partitions[part_id]()
        assert_frame_equal(reloaded_data, original_data)

    @pytest.mark.parametrize("dataset", LOCAL_DATASET_DEFINITION)
    @pytest.mark.parametrize("suffix", ["", ".csv"])
    def test_lazy_save(self, dataset, local_csvs, suffix):
        pds = PartitionedDataset(
            path=str(local_csvs), dataset=dataset, filename_suffix=suffix
        )

        def original_data():
            return pd.DataFrame({"foo": 42, "bar": ["a", "b", None]})

        part_id = "new/data"
        pds.save({part_id: original_data})

        assert (local_csvs / "new" / ("data" + suffix)).is_file()
        loaded_partitions = pds.load()
        assert part_id in loaded_partitions
        reloaded_data = loaded_partitions[part_id]()
        assert_frame_equal(reloaded_data, original_data())

    def test_save_invalidates_cache(self, local_csvs, mocker):
        """Test that save calls invalidate partition cache"""
        pds = PartitionedDataset(path=str(local_csvs), dataset="pandas.CSVDataset")
        mocked_fs_invalidate = mocker.patch.object(pds._filesystem, "invalidate_cache")
        first_load = pds.load()
        assert pds._partition_cache.currsize == 1
        mocked_fs_invalidate.assert_not_called()

        # save clears cache
        data = pd.DataFrame({"foo": 42, "bar": ["a", "b", None]})
        new_partition = "new/data.csv"
        pds.save({new_partition: data})
        assert pds._partition_cache.currsize == 0
        # it seems that `_filesystem.invalidate_cache` calls itself inside,
        # resulting in not one, but 2 mock calls
        # hence using `assert_any_call` instead of `assert_called_once_with`
        mocked_fs_invalidate.assert_any_call(pds._normalized_path)

        # new load returns new partition too
        second_load = pds.load()
        assert new_partition not in first_load
        assert new_partition in second_load

    @pytest.mark.parametrize("overwrite,expected_num_parts", [(False, 6), (True, 1)])
    def test_overwrite(self, local_csvs, overwrite, expected_num_parts):
        pds = PartitionedDataset(
            path=str(local_csvs), dataset="pandas.CSVDataset", overwrite=overwrite
        )
        original_data = pd.DataFrame({"foo": 42, "bar": ["a", "b", None]})
        part_id = "new/data"
        pds.save({part_id: original_data})
        loaded_partitions = pds.load()

        assert part_id in loaded_partitions
        assert len(loaded_partitions.keys()) == expected_num_parts

    def test_release_instance_cache(self, local_csvs):
        """Test that cache invalidation does not affect other instances"""
        ds_a = PartitionedDataset(path=str(local_csvs), dataset="pandas.CSVDataset")
        ds_a.load()
        ds_b = PartitionedDataset(path=str(local_csvs), dataset="pandas.CSVDataset")
        ds_b.load()

        assert ds_a._partition_cache.currsize == 1
        assert ds_b._partition_cache.currsize == 1

        # invalidate cache of the dataset A
        ds_a.release()
        assert ds_a._partition_cache.currsize == 0
        # cache of the dataset B is unaffected
        assert ds_b._partition_cache.currsize == 1

    @pytest.mark.parametrize("dataset", ["pandas.CSVDataset", "pandas.ParquetDataset"])
    def test_exists(self, local_csvs, dataset):
        assert PartitionedDataset(path=str(local_csvs), dataset=dataset).exists()

        empty_folder = local_csvs / "empty" / "folder"
        assert not PartitionedDataset(path=str(empty_folder), dataset=dataset).exists()
        empty_folder.mkdir(parents=True)
        assert not PartitionedDataset(path=str(empty_folder), dataset=dataset).exists()

    @pytest.mark.parametrize("dataset", LOCAL_DATASET_DEFINITION)
    def test_release(self, dataset, local_csvs):
        partition_to_remove = "p2.csv"
        pds = PartitionedDataset(path=str(local_csvs), dataset=dataset)
        initial_load = pds.load()
        assert partition_to_remove in initial_load

        (local_csvs / partition_to_remove).unlink()
        cached_load = pds.load()
        assert initial_load.keys() == cached_load.keys()

        pds.release()
        load_after_release = pds.load()
        assert initial_load.keys() ^ load_after_release.keys() == {partition_to_remove}

    @pytest.mark.parametrize("dataset", LOCAL_DATASET_DEFINITION)
    def test_describe(self, dataset):
        path = str(Path.cwd())
        pds = PartitionedDataset(path=path, dataset=dataset)

        assert f"path={path}" in str(pds)
        assert "dataset_type=CSVDataset" in str(pds)
        assert "dataset_config" in str(pds)

    def test_load_args(self, mocker):
        fake_partition_name = "fake_partition"
        mocked_filesystem = mocker.patch("fsspec.filesystem")
        mocked_find = mocked_filesystem.return_value.find
        mocked_find.return_value = [fake_partition_name]

        path = str(Path.cwd())
        load_args = {"maxdepth": 42, "withdirs": True}
        pds = PartitionedDataset(
            path=path, dataset="pandas.CSVDataset", load_args=load_args
        )
        mocker.patch.object(pds, "_path_to_partition", return_value=fake_partition_name)

        assert pds.load().keys() == {fake_partition_name}
        mocked_find.assert_called_once_with(path, **load_args)

    @pytest.mark.parametrize(
        "credentials,expected_pds_creds,expected_dataset_creds",
        [({"cred": "common"}, {"cred": "common"}, {"cred": "common"}), (None, {}, {})],
    )
    def test_credentials(
        self, mocker, credentials, expected_pds_creds, expected_dataset_creds
    ):
        mocked_filesystem = mocker.patch("fsspec.filesystem")
        path = str(Path.cwd())
        pds = PartitionedDataset(
            path=path, dataset="pandas.CSVDataset", credentials=credentials
        )

        assert mocked_filesystem.call_count == 2
        mocked_filesystem.assert_called_with("file", **expected_pds_creds)
        if expected_dataset_creds:
            assert pds._dataset_config[CREDENTIALS_KEY] == expected_dataset_creds
        else:
            assert CREDENTIALS_KEY not in pds._dataset_config

        str_repr = str(pds)

        def _assert_not_in_repr(value):
            if isinstance(value, dict):
                for k_, v_ in value.items():
                    _assert_not_in_repr(k_)
                    _assert_not_in_repr(v_)
            if value is not None:
                assert str(value) not in str_repr

        _assert_not_in_repr(credentials)

    def test_fs_args(self, mocker):
        fs_args = {"foo": "bar"}

        mocked_filesystem = mocker.patch("fsspec.filesystem")
        path = str(Path.cwd())
        pds = PartitionedDataset(
            path=path, dataset="pandas.CSVDataset", fs_args=fs_args
        )

        assert mocked_filesystem.call_count == 2
        mocked_filesystem.assert_called_with("file", **fs_args)
        assert pds._dataset_config["fs_args"] == fs_args

    @pytest.mark.parametrize("dataset", ["pandas.ParquetDataset", ParquetDataset])
    def test_invalid_dataset(self, dataset, local_csvs):
        pds = PartitionedDataset(path=str(local_csvs), dataset=dataset)
        loaded_partitions = pds.load()

        for partition, df_loader in loaded_partitions.items():
            pattern = r"Failed while loading data from data set ParquetDataset(.*)"
            with pytest.raises(DatasetError, match=pattern) as exc_info:
                df_loader()
            error_message = str(exc_info.value)
            assert (
                "Either the file is corrupted or this is not a parquet file"
                in error_message
            )
            assert str(partition) in error_message

    @pytest.mark.parametrize(
        "dataset_config,error_pattern",
        [
            ("UndefinedDatasetType", "Class 'UndefinedDatasetType' not found"),
            (
                "missing.module.UndefinedDatasetType",
                r"Class 'missing\.module\.UndefinedDatasetType' not found",
            ),
            (
                FakeDataset,
                r"Dataset type 'tests\.partitions\.test_partitioned_dataset\.FakeDataset' "
                r"is invalid\: all data set types must extend 'AbstractDataset'",
            ),
            ({}, "'type' is missing from dataset catalog configuration"),
        ],
    )
    def test_invalid_dataset_config(self, dataset_config, error_pattern):
        with pytest.raises(DatasetError, match=error_pattern):
            PartitionedDataset(path=str(Path.cwd()), dataset=dataset_config)

    @pytest.mark.parametrize(
        "dataset_config",
        [
            {**ds_config, "versioned": True}
            for ds_config in LOCAL_DATASET_DEFINITION
            if isinstance(ds_config, dict)
        ],
    )
    @pytest.mark.parametrize(
        "suffix,expected_num_parts", [("", 5), (".csv", 3), ("p4", 1)]
    )
    def test_versioned_dataset_save_and_load(
        self,
        mocker,
        filepath_csvs,
        dataset_config,
        suffix,
        expected_num_parts,
        partitioned_data_pandas,
    ):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        save_version = "2020-01-01T00.00.00.000Z"
        mock_ts = mocker.patch(
            "kedro.io.core.generate_timestamp", return_value=save_version
        )
        PartitionedDataset(path=filepath_csvs, dataset=dataset_config).save(
            partitioned_data_pandas
        )
        mock_ts.assert_called_once()

        pds = PartitionedDataset(
            path=filepath_csvs, dataset=dataset_config, filename_suffix=suffix
        )
        loaded_partitions = pds.load()

        assert len(loaded_partitions) == expected_num_parts
        actual_save_versions = set()
        for partition_id, load_func in loaded_partitions.items():
            partition_dir = Path(filepath_csvs, partition_id + suffix)
            actual_save_versions |= {each.name for each in partition_dir.iterdir()}
            df = load_func()
            assert_frame_equal(df, partitioned_data_pandas[partition_id + suffix])
            if suffix:
                assert not partition_id.endswith(suffix)

        if expected_num_parts:
            # all partitions were saved using the same version string
            assert actual_save_versions == {save_version}

    def test_malformed_versioned_path(self, tmp_path):
        local_dir = tmp_path / "files"
        local_dir.mkdir()

        path = local_dir / "path/to/folder/new/partition/version/partition/file"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("content")

        pds = PartitionedDataset(
            path=str(local_dir / "path/to/folder"),
            dataset={"type": "pandas.CSVDataset", "versioned": True},
        )

        pattern = re.escape(
            f"`{path.as_posix()}` is not a well-formed versioned path ending with "
            f"`filename/timestamp/filename` (got `version/partition/file`)."
        )
        with pytest.raises(DatasetError, match=pattern):
            pds.load()

    def test_no_partitions(self, tmpdir):
        pds = PartitionedDataset(path=str(tmpdir), dataset="pandas.CSVDataset")

        pattern = re.escape(f"No partitions found in '{tmpdir}'")
        with pytest.raises(DatasetError, match=pattern):
            pds.load()

    @pytest.mark.parametrize(
        "pds_config,filepath_arg",
        [
            (
                {
                    "path": str(Path.cwd()),
                    "dataset": {"type": CSVDataset, "filepath": "fake_path"},
                },
                "filepath",
            ),
            (
                {
                    "path": str(Path.cwd()),
                    "dataset": {"type": CSVDataset, "other_arg": "fake_path"},
                    "filepath_arg": "other_arg",
                },
                "other_arg",
            ),
        ],
    )
    def test_filepath_arg_warning(self, pds_config, filepath_arg):
        pattern = (
            f"'{filepath_arg}' key must not be specified in the dataset definition as it "
            f"will be overwritten by partition path"
        )
        with pytest.warns(UserWarning, match=re.escape(pattern)):
            PartitionedDataset(**pds_config)

    def test_credentials_log_warning(self, caplog):
        """Check that the warning is logged if the dataset credentials will overwrite
        the top-level ones"""
        pds = PartitionedDataset(
            path=str(Path.cwd()),
            dataset={"type": CSVDataset, "credentials": {"secret": "dataset"}},
            credentials={"secret": "global"},
        )
        log_message = KEY_PROPAGATION_WARNING % {
            "keys": "credentials",
            "target": "underlying dataset",
        }
        assert caplog.record_tuples == [("kedro.io.core", logging.WARNING, log_message)]
        assert pds._dataset_config["credentials"] == {"secret": "dataset"}

    def test_fs_args_log_warning(self, caplog):
        """Check that the warning is logged if the dataset filesystem
        arguments will overwrite the top-level ones"""
        pds = PartitionedDataset(
            path=str(Path.cwd()),
            dataset={"type": CSVDataset, "fs_args": {"args": "dataset"}},
            fs_args={"args": "dataset"},
        )
        log_message = KEY_PROPAGATION_WARNING % {
            "keys": "filesystem arguments",
            "target": "underlying dataset",
        }
        assert caplog.record_tuples == [("kedro.io.core", logging.WARNING, log_message)]
        assert pds._dataset_config["fs_args"] == {"args": "dataset"}

    @pytest.mark.parametrize(
        "pds_config,expected_ds_creds,global_creds",
        [
            (
                {"dataset": "pandas.CSVDataset", "credentials": {"secret": "global"}},
                {"secret": "global"},
                {"secret": "global"},
            ),
            (
                {
                    "dataset": {
                        "type": CSVDataset,
                        "credentials": {"secret": "expected"},
                    },
                },
                {"secret": "expected"},
                {},
            ),
            (
                {
                    "dataset": {"type": CSVDataset, "credentials": None},
                    "credentials": {"secret": "global"},
                },
                None,
                {"secret": "global"},
            ),
            (
                {
                    "dataset": {
                        "type": CSVDataset,
                        "credentials": {"secret": "expected"},
                    },
                    "credentials": {"secret": "global"},
                },
                {"secret": "expected"},
                {"secret": "global"},
            ),
        ],
    )
    def test_dataset_creds(self, pds_config, expected_ds_creds, global_creds):
        """Check that global credentials do not interfere dataset credentials."""
        pds = PartitionedDataset(path=str(Path.cwd()), **pds_config)
        assert pds._dataset_config["credentials"] == expected_ds_creds
        assert pds._credentials == global_creds


BUCKET_NAME = "fake_bucket_name"
S3_DATASET_DEFINITION = [
    "pandas.CSVDataset",
    "kedro_datasets.pandas.CSVDataset",
    CSVDataset,
    {"type": "kedro_datasets.pandas.CSVDataset", "save_args": {"index": False}},
    {"type": CSVDataset},
]


@pytest.fixture
def mocked_s3_bucket():
    """Create a bucket for testing using moto."""
    with mock_s3():
        conn = boto3.client(
            "s3",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        conn.create_bucket(Bucket=BUCKET_NAME)
        yield conn


@pytest.fixture
def mocked_csvs_in_s3(mocked_s3_bucket, partitioned_data_pandas):
    prefix = "csvs"
    for key, data in partitioned_data_pandas.items():
        mocked_s3_bucket.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{prefix}/{key}",
            Body=data.to_csv(index=False),
        )
    return f"s3://{BUCKET_NAME}/{prefix}"


class TestPartitionedDatasetS3:
    os.environ["AWS_ACCESS_KEY_ID"] = "FAKE_ACCESS_KEY"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "FAKE_SECRET_KEY"

    @pytest.mark.parametrize("dataset", S3_DATASET_DEFINITION)
    def test_load(self, dataset, mocked_csvs_in_s3, partitioned_data_pandas):
        pds = PartitionedDataset(path=mocked_csvs_in_s3, dataset=dataset)
        loaded_partitions = pds.load()

        assert loaded_partitions.keys() == partitioned_data_pandas.keys()
        for partition_id, load_func in loaded_partitions.items():
            df = load_func()
            assert_frame_equal(df, partitioned_data_pandas[partition_id])

    def test_load_s3a(self, mocked_csvs_in_s3, partitioned_data_pandas, mocker):
        path = mocked_csvs_in_s3.split("://", 1)[1]
        s3a_path = f"s3a://{path}"
        # any type is fine as long as it passes isinstance check
        # since _dataset_type is mocked later anyways
        pds = PartitionedDataset(path=s3a_path, dataset="pandas.CSVDataset")
        assert pds._protocol == "s3a"

        mocked_ds = mocker.patch.object(pds, "_dataset_type")
        mocked_ds.__name__ = "mocked"
        loaded_partitions = pds.load()

        assert loaded_partitions.keys() == partitioned_data_pandas.keys()
        assert mocked_ds.call_count == len(loaded_partitions)
        expected = [
            mocker.call(filepath=f"{s3a_path}/{partition_id}")
            for partition_id in loaded_partitions
        ]
        mocked_ds.assert_has_calls(expected, any_order=True)

    @pytest.mark.parametrize("dataset", S3_DATASET_DEFINITION)
    def test_save(self, dataset, mocked_csvs_in_s3):
        pds = PartitionedDataset(path=mocked_csvs_in_s3, dataset=dataset)
        original_data = pd.DataFrame({"foo": 42, "bar": ["a", "b", None]})
        part_id = "new/data.csv"
        pds.save({part_id: original_data})

        s3 = s3fs.S3FileSystem()
        assert s3.exists("/".join([mocked_csvs_in_s3, part_id]))

        loaded_partitions = pds.load()
        assert part_id in loaded_partitions
        reloaded_data = loaded_partitions[part_id]()
        assert_frame_equal(reloaded_data, original_data)

    def test_save_s3a(self, mocked_csvs_in_s3, mocker):
        """Test that save works in case of s3a protocol"""
        path = mocked_csvs_in_s3.split("://", 1)[1]
        s3a_path = f"s3a://{path}"
        # any type is fine as long as it passes isinstance check
        # since _dataset_type is mocked later anyways
        pds = PartitionedDataset(
            path=s3a_path, dataset="pandas.CSVDataset", filename_suffix=".csv"
        )
        assert pds._protocol == "s3a"

        mocked_ds = mocker.patch.object(pds, "_dataset_type")
        mocked_ds.__name__ = "mocked"
        new_partition = "new/data"
        data = "data"

        pds.save({new_partition: data})
        mocked_ds.assert_called_once_with(filepath=f"{s3a_path}/{new_partition}.csv")
        mocked_ds.return_value.save.assert_called_once_with(data)

    @pytest.mark.parametrize("dataset", ["pandas.CSVDataset", "pandas.HDFDataset"])
    def test_exists(self, dataset, mocked_csvs_in_s3):
        assert PartitionedDataset(path=mocked_csvs_in_s3, dataset=dataset).exists()

        empty_folder = "/".join([mocked_csvs_in_s3, "empty", "folder"])
        assert not PartitionedDataset(path=empty_folder, dataset=dataset).exists()

        s3fs.S3FileSystem().mkdir(empty_folder)
        assert not PartitionedDataset(path=empty_folder, dataset=dataset).exists()

    @pytest.mark.parametrize("dataset", S3_DATASET_DEFINITION)
    def test_release(self, dataset, mocked_csvs_in_s3):
        partition_to_remove = "p2.csv"
        pds = PartitionedDataset(path=mocked_csvs_in_s3, dataset=dataset)
        initial_load = pds.load()
        assert partition_to_remove in initial_load

        s3 = s3fs.S3FileSystem()
        s3.rm("/".join([mocked_csvs_in_s3, partition_to_remove]))
        cached_load = pds.load()
        assert initial_load.keys() == cached_load.keys()

        pds.release()
        load_after_release = pds.load()
        assert initial_load.keys() ^ load_after_release.keys() == {partition_to_remove}

    @pytest.mark.parametrize("dataset", S3_DATASET_DEFINITION)
    def test_describe(self, dataset):
        path = f"s3://{BUCKET_NAME}/foo/bar"
        pds = PartitionedDataset(path=path, dataset=dataset)

        assert f"path={path}" in str(pds)
        assert "dataset_type=CSVDataset" in str(pds)
        assert "dataset_config" in str(pds)
