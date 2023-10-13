from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
import pytest
from kedro.io.core import AbstractDataset, DatasetError
from kedro.io.data_catalog import CREDENTIALS_KEY
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

from kedro_datasets.partitions import IncrementalDataset
from kedro_datasets.pickle import PickleDataset
from kedro_datasets.text import TextDataset

DATASET = "kedro_datasets.pandas.csv_dataset.CSVDataset"


@pytest.fixture
def partitioned_data_pandas():
    return {
        f"p{counter:02d}/data.csv": pd.DataFrame(
            {"part": counter, "col": list(range(counter + 1))}
        )
        for counter in range(5)
    }


@pytest.fixture
def local_csvs(tmp_path, partitioned_data_pandas):
    local_dir = Path(tmp_path / "csvs")
    local_dir.mkdir()

    for k, data in partitioned_data_pandas.items():
        path = local_dir / k
        path.parent.mkdir(parents=True)
        data.to_csv(str(path), index=False)
    return local_dir


class DummyDataset(AbstractDataset):  # pragma: no cover
    def __init__(self, filepath):
        pass

    def _describe(self) -> dict[str, Any]:
        return {"dummy": True}

    def _load(self) -> Any:
        pass

    def _save(self, data: Any) -> None:
        pass


def dummy_gt_func(value1: str, value2: str):
    return value1 > value2


def dummy_lt_func(value1: str, value2: str):
    return value1 < value2


class TestIncrementalDatasetLocal:
    def test_load_and_confirm(self, local_csvs, partitioned_data_pandas):
        """Test the standard flow for loading, confirming and reloading
        an IncrementalDataset"""
        pds = IncrementalDataset(str(local_csvs), DATASET)
        loaded = pds.load()
        assert loaded.keys() == partitioned_data_pandas.keys()
        for partition_id, data in loaded.items():
            assert_frame_equal(data, partitioned_data_pandas[partition_id])

        checkpoint_path = local_csvs / pds.DEFAULT_CHECKPOINT_FILENAME
        assert not checkpoint_path.exists()
        pds.confirm()
        assert checkpoint_path.is_file()
        assert checkpoint_path.read_text() == pds._read_checkpoint() == "p04/data.csv"

        reloaded = pds.load()
        assert reloaded.keys() == loaded.keys()

        pds.release()
        reloaded_after_release = pds.load()
        assert not reloaded_after_release

    def test_save(self, local_csvs):
        """Test saving a new partition into an IncrementalDataset"""
        df = pd.DataFrame({"dummy": [1, 2, 3]})
        new_partition_key = "p05/data.csv"
        new_partition_path = local_csvs / new_partition_key
        pds = IncrementalDataset(str(local_csvs), DATASET)

        assert not new_partition_path.exists()
        assert new_partition_key not in pds.load()

        pds.save({new_partition_key: df})
        assert new_partition_path.exists()
        loaded = pds.load()
        assert_frame_equal(loaded[new_partition_key], df)

    @pytest.mark.parametrize(
        "filename_suffix,expected_partitions",
        [
            (
                "",
                {
                    "p00/data.csv",
                    "p01/data.csv",
                    "p02/data.csv",
                    "p03/data.csv",
                    "p04/data.csv",
                },
            ),
            (".csv", {"p00/data", "p01/data", "p02/data", "p03/data", "p04/data"}),
            (".fake", set()),
        ],
    )
    def test_filename_suffix(self, filename_suffix, expected_partitions, local_csvs):
        """Test how specifying filename_suffix affects the available
        partitions and their names"""
        pds = IncrementalDataset(
            str(local_csvs), DATASET, filename_suffix=filename_suffix
        )
        loaded = pds.load()
        assert loaded.keys() == expected_partitions

    @pytest.mark.parametrize(
        "forced_checkpoint,expected_partitions",
        [
            (
                "",
                {
                    "p00/data.csv",
                    "p01/data.csv",
                    "p02/data.csv",
                    "p03/data.csv",
                    "p04/data.csv",
                },
            ),
            (
                "p00/data.csv",
                {"p01/data.csv", "p02/data.csv", "p03/data.csv", "p04/data.csv"},
            ),
            ("p03/data.csv", {"p04/data.csv"}),
        ],
    )
    def test_force_checkpoint_no_checkpoint_file(
        self, forced_checkpoint, expected_partitions, local_csvs
    ):
        """Test how forcing checkpoint value affects the available partitions
        if the checkpoint file does not exist"""
        pds = IncrementalDataset(str(local_csvs), DATASET, checkpoint=forced_checkpoint)
        loaded = pds.load()
        assert loaded.keys() == expected_partitions

        confirm_path = local_csvs / pds.DEFAULT_CHECKPOINT_FILENAME
        assert not confirm_path.exists()
        pds.confirm()
        assert confirm_path.is_file()
        assert confirm_path.read_text() == max(expected_partitions)

    @pytest.mark.parametrize(
        "forced_checkpoint,expected_partitions",
        [
            (
                "",
                {
                    "p00/data.csv",
                    "p01/data.csv",
                    "p02/data.csv",
                    "p03/data.csv",
                    "p04/data.csv",
                },
            ),
            (
                "p00/data.csv",
                {"p01/data.csv", "p02/data.csv", "p03/data.csv", "p04/data.csv"},
            ),
            ("p03/data.csv", {"p04/data.csv"}),
        ],
    )
    def test_force_checkpoint_checkpoint_file_exists(
        self, forced_checkpoint, expected_partitions, local_csvs
    ):
        """Test how forcing checkpoint value affects the available partitions
        if the checkpoint file exists"""
        IncrementalDataset(str(local_csvs), DATASET).confirm()
        checkpoint = local_csvs / IncrementalDataset.DEFAULT_CHECKPOINT_FILENAME
        assert checkpoint.read_text() == "p04/data.csv"

        pds = IncrementalDataset(str(local_csvs), DATASET, checkpoint=forced_checkpoint)
        assert pds._checkpoint.exists()
        loaded = pds.load()
        assert loaded.keys() == expected_partitions

    @pytest.mark.parametrize(
        "forced_checkpoint", ["p04/data.csv", "p10/data.csv", "p100/data.csv"]
    )
    def test_force_checkpoint_no_partitions(self, forced_checkpoint, local_csvs):
        """Test that forcing the checkpoint to certain values results in no
        partitions being returned"""
        pds = IncrementalDataset(str(local_csvs), DATASET, checkpoint=forced_checkpoint)
        loaded = pds.load()
        assert not loaded

        confirm_path = local_csvs / pds.DEFAULT_CHECKPOINT_FILENAME
        assert not confirm_path.exists()
        pds.confirm()
        # confirming with no partitions available must have no effect
        assert not confirm_path.exists()

    def test_checkpoint_path(self, local_csvs, partitioned_data_pandas):
        """Test configuring a different checkpoint path"""
        checkpoint_path = local_csvs / "checkpoint_folder" / "checkpoint_file"
        assert not checkpoint_path.exists()

        IncrementalDataset(
            str(local_csvs), DATASET, checkpoint={"filepath": str(checkpoint_path)}
        ).confirm()
        assert checkpoint_path.is_file()
        assert checkpoint_path.read_text() == max(partitioned_data_pandas)

    @pytest.mark.parametrize(
        "checkpoint_config,expected_checkpoint_class",
        [
            (None, TextDataset),
            ({"type": "kedro_datasets.pickle.PickleDataset"}, PickleDataset),
            (
                {"type": "tests.partitions.test_incremental_dataset.DummyDataset"},
                DummyDataset,
            ),
        ],
    )
    def test_checkpoint_type(
        self, tmp_path, checkpoint_config, expected_checkpoint_class
    ):
        """Test configuring a different checkpoint dataset type"""
        pds = IncrementalDataset(str(tmp_path), DATASET, checkpoint=checkpoint_config)
        assert isinstance(pds._checkpoint, expected_checkpoint_class)

    @pytest.mark.parametrize(
        "checkpoint_config,error_pattern",
        [
            (
                {"versioned": True},
                "'IncrementalDataset' does not support versioning "
                "of the checkpoint. Please remove 'versioned' key from the "
                "checkpoint definition.",
            ),
            (
                {"version": None},
                "'IncrementalDataset' does not support versioning "
                "of the checkpoint. Please remove 'version' key from the "
                "checkpoint definition.",
            ),
        ],
    )
    def test_version_not_allowed(self, tmp_path, checkpoint_config, error_pattern):
        """Test that invalid checkpoint configurations raise expected errors"""
        with pytest.raises(DatasetError, match=re.escape(error_pattern)):
            IncrementalDataset(str(tmp_path), DATASET, checkpoint=checkpoint_config)

    @pytest.mark.parametrize(
        "pds_config,fs_creds,dataset_creds,checkpoint_creds",
        [
            (
                {"dataset": DATASET, "credentials": {"cred": "common"}},
                {"cred": "common"},
                {"cred": "common"},
                {"cred": "common"},
            ),
            (
                {
                    "dataset": {"type": DATASET, "credentials": {"ds": "only"}},
                    "credentials": {"cred": "common"},
                },
                {"cred": "common"},
                {"ds": "only"},
                {"cred": "common"},
            ),
            (
                {
                    "dataset": DATASET,
                    "credentials": {"cred": "common"},
                    "checkpoint": {"credentials": {"cp": "only"}},
                },
                {"cred": "common"},
                {"cred": "common"},
                {"cp": "only"},
            ),
            (
                {
                    "dataset": {"type": DATASET, "credentials": {"ds": "only"}},
                    "checkpoint": {"credentials": {"cp": "only"}},
                },
                {},
                {"ds": "only"},
                {"cp": "only"},
            ),
            (
                {
                    "dataset": {"type": DATASET, "credentials": None},
                    "credentials": {"cred": "common"},
                    "checkpoint": {"credentials": None},
                },
                {"cred": "common"},
                None,
                None,
            ),
        ],
    )
    def test_credentials(self, pds_config, fs_creds, dataset_creds, checkpoint_creds):
        """Test correctness of credentials propagation into the dataset and
        checkpoint constructors"""
        pds = IncrementalDataset(str(Path.cwd()), **pds_config)
        assert pds._credentials == fs_creds
        assert pds._dataset_config[CREDENTIALS_KEY] == dataset_creds
        assert pds._checkpoint_config[CREDENTIALS_KEY] == checkpoint_creds

    @pytest.mark.parametrize(
        "comparison_func,expected_partitions",
        [
            (
                "tests.partitions.test_incremental_dataset.dummy_gt_func",
                {"p03/data.csv", "p04/data.csv"},
            ),
            (dummy_gt_func, {"p03/data.csv", "p04/data.csv"}),
            (
                "tests.partitions.test_incremental_dataset.dummy_lt_func",
                {"p00/data.csv", "p01/data.csv"},
            ),
            (dummy_lt_func, {"p00/data.csv", "p01/data.csv"}),
        ],
    )
    def test_comparison_func(self, comparison_func, expected_partitions, local_csvs):
        """Test that specifying a custom function for comparing the checkpoint value
        to a partition id results in expected partitions being returned on load"""
        checkpoint_config = {
            "force_checkpoint": "p02/data.csv",
            "comparison_func": comparison_func,
        }
        pds = IncrementalDataset(str(local_csvs), DATASET, checkpoint=checkpoint_config)
        assert pds.load().keys() == expected_partitions


BUCKET_NAME = "fake_bucket_name"


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


class TestIncrementalDatasetS3:
    os.environ["AWS_ACCESS_KEY_ID"] = "FAKE_ACCESS_KEY"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "FAKE_SECRET_KEY"

    def test_load_and_confirm(self, mocked_csvs_in_s3, partitioned_data_pandas):
        """Test the standard flow for loading, confirming and reloading
        a IncrementalDataset in S3"""
        pds = IncrementalDataset(mocked_csvs_in_s3, DATASET)
        assert pds._checkpoint._protocol == "s3"
        loaded = pds.load()
        assert loaded.keys() == partitioned_data_pandas.keys()
        for partition_id, data in loaded.items():
            assert_frame_equal(data, partitioned_data_pandas[partition_id])

        assert not pds._checkpoint.exists()
        assert pds._read_checkpoint() is None
        pds.confirm()
        assert pds._checkpoint.exists()
        assert pds._read_checkpoint() == max(partitioned_data_pandas)

    def test_load_and_confirm_s3a(
        self, mocked_csvs_in_s3, partitioned_data_pandas, mocker
    ):
        s3a_path = f"s3a://{mocked_csvs_in_s3.split('://', 1)[1]}"
        pds = IncrementalDataset(s3a_path, DATASET)
        assert pds._protocol == "s3a"
        assert pds._checkpoint._protocol == "s3"

        mocked_ds = mocker.patch.object(pds, "_dataset_type")
        mocked_ds.__name__ = "mocked"
        loaded = pds.load()

        assert loaded.keys() == partitioned_data_pandas.keys()
        assert not pds._checkpoint.exists()
        assert pds._read_checkpoint() is None
        pds.confirm()
        assert pds._checkpoint.exists()
        assert pds._read_checkpoint() == max(partitioned_data_pandas)

    @pytest.mark.parametrize(
        "forced_checkpoint,expected_partitions",
        [
            (
                "",
                {
                    "p00/data.csv",
                    "p01/data.csv",
                    "p02/data.csv",
                    "p03/data.csv",
                    "p04/data.csv",
                },
            ),
            (
                "p00/data.csv",
                {"p01/data.csv", "p02/data.csv", "p03/data.csv", "p04/data.csv"},
            ),
            ("p03/data.csv", {"p04/data.csv"}),
        ],
    )
    def test_force_checkpoint_no_checkpoint_file(
        self, forced_checkpoint, expected_partitions, mocked_csvs_in_s3
    ):
        """Test how forcing checkpoint value affects the available partitions
        in S3 if the checkpoint file does not exist"""
        pds = IncrementalDataset(
            mocked_csvs_in_s3, DATASET, checkpoint=forced_checkpoint
        )
        loaded = pds.load()
        assert loaded.keys() == expected_partitions

        assert not pds._checkpoint.exists()
        pds.confirm()
        assert pds._checkpoint.exists()
        assert pds._checkpoint.load() == max(expected_partitions)

    @pytest.mark.parametrize(
        "forced_checkpoint,expected_partitions",
        [
            (
                "",
                {
                    "p00/data.csv",
                    "p01/data.csv",
                    "p02/data.csv",
                    "p03/data.csv",
                    "p04/data.csv",
                },
            ),
            (
                "p00/data.csv",
                {"p01/data.csv", "p02/data.csv", "p03/data.csv", "p04/data.csv"},
            ),
            ("p03/data.csv", {"p04/data.csv"}),
        ],
    )
    def test_force_checkpoint_checkpoint_file_exists(
        self, forced_checkpoint, expected_partitions, mocked_csvs_in_s3
    ):
        """Test how forcing checkpoint value affects the available partitions
        in S3 if the checkpoint file exists"""
        # create checkpoint and assert that it exists
        IncrementalDataset(mocked_csvs_in_s3, DATASET).confirm()
        checkpoint_path = (
            f"{mocked_csvs_in_s3}/{IncrementalDataset.DEFAULT_CHECKPOINT_FILENAME}"
        )
        checkpoint_value = TextDataset(checkpoint_path).load()
        assert checkpoint_value == "p04/data.csv"

        pds = IncrementalDataset(
            mocked_csvs_in_s3, DATASET, checkpoint=forced_checkpoint
        )
        assert pds._checkpoint.exists()
        loaded = pds.load()
        assert loaded.keys() == expected_partitions

    @pytest.mark.parametrize(
        "forced_checkpoint", ["p04/data.csv", "p10/data.csv", "p100/data.csv"]
    )
    def test_force_checkpoint_no_partitions(self, forced_checkpoint, mocked_csvs_in_s3):
        """Test that forcing the checkpoint to certain values results in no
        partitions returned from S3"""
        pds = IncrementalDataset(
            mocked_csvs_in_s3, DATASET, checkpoint=forced_checkpoint
        )
        loaded = pds.load()
        assert not loaded

        assert not pds._checkpoint.exists()
        pds.confirm()
        # confirming with no partitions available must have no effect
        assert not pds._checkpoint.exists()
