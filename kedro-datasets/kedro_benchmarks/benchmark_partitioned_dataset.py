"""ASV benchmarks for ``PartitionedDataset.load()``."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import fsspec
import pandas as pd
from kedro.io.core import DatasetError

from kedro_datasets.partitions import PartitionedDataset

PARTITION_COUNTS = (10, 1000)
FILESYSTEMS = ("local", "s3")
REPEAT_COUNT = 5
S3_BUCKET_NAME = "kedro-partitioned-dataset-benchmarks"
S3_PREFIX = "partitions"


class _FakeS3FileSystem:
    """Minimal S3-like filesystem backed by the local filesystem."""

    sep = "/"

    def __init__(self, root_path: Path):
        self._root_path = root_path

    def _strip_protocol(self, path: str) -> str:
        return path.split("://", 1)[1] if "://" in path else path

    def invalidate_cache(self, *_args, **_kwargs):
        return None

    def exists(self, path: str) -> bool:
        return (self._root_path / self._strip_protocol(path)).exists()

    def glob(self, path: str, **_kwargs) -> list[str]:
        local_pattern = self._root_path / self._strip_protocol(path)
        return [
            f"s3://{file_path.relative_to(self._root_path).as_posix()}"
            for file_path in self._root_path.glob(str(local_pattern.relative_to(self._root_path)))
            if file_path.is_file()
        ]

    def find(self, path: str, **_kwargs) -> list[str]:
        local_path = self._root_path / self._strip_protocol(path)
        if not local_path.exists():
            return []

        return [
            f"s3://{file_path.relative_to(self._root_path).as_posix()}"
            for file_path in local_path.rglob("*")
            if file_path.is_file()
        ]


class CachedPartitionedDataset(PartitionedDataset):
    """A benchmark-only variant that preserves the old cached load behavior."""

    def load(self):
        partitions = {}

        for partition_file_path in self._list_partitions():
            kwargs = self._dataset_config.copy()
            kwargs[self._filepath_arg] = self._join_protocol(partition_file_path)
            dataset = self._dataset_type(**kwargs)  # type: ignore[misc]
            partition_id = self._path_to_partition(partition_file_path)
            partitions[partition_id] = dataset.load

        if not partitions:
            raise DatasetError(f"No partitions found in '{self._path}'")

        return partitions


class TimePartitionedDatasetLoad:
    """Benchmark the load path for local and mocked S3 partitioned datasets."""

    params = (FILESYSTEMS, PARTITION_COUNTS)
    param_names = ("filesystem", "partition_count")

    def setup(self, filesystem: str, partition_count: int):
        self._tempdir = TemporaryDirectory()
        self._real_filesystem_factory = fsspec.filesystem
        self._filesystem_patch = patch(
            "kedro_datasets.partitions.partitioned_dataset.fsspec.filesystem",
            side_effect=self._filesystem_factory,
        )
        self._filesystem_patch.start()

        self.partition_count = partition_count
        self.partition_data = self._build_partition_data(partition_count)
        self._fake_s3_root = Path(self._tempdir.name)
        self._fake_s3_filesystem = _FakeS3FileSystem(self._fake_s3_root)

        if filesystem == "local":
            self.dataset_path = self._prepare_local_dataset()
        else:
            self.dataset_path = self._prepare_mock_s3_dataset()

        self.current_dataset = PartitionedDataset(
            path=self.dataset_path,
            dataset="pandas.CSVDataset",
            filename_suffix=".csv",
        )
        self.cached_dataset = CachedPartitionedDataset(
            path=self.dataset_path,
            dataset="pandas.CSVDataset",
            filename_suffix=".csv",
        )

    def teardown(self, _filesystem: str, _partition_count: int):
        self._filesystem_patch.stop()
        self._tempdir.cleanup()

    def _filesystem_factory(self, protocol: str, **_kwargs):
        if protocol in {"s3", "s3a", "s3n"}:
            return self._fake_s3_filesystem
        return self._real_filesystem_factory(protocol, **_kwargs)

    def _build_partition_data(self, partition_count: int) -> dict[str, pd.DataFrame]:
        return {
            f"partition_{index:05d}/data.csv": pd.DataFrame(
                {"partition": [index], "value": [index * 2]}
            )
            for index in range(partition_count)
        }

    def _prepare_local_dataset(self) -> str:
        base_path = Path(self._tempdir.name) / f"local_{self.partition_count}"
        for partition_path, dataframe in self.partition_data.items():
            file_path = base_path / partition_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(dataframe.to_csv(index=False), encoding="utf-8")
        return str(base_path)

    def _prepare_mock_s3_dataset(self) -> str:
        prefix = f"{S3_PREFIX}/{self.partition_count}"
        s3_root = self._fake_s3_root / S3_BUCKET_NAME / prefix
        for partition_path, dataframe in self.partition_data.items():
            file_path = s3_root / partition_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(dataframe.to_csv(index=False), encoding="utf-8")
        return f"s3://{S3_BUCKET_NAME}/{prefix}"

    def time_load_current(self, _filesystem: str, _partition_count: int):
        """Benchmark the current re-scan-on-load behavior."""
        self.current_dataset.load()

    def time_load_cached(self, _filesystem: str, _partition_count: int):
        """Benchmark the old cached load behavior for comparison."""
        self.cached_dataset.load()

    def time_repeated_load_current(self, _filesystem: str, _partition_count: int):
        """Benchmark repeated calls to load() on the current implementation."""
        for _ in range(REPEAT_COUNT):
            self.current_dataset.load()

    def time_repeated_load_cached(self, _filesystem: str, _partition_count: int):
        """Benchmark repeated calls to load() on the cached implementation."""
        for _ in range(REPEAT_COUNT):
            self.cached_dataset.load()
