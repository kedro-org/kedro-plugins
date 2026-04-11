from __future__ import annotations

import os
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, ClassVar, TypeAlias

import fsspec
from datasets import (
    Dataset,
    DatasetDict,
    IterableDataset,
    IterableDatasetDict,
    load_dataset,
)
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

DatasetLike: TypeAlias = Dataset | DatasetDict | IterableDataset | IterableDatasetDict


class FilesystemDataset(AbstractVersionedDataset[DatasetLike, DatasetLike]):
    """Base class for Hugging Face dataset types persisted on a filesystem.

    Not intended for direct use — use a format-specific subclass instead
    (e.g. ``ArrowDataset``, ``ParquetDataset``).
    """

    BUILDER: ClassVar[str]
    EXTENSION: ClassVar[str]

    def __init__(  # noqa: PLR0913
        self,
        *,
        path: str | os.PathLike,
        version: Version | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``FilesystemDataset``.

        Args:
            path: Path to a file or directory for persisting Hugging Face
                datasets. Supports local paths, ``os.PathLike`` objects,
                and remote URIs (e.g. ``s3://bucket/data``).
            version: Optional versioning configuration
                (see :class:`~kedro.io.core.Version`).
            load_args: Additional keyword arguments passed to the
                underlying load function.
            save_args: Additional keyword arguments passed to the
                underlying save function.
            credentials: Credentials for the underlying filesystem
                (e.g. ``key``/``secret`` for S3). Passed to the
                ``storage_options`` parameter in the underlying
                ``datasets`` implementation.
            fs_args: Extra arguments passed to the ``fsspec`` filesystem
                initialiser. Passed to the ``storage_options`` parameter
                in the underlying ``datasets`` implementation.
            metadata: Any arbitrary metadata. This is ignored by Kedro
                but may be consumed by users or external plugins.
        """
        _fs_args = deepcopy(fs_args) or {}
        _credentials = deepcopy(credentials) or {}

        protocol, resolved_path = get_protocol_and_path(path, version)
        self._protocol = protocol

        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

        self._load_args = load_args or {}
        self._save_args = save_args or {}
        self.metadata = metadata

        self._storage_options = {**_credentials, **_fs_args} or None

        super().__init__(
            filepath=PurePosixPath(resolved_path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

    def load(self) -> DatasetLike:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        return self._load_dataset(load_path)

    def save(self, data: DatasetLike) -> None:
        if not isinstance(
            data,
            Dataset | DatasetDict | IterableDataset | IterableDatasetDict,
        ):
            msg = (
                f"{type(self).__name__} only supports `datasets.Dataset`, "
                "`datasets.DatasetDict`, "
                "`datasets.IterableDataset`, and "
                "`datasets.IterableDatasetDict` instances. "
                f"Got {type(data)}"
            )
            raise DatasetError(msg)

        if isinstance(data, IterableDatasetDict):
            data = DatasetDict({k: Dataset.from_list(list(v)) for k, v in data.items()})
        elif isinstance(data, IterableDataset):
            data = Dataset.from_list(list(data))

        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        if isinstance(data, DatasetDict):
            self._save_dataset_dict(data, save_path)
        else:
            self._save_dataset(data, save_path)

        self._invalidate_cache()

    def _load_dataset(self, load_path: str) -> DatasetLike:
        if self._fs.isdir(load_path):
            ext = self.EXTENSION
            data_files = {
                PurePosixPath(p).stem: p for p in self._fs.glob(f"{load_path}/*{ext}")
            }
            # Note: nosec is fine here since we're always loading from a filesystem.
            #   Bandit throws an exception because it wants a revision number,
            #   which is only relevatn when loading from the Hub.
            return load_dataset(
                self.BUILDER, data_files=data_files, **self._load_args
            )  # nosec

        result = load_dataset(  # nosec
            self.BUILDER,
            data_files=load_path,
            storage_options=self._storage_options,
            **self._load_args,
        )

        # load_dataset wraps a single file in a DatasetDict with one
        # split (typically "train"). When the caller didn't ask for a
        # specific split, unwrap it so a single file round-trips as a
        # Dataset, not a DatasetDict.
        if (
            "split" not in self._load_args
            and isinstance(result, DatasetDict)
            and len(result) == 1
        ):
            return next(iter(result.values()))

        return result

    def _save_dataset(self, data: Dataset, save_path: str) -> None:
        saver = f"to_{self.BUILDER}"
        getattr(data, saver)(
            save_path,
            storage_options=self._storage_options,
            **self._save_args,
        )

    def _save_dataset_dict(self, data: DatasetDict, save_path: str) -> None:
        self._fs.mkdirs(save_path, exist_ok=True)
        ext = self.EXTENSION
        saver = f"to_{self.BUILDER}"
        for split, split_ds in data.items():
            split_path = f"{save_path}/{split}{ext}"
            getattr(split_ds, saver)(
                split_path,
                storage_options=self._storage_options,
                **self._save_args,
            )

    def _describe(self) -> dict[str, Any]:
        return {
            "path": self._filepath,
            "file_format": self.BUILDER,
            "protocol": self._protocol,
            "version": self._version,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False
        return self._fs.exists(load_path)

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        path = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(path)
