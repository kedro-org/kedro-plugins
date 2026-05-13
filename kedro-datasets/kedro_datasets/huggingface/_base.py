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
        data_files: dict[str, str] | None = None,
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
            data_files: Mapping of split name to filename for loading and
                saving a ``DatasetDict`` from a directory
                (e.g. ``{"train": "train.csv"}``). The keys must match
                the split names of the ``DatasetDict`` being saved, and
                the filenames must use the correct extension for the
                format (e.g. ``.csv`` for ``CSVDataset``).
            load_args: Additional keyword arguments passed to the
                underlying load function. For backwards compatibility,
                this may include ``data_files`` if the top-level
                ``data_files`` argument is not used.
            save_args: Additional keyword arguments passed to the
                underlying save function. For backwards compatibility,
                this may include ``data_files`` if the top-level
                ``data_files`` argument is not used.
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

        self._load_args = deepcopy(load_args or {})
        self._save_args = deepcopy(save_args or {})
        self._load_data_files, self._save_data_files = self._resolve_data_files(
            data_files
        )
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
        if isinstance(data, IterableDataset | IterableDatasetDict):
            msg = (
                f"{type(self).__name__} got iterable dataset. "
                "Before saving an iterable dataset "
                "you must materialize it into a `Dataset` or `DatasetDict`."
            )
            raise DatasetError(msg)

        if not isinstance(data, Dataset | DatasetDict):
            msg = (
                f"{type(self).__name__} only supports `datasets.Dataset`, "
                "`datasets.DatasetDict`, "
                f"Got {type(data)}"
            )
            raise DatasetError(msg)

        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        if isinstance(data, DatasetDict):
            self._save_dataset_dict(data, save_path)
        else:
            self._save_dataset(data, save_path)

        self._invalidate_cache()

    def _build_data_files(self) -> str | dict[str, str]:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        if self._load_data_files:
            return {
                split: os.path.join(load_path, filename)
                for split, filename in self._load_data_files.items()
            }

        # Otherwise, just return the path to the Dataset to be loaded.
        return load_path

    def _load_dataset(self, load_path: str) -> DatasetLike:
        data_files: str | dict[str, str] = self._build_data_files()

        return load_dataset(  # nosec
            self.BUILDER,
            data_files=data_files,
            storage_options=self._storage_options,
            **self._load_args,
        )

    def _save_dataset(self, data: Dataset, save_path: str) -> None:
        saver = f"to_{self.BUILDER}"
        getattr(data, saver)(
            save_path,
            storage_options=self._storage_options,
            **self._save_args,
        )

    def _save_dataset_dict(self, data: DatasetDict, save_path: str) -> None:
        """Hugging Face only provides ``DatasetDict.save_to_disk`` for Arrow format.

        As a result, we have to call ``to_<format>`` per split.
        """
        if self._save_data_files:
            data_files_keys = set(self._save_data_files.keys())
            split_names = set(data.keys())
            if data_files_keys != split_names:
                msg = (
                    f"DatasetDict split names {sorted(split_names)} do not match "
                    f"``data_files`` keys {sorted(data_files_keys)}. "
                    "The data_files keys must match the DatasetDict split names "
                    "so the saved files can be found on load."
                )
                raise DatasetError(msg)
        self._fs.mkdirs(save_path, exist_ok=True)
        ext = self.EXTENSION
        for split, split_ds in data.items():
            filename = (
                self._save_data_files[split]
                if self._save_data_files
                else f"{split}{ext}"
            )
            split_path = f"{save_path}/{filename}"
            self._save_dataset(split_ds, split_path)

    def _resolve_data_files(
        self, data_files: dict[str, str] | None
    ) -> tuple[dict[str, str] | None, dict[str, str] | None]:
        if data_files is not None and (
            "data_files" in self._load_args or "data_files" in self._save_args
        ):
            msg = (
                f"{type(self).__name__} got ``data_files`` as a top-level "
                "argument and in ``load_args`` or ``save_args``. Pass it "
                "in only one place."
            )
            raise DatasetError(msg)

        load_data_files, save_data_files = None, None

        if data_files is not None:
            save_data_files = load_data_files = deepcopy(data_files)

        else:
            load_data_files = deepcopy(self._load_args.pop("data_files", None))
            save_data_files = deepcopy(self._save_args.pop("data_files", None))

        return load_data_files, save_data_files

    def _describe(self) -> dict[str, Any]:
        return {
            "path": self._filepath,
            "file_format": self.BUILDER,
            "protocol": self._protocol,
            "version": self._version,
            "data_files": self._load_data_files,
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
