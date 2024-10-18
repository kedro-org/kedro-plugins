from __future__ import annotations

import importlib
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any

import fsspec
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)


class SafetensorsDataset(AbstractVersionedDataset[Any, Any]):

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}
    DEFAULT_FS_ARGS: dict[str, Any] = {"open_args_save": {"mode": "wb"}}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        backend: str = "torch",
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        try:
            importlib.import_module(f"safetensors.{backend}")
        except ImportError as exc:
            raise ImportError(
                f"Selected backend '{backend}' could not be imported. "
                "Make sure it is installed and importable."
            ) from exc
        
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

        self.metadata = metadata

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        self._backend = backend

        self._fs_open_args_load = {
            **self.DEFAULT_FS_ARGS.get("open_args_load", {}),
            **(_fs_open_args_load or {}),
        }
        self._fs_open_args_save = {
            **self.DEFAULT_FS_ARGS.get("open_args_save", {}),
            **(_fs_open_args_save or {}),
        }

    def load(self) -> Any:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
            imported_backend = importlib.import_module(f"safetensors.{self._backend}")
            return imported_backend.load(fs_file.read())

    def save(self, data: Any) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            try:
                imported_backend = importlib.import_module(f"safetensors.{self._backend}")
                imported_backend.save_file(data, fs_file.name)
            except Exception as exc:
                raise DatasetError(
                    f"{data.__class__} was not serialised due to: {exc}"
                ) from exc
            
        self._invalidate_cache()

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "backend": self._backend,
            "protocol": self._protocol,
            "version": self._version,
        }

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False

        return self._fs.exists(load_path)

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)