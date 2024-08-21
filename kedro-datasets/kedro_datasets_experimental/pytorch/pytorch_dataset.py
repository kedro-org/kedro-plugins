from __future__ import annotations

from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any

import fsspec
import torch
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)


class PyTorchDataset(AbstractVersionedDataset[Any, Any]):
    """``PyTorchDataset`` loads and saves PyTorch models' `state_dict`
    using PyTorch's recommended zipfile serialization protocol. To avoid
    security issues with Pickle.

    .. code-block:: yaml

        model:
          type: pytorch.PyTorchDataset
          filepath: data/06_models/model.pt

    .. code-block:: pycon

        >>> from kedro_datasets_experimental.pytorch import PyTorchDataset
        >>> import torch
        >>>
        >>> model: torch.nn.Module
        >>> model = torch.nn.Sequential(torch.nn.Linear(10, 10), torch.nn.ReLU())
        >>> dataset = PyTorchDataset(filepath=tmp_path / "model.pt")
        >>> dataset.save(model)
        >>> reloaded = TheModelClass(*args, **kwargs)
        >>> reloaded.load_state_dict(dataset.load())
    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
            self,
            *,
            filepath,
            load_args: dict[str, Any] = None,
            save_args: dict[str, Any] = None,
            version: Version | None = None,
            credentials: dict[str, Any] = None,
            fs_args: dict[str, Any] = None,
            metadata: dict[str, Any] = None,
    ):
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

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }

    def _load(self) -> Any:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        return torch.load(load_path, **self._fs_open_args_load)

    def _save(self, data: torch.nn.Module) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        torch.save(data.state_dict(), save_path, **self._fs_open_args_save)

        self._invalidate_cache()

    def _exists(self):
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
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
