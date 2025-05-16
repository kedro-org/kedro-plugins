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
    """`PyTorchDataset` loads and saves PyTorch models' `state_dict` using PyTorch's recommended zipfile serialization protocol to avoid security issues with Pickle.

    ### Example usage for the [YAML API](https://kedro.readthedocs.io/en/stable/data/data_catalog_yaml_examples.html)

    ```yaml
    model:
        type: pytorch.PyTorchDataset
        filepath: data/06_models/model.pt
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html)

    ```python
    from kedro_datasets_experimental.pytorch import PyTorchDataset
    import torch

    # Define your model
    model: torch.nn.Module
    model = torch.nn.Sequential(torch.nn.Linear(10, 10), torch.nn.ReLU())

    # Save model state dict
    dataset = PyTorchDataset(filepath="data/06_models/model.pt")
    dataset.save(model)

    # Reload model state dict
    reloaded = TheModelClass(*args, **kwargs)
    reloaded.load_state_dict(dataset.load())
    ```

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

    def load(self) -> Any:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        return torch.load(load_path, **self._fs_open_args_load)  #nosec: B614

    def save(self, data: torch.nn.Module) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        torch.save(data.state_dict(), save_path, **self._fs_open_args_save)  #nosec: B614

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
