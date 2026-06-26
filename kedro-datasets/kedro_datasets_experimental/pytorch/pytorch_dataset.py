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
    """`PyTorchDataset` loads and saves PyTorch models' `state_dict` using ``torch.save``
    and ``torch.load``.

    .. warning::
        Loading is **not** safe for untrusted files. ``torch.load`` deserializes a
        pickle stream (the zipfile produced by ``torch.save`` is only a container
        around that pickle), so a maliciously crafted ``.pt`` file can execute
        arbitrary code on load. To mitigate this, ``PyTorchDataset`` enforces
        ``weights_only=True`` by default, which restricts loading to tensors and a
        small allow-list of safe types. Only set ``load_args: {weights_only: false}``
        for files you fully trust, and prefer ``torch>=2.6`` (where ``weights_only=True``
        is also the upstream default) or a non-pickle format such as ``safetensors``
        when handling untrusted inputs.

    ### Example usage for the [YAML API](https://kedro.readthedocs.io/en/stable/data/data_catalog_yaml_examples.html)

    ```yaml
    model:
        type: pytorch.PyTorchDataset
        filepath: data/06_models/model.pt
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)

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

    # Enforce safe deserialization by default. ``weights_only=True`` restricts
    # ``torch.load`` to tensors and a small allow-list of safe types, blocking the
    # arbitrary-code-execution path through pickle's ``__reduce__``. Users can opt
    # out for trusted files via ``load_args``.
    DEFAULT_LOAD_ARGS: dict[str, Any] = {"weights_only": True}
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
        # PyTorch serialization is binary; open in binary mode by default.
        _fs_open_args_load.setdefault("mode", "rb")
        _fs_open_args_save.setdefault("mode", "wb")
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
        with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
            # Suppression of B614 - weights_only defaults to True via DEFAULT_LOAD_ARGS,
            # restricting deserialisation to safe types. Users may override via
            # load_args only for files they fully trust.
            return torch.load(fs_file, **self._load_args)  # nosec: B614

    def save(self, data: torch.nn.Module) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            torch.save(data.state_dict(), fs_file, **self._save_args)

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
