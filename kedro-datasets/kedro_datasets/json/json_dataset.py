"""``JSONDataset`` loads/saves data from/to a JSON file using an underlying
filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file.
"""
from __future__ import annotations

import json
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

from kedro_datasets._typing import JSONPreview


class JSONDataset(AbstractVersionedDataset[Any, Any]):
    """``JSONDataset`` loads/saves data from/to a JSON file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        cars:
          type: json.JSONDataset
          filepath: gcs://your_bucket/cars.json
          fs_args:
            project: my-project
          credentials: my_gcp_credentials
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> from kedro_datasets.json import JSONDataset
        >>>
        >>> data = {"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]}
        >>>
        >>> dataset = JSONDataset(filepath=tmp_path / "test.json")
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert data == reloaded

    """

    DEFAULT_SAVE_ARGS: dict[str, Any] = {"indent": 2}
    DEFAULT_FS_ARGS: dict[str, Any] = {"open_args_save": {"mode": "w"}}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``JSONDataset`` pointing to a concrete JSON file
        on a specific filesystem.

        Args:
            filepath: Filepath in POSIX format to a JSON file prefixed with a protocol like `s3://`.
                If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            save_args: json options for saving JSON files (arguments passed
                into ```json.dump``). Here you can find all available arguments:
                https://docs.python.org/3/library/json.html
                All defaults are preserved, but "default_flow_style", which is set to False.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``), as well as
                to pass to the filesystem's `open` method through nested keys
                `open_args_load` and `open_args_save`.
                Here you can find all available arguments for `open`:
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.open
                All defaults are preserved, except `mode`, which is set to `w` when saving.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})

        _credentials = deepcopy(credentials) or {}
        protocol, path = get_protocol_and_path(filepath, version)

        self._protocol = protocol
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

        self.metadata = metadata

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        # Handle default save and fs arguments
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}
        self._fs_open_args_load = {
            **self.DEFAULT_FS_ARGS.get("open_args_load", {}),
            **(_fs_open_args_load or {}),
        }
        self._fs_open_args_save = {
            **self.DEFAULT_FS_ARGS.get("open_args_save", {}),
            **(_fs_open_args_save or {}),
        }

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "save_args": self._save_args,
            "version": self._version,
        }

    def load(self) -> Any:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
            return json.load(fs_file)

    def save(self, data: Any) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            json.dump(data, fs_file, **self._save_args)

        self._invalidate_cache()

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
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)

    def preview(self) -> JSONPreview:
        """
        Generate a preview of the JSON dataset with a specified number of items.

        Returns:
            A string representing the JSON data for previewing.
        """
        data = self.load.__wrapped__(self)  # type: ignore[attr-defined]

        return JSONPreview(json.dumps(data))
