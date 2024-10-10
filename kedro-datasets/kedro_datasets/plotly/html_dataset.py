"""``HTMLDataset`` saves a plotly figure to an HTML file using an underlying
filesystem (e.g.: local, S3, GCS).
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, NoReturn

import fsspec
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)
from plotly import graph_objects as go


class HTMLDataset(AbstractVersionedDataset[go.Figure, go.Figure | go.FigureWidget]):
    """``HTMLDataset`` saves a plotly figure to an HTML file using an
    underlying filesystem (e.g.: local, S3, GCS).

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        scatter_plot:
          type: plotly.HTMLDataset
          filepath: data/08_reporting/scatter_plot.html
          save_args:
            auto_open: False

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.plotly import HTMLDataset
        >>> import plotly.express as px
        >>>
        >>> fig = px.bar(x=["a", "b", "c"], y=[1, 3, 2])
        >>> dataset = HTMLDataset(filepath=tmp_path / "test.html")
        >>> dataset.save(fig)
    """

    DEFAULT_SAVE_ARGS: dict[str, Any] = {}
    DEFAULT_FS_ARGS: dict[str, Any] = {
        "open_args_save": {"mode": "w", "encoding": "utf-8"}
    }

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
        """Creates a new instance of ``HTMLDataset`` pointing to a concrete HTML file
        on a specific filesystem.

        Args:
            filepath: Filepath in POSIX format to an HTML file prefixed with a protocol like `s3://`.
                If prefix is not provided `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            save_args: Plotly options for saving HTML files.
                Here you can find all available arguments:
                https://plotly.com/python-api-reference/generated/plotly.io.write_html.html#plotly.io.write_html
                All defaults are preserved.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{'token': None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``), as well as
                to pass to the filesystem's `open` method through nested keys
                `open_args_load` and `open_args_save`.
                Here you can find all available arguments for `open`:
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.open
                All defaults are preserved, except `mode`, which is set to `w` when
                saving.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        _fs_args = deepcopy(fs_args) or {}
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

        # Handle default save and fs arguments
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}
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

    def load(self) -> NoReturn:
        raise DatasetError(f"Loading not supported for '{self.__class__.__name__}'")

    def save(self, data: go.Figure) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            data.write_html(fs_file, **self._save_args)

        self._invalidate_cache()

    def _exists(self) -> bool:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        return self._fs.exists(load_path)

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
