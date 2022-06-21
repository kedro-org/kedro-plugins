"""``JSONDataSet`` loads and saves graphs to a JSON file using an underlying
filesystem (e.g.: local, S3, GCS). ``NetworkX`` is used to create JSON data.
"""

import json
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, Dict

import fsspec
import networkx

from kedro.io.core import (
    AbstractVersionedDataSet,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)


class JSONDataSet(AbstractVersionedDataSet):
    """NetworkX ``JSONDataSet`` loads and saves graphs to a JSON file using an
    underlying filesystem (e.g.: local, S3, GCS). ``NetworkX`` is used to
    create JSON data.
    See https://networkx.org/documentation/stable/tutorial.html for details.

    Example:
    ::

        >>> from kedro_datasets.datasets.networkx import JSONDataSet
        >>> import networkx as nx
        >>> graph = nx.complete_graph(100)
        >>> graph_dataset = JSONDataSet(filepath="test.json")
        >>> graph_dataset.save(graph)
        >>> reloaded = graph_dataset.load()
        >>> assert nx.is_isomorphic(graph, reloaded)

    """

    DEFAULT_LOAD_ARGS = {}  # type: Dict[str, Any]
    DEFAULT_SAVE_ARGS = {}  # type: Dict[str, Any]

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        filepath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None,
        credentials: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``JSONDataSet``.

        Args:
            filepath: Filepath in POSIX format to the NetworkX graph JSON file.
            load_args: Arguments passed on to ``networkx.node_link_graph``.
                See the details in
                https://networkx.org/documentation/networkx-1.9.1/reference/generated/networkx.readwrite.json_graph.node_link_graph.html
            save_args: Arguments passed on to ``networkx.node_link_data``.
                See the details in
                https://networkx.org/documentation/networkx-1.9.1/reference/generated/networkx.readwrite.json_graph.node_link_data.html
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
                All defaults are preserved, except `mode`, which is set to `r` when loading
                and to `w` when saving.
        """
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

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

        _fs_open_args_save.setdefault("mode", "w")
        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

    def _load(self) -> networkx.Graph:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
            json_payload = json.load(fs_file)

        return networkx.node_link_graph(json_payload, **self._load_args)

    def _save(self, data: networkx.Graph) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        json_graph = networkx.node_link_data(data, **self._save_args)
        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            json.dump(json_graph, fs_file)

        self._invalidate_cache()

    def _exists(self) -> bool:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        return self._fs.exists(load_path)

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
            protocol=self._protocol,
            load_args=self._load_args,
            save_args=self._save_args,
            version=self._version,
        )

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
