"""``ImageDataset`` loads/saves image data as `numpy` from an underlying
filesystem (e.g.: local, S3, GCS). It uses Pillow to handle image file.
"""

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
from PIL import Image


class ImageDataset(AbstractVersionedDataset[Image.Image, Image.Image]):
    """``ImageDataset`` loads/saves image data as `numpy` from an underlying
    filesystem (e.g.: local, S3, GCS). It uses Pillow to handle image file.

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> import sys
        >>>
        >>> import pytest
        >>> from kedro_datasets.pillow import ImageDataset
        >>>
        >>> if sys.platform.startswith("win"):
        ...     pytest.skip("this doctest hangs on Windows CI runner")
        ...
        >>> dataset = ImageDataset(
        ...     filepath="https://storage.googleapis.com/gtv-videos-bucket/sample/images/ForBiggerBlazes.jpg"
        ... )
        >>> image = dataset.load()
        >>> image.show()

    """

    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

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
        """Creates a new instance of ``ImageDataset`` pointing to a concrete image file
        on a specific filesystem.

        Args:
            filepath: Filepath in POSIX format to an image file prefixed with a protocol like
                `s3://`. If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            save_args: Pillow options for saving image files.
                Here you can find all available arguments:
                https://pillow.readthedocs.io/en/stable/reference/Image.html#PIL.Image.Image.save
                All defaults are preserved.
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
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
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

        self.metadata = metadata

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        # Handle default save argument
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        _fs_open_args_save.setdefault("mode", "wb")
        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "save_args": self._save_args,
            "version": self._version,
        }

    def _load(self) -> Image.Image:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
            return Image.open(fs_file).copy()

    def _save(self, data: Image.Image) -> None:
        save_path = self._get_save_path()

        with self._fs.open(
            get_filepath_str(save_path, self._protocol), **self._fs_open_args_save
        ) as fs_file:
            data.save(fs_file, format=self._get_format(save_path), **self._save_args)

        self._invalidate_cache()

    @staticmethod
    def _get_format(file_path: PurePosixPath):
        ext = file_path.suffix.lower()
        if ext not in Image.EXTENSION:
            Image.init()
        return Image.EXTENSION.get(ext)

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
