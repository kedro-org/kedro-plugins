"""``GenericDataSet`` loads/saves data from/to a data file using an underlying
filesystem (e.g.: local, S3, GCS). It uses polars to handle the
type of read/write target.
"""
from copy import deepcopy
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any, Dict

import fsspec
import polars as pl
from kedro.io.core import (
    AbstractVersionedDataSet,
    DataSetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

ACCEPTED_WRITE_FILE_FORMATS = [
    "csv",
    "ipc",
    "parquet",
    "json",
    "ndjson",
    "avro",
]
# always a superset of ACCEPTED_WRITE_FILE_FORMATS
ACCEPTED_READ_FILE_FORMATS = ACCEPTED_WRITE_FILE_FORMATS + [
    "excel",
    "delta",
]


# pylint: disable=too-many-instance-attributes
class GenericDataSet(AbstractVersionedDataSet[pl.DataFrame, pl.DataFrame]):
    """`polars.GenericDataSet` loads/saves data from/to a data file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses polars to dynamically select the
    appropriate type of read/write target on a best effort basis.
    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:
    .. code-block:: yaml
        cars:
          type: polars.GenericDataSet
          file_format: parquet
          filepath: s3://data/01_raw/company/cars.parquet
          load_args:
            low_memory: True
          save_args:
            compression: "snappy"

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-code-api>`_:
    ::
        >>> from kedro_datasets.polars import GenericDataSet
        >>> import polars as pl
        >>>
        >>> data = pl.DataFrame({'col1': [1, 2], 'col2': [4, 5],
        >>>                      'col3': [5, 6]})
        >>>
        >>> data_set = GenericDataSet(filepath="test.parquet", file_format='parquet')
        >>> data_set.save(data)
        >>> reloaded = data_set.load()
        >>> assert data.frame_equal(reloaded)
    """

    DEFAULT_LOAD_ARGS = {}  # type: Dict[str, Any]
    DEFAULT_SAVE_ARGS = {}  # type: Dict[str, Any]

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        filepath: str,
        file_format: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None,
        credentials: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
    ):
        """Creates a new instance of ``GenericDataSet`` pointing to a concrete data file
        on a specific filesystem. The appropriate polars load/save methods are
        dynamically identified by string matching on a best effort basis.
        Args:
            filepath: Filepath in POSIX format to a file prefixed with a protocol like
                `s3://`.
                If prefix is not provided, `file` protocol (local filesystem)
                will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Key assumption: The first argument of either load/save method points to
                a filepath/buffer/io type location. There are some read/write targets
                such as 'clipboard' or 'records' that will fail since they do not take a
                filepath like argument.
            file_format: String which is used to match the appropriate load/save method
                on a best effort basis. For example if 'csv' is passed the
                `polars.read_csv` and
                `polars.DataFrame.write_csv` methods will be identified. An error will
                be raised unless
                at least one matching `read_{file_format}` or `write_{file_format}`.
            load_args: polars options for loading files.
                Here you can find all available arguments:
                https://pola-rs.github.io/polars/py-polars/html/reference/io.html
                All defaults are preserved.
            save_args: Polars options for saving files.
                Here you can find all available arguments:
                https://pola-rs.github.io/polars/py-polars/html/reference/io.html
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
        Raises:
            DataSetError: Will be raised if at least less than one appropriate
                read or write methods are identified.
        """

        self._file_format = file_format.lower()

        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath)
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

        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        _fs_open_args_save.setdefault("mode", "wb")
        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

    def _load(self) -> pl.DataFrame:  # pylint: disable= inconsistent-return-statements
        if self._file_format not in ACCEPTED_READ_FILE_FORMATS:
            raise DataSetError(
                f"Unable to retrieve 'polars.read_{self._file_format}' method, please"
                " ensure that your "
                "'file_format' parameter has been defined correctly as per the Polars"
                " API"
                " https://pola-rs.github.io/polars/py-polars/html/reference/io.html"
            )

        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        load_method = getattr(pl, f"read_{self._file_format}", None)
        if load_method:
            with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
                return load_method(fs_file, **self._load_args)

    def _save(self, data: pl.DataFrame) -> None:
        if (
            self._file_format not in ACCEPTED_WRITE_FILE_FORMATS
        ):
            raise DataSetError(
                f"Unable to retrieve 'polars.DataFrame.write_{self._file_format}' "
                "method, please "
                "ensure that your 'file_format' parameter has been defined correctly as"
                " per the Polars API "
                "https://pola-rs.github.io/polars/py-polars/html/reference/io.html"
            )

        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        save_method = getattr(data, f"write_{self._file_format}", None)

        if save_method:
            buf = BytesIO()
            save_method(file=buf, **self._save_args)
            with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
                fs_file.write(buf.getvalue())
                self._invalidate_cache()

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DataSetError:
            return False

        return self._fs.exists(load_path)

    def _describe(self) -> Dict[str, Any]:
        return {
            "file_format": self._file_format,
            "filepath": self._filepath,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
