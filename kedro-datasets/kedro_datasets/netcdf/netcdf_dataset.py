"""NetCDFDataset loads and saves data to a local netcdf (.nc) file."""
import logging
from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import Any, Dict

import fsspec
import xarray as xr
from kedro.io.core import (
    AbstractDataset,
    DataSetError,
    get_filepath_str,
    get_protocol_and_path,
)

log = logging.getLogger(__name__)


class NetCDFDataSet(AbstractDataset):
    """``NetCDFDataSet`` loads/saves data from/to a NetCDF file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses xarray to handle the NetCDF file.
    """

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {}

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        filepath: str,
        temppath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
        # credentials: Dict[str, Any] = None,
    ):
        """Creates a new instance of ``NetcdfDataSet`` pointing to a concrete NetCDF
        file on a specific filesystem

        Args:
            filepath: Filepath in POSIX format to a NetCDF file prefixed with a
                protocol like `s3://`. If prefix is not provided, `file` protocol
                (local filesystem) will be used. The prefix should be any protocol
                supported by ``fsspec``. It can also be a path to a glob. If a
                glob is provided then it can be used for reading multiple NetCDF
                files.
            temppath: Local temporary directory, used when reading from remote storage,
                since NetCDF files cannot be directly read from remote storage.
            load_args: Additional options for loading NetCDF file(s).
                Here you can find all available arguments when reading single file:
                https://xarray.pydata.org/en/stable/generated/xarray.open_dataset.html
                Here you can find all available arguments when reading multiple files:
                https://xarray.pydata.org/en/stable/generated/xarray.open_mfdataset.html
                All defaults are preserved.
            save_args: Additional saving options for saving NetCDF file(s).
                Here you can find all available arguments:
                https://xarray.pydata.org/en/stable/generated/xarray.Dataset.to_netcdf.html
                All defaults are preserved.
            fs_args: Extra arguments to pass into underlying filesystem class
                constructor (e.g. `{"cache_regions": "us-east-1"}` for
                ``s3fs.S3FileSystem``).
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.

        """
        self._fs_args = deepcopy(fs_args) or {}
        # self._credentials = deepcopy(credentials) or {}
        protocol, path = get_protocol_and_path(filepath)
        if protocol == "file":
            self._fs_args.setdefault("auto_mkdir", True)
        self._temppath = Path(temppath)
        self._protocol = protocol
        self._filepath = PurePosixPath(path)

        # self._storage_options = {**self._credentials, **self._fs_args}
        self._storage_options = {**self._fs_args}
        self._fs = fsspec.filesystem(self._protocol, **self._storage_options)

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

    def _load(self) -> xr.Dataset:
        load_path = get_filepath_str(self._filepath, self._protocol)

        # If NetCDF(s) are on any type of remote storage, need to sync to local to open.
        # It's assumed this would happen on a remote filesystem. Kerchunk could be
        # implemented here in the future for direct remote reading.
        if self._protocol != "file":
            log.info("Syncing remote to local storage.")
            # TODO: Figure out how to generalize this for different remote storage types
            load_path = "s3://" + load_path
            # TODO: Add recursive=True for multiple files.
            self._fs.get(load_path, str(self._temppath) + "/")
            load_path = f"{self._temppath}/{self._filepath.stem}.nc"

        if "*" in str(load_path):
            data = xr.open_mfdataset(str(load_path), **self._load_args)
        else:
            data = xr.open_dataset(load_path, **self._load_args)
        return data

    def _save(self, data: xr.Dataset):
        save_path = get_filepath_str(self._filepath, self._protocol)

        if Path(save_path).is_dir():
            raise DataSetError(
                f"Saving {self.__class__.__name__} as a directory is not supported."
            )

        bytes_buffer = data.to_netcdf(**self._save_args)

        with self._fs.open(save_path, mode="wb") as fs_file:
            fs_file.write(bytes_buffer)

        self._invalidate_cache()

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
            protocol=self._protocol,
            load_args=self._load_args,
            save_args=self._save_args,
        )

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._filepath, self._protocol)
        except DataSetError:
            return False

        return self._fs.exists(load_path)

    def _invalidate_cache(self):
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)

    def __del__(self):
        """Cleanup temporary directory"""
        self._temppath.unlink(missing_ok=True)
