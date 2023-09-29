"""NetCDFDataset loads and saves data to a local netcdf (.nc) file."""
from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import Any, Dict

import fsspec
import xarray as xr
from kedro.io.core import (
    AbstractDataSet,
    DataSetError,
    get_filepath_str,
    get_protocol_and_path,
)


class NetCDFDataSet(AbstractDataSet):
    """``NetCDFDataSet`` loads/saves data from/to a NetCDF file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses xarray to handle the NetCDF file.
    """

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {}

    def __init__(
        self,
        filepath: str,
        temppath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
    ) -> None:
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

        """
        self._fs_args = deepcopy(fs_args) or {}

        protocol, path = get_protocol_and_path(filepath)
        if protocol == "file":
            self._fs_args.setdefault("auto_mkdir", True)
        self._temppath = Path(temppath) / Path(path).parent

        self._protocol = protocol
        self._storage_options = {**self._fs_args}
        self._fs = fsspec.filesystem(self._protocol, **self._storage_options)
        self._filepath = PurePosixPath(path)

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

    def _load(self) -> xr.Dataset:
        load_path = get_filepath_str(self._filepath, self._protocol)
        if "*" in str(load_path):
            data = xr.open_mfdataset(str(load_path), **self._load_args)
        else:
            data = xr.open_dataset(load_path, **self._load_args)
        return data

    def _save(self, data: xr.Dataset) -> None:
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

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
