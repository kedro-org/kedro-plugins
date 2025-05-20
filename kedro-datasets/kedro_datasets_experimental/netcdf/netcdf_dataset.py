"""NetCDFDataset loads and saves data to a local netcdf (.nc) file."""

from __future__ import annotations

import logging
from copy import deepcopy
from glob import glob
from pathlib import Path, PurePosixPath
from typing import Any

import fsspec
import xarray as xr
from kedro.io.core import (
    AbstractDataset,
    DatasetError,
    get_protocol_and_path,
)

logger = logging.getLogger(__name__)


class NetCDFDataset(AbstractDataset):
    """`NetCDFDataset` loads and saves data to a local netcdf (.nc) file.

    ### Example usage for the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):
    ```yaml
    single-file:
        type: netcdf.NetCDFDataset
        filepath: s3://bucket_name/path/to/folder/data.nc
        save_args:
            mode: a
        load_args:
            decode_times: False

    multi-file:
        type: netcdf.NetCDFDataset
        filepath: s3://bucket_name/path/to/folder/data*.nc
        load_args:
            concat_dim: time
            combine: nested
            parallel: True
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):
    ```python
    from kedro_datasets.netcdf import NetCDFDataset
    import xarray as xr
    ds = xr.DataArray(
         [0, 1, 2], dims=["x"], coords={"x": [0, 1, 2]}, name="data"
     ).to_dataset()
    dataset = NetCDFDataset(
         filepath=tmp_path / "data.nc",
         save_args={"mode": "w"},
    )
    dataset.save(ds)
    reloaded = dataset.load()
    assert ds.equals(reloaded)
    ```
    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa
        self,
        *,
        filepath: str,
        temppath: str | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """Creates a new instance of ``NetCDFDataset`` pointing to a concrete NetCDF
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
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        self._fs_args = deepcopy(fs_args) or {}
        self._credentials = deepcopy(credentials) or {}
        self._temppath = Path(temppath) if temppath is not None else None
        protocol, path = get_protocol_and_path(filepath)
        if protocol == "file":
            self._fs_args.setdefault("auto_mkdir", True)
        elif protocol != "file" and self._temppath is None:
            raise ValueError(
                "Need to set temppath in catalog if NetCDF file exists on remote "
                + "filesystem"
            )
        self._protocol = protocol
        self._filepath = filepath

        self._storage_options = {**self._credentials, **self._fs_args}
        self._fs = fsspec.filesystem(self._protocol, **self._storage_options)

        self.metadata = metadata

        # Handle default load and save arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}

        # Determine if multiple NetCDF files are being loaded in.
        self._is_multifile = (
            True if "*" in str(PurePosixPath(self._filepath).stem) else False
        )

    def load(self) -> xr.Dataset:
        load_path = self._filepath
        multi_load_path = load_path

        # If NetCDF(s) are on any type of remote storage, need to sync to local to open.
        # Kerchunk could be implemented here in the future for direct remote reading.
        if self._protocol != "file":
            logger.info("Syncing remote NetCDF file to local storage.")

            if self._is_multifile:
                multi_load_path = sorted(self._fs.glob(load_path))  # type: ignore[assignment]

            self._fs.get(load_path, f"{self._temppath}/")
            load_path = f"{self._temppath}/{str(Path(self._filepath).stem)}.nc"

        if self._is_multifile and multi_load_path:
            data = xr.open_mfdataset(multi_load_path, **self._load_args)
        else:
            data = xr.open_dataset(load_path, **self._load_args)

        return data

    def save(self, data: xr.Dataset):
        if self._is_multifile:
            raise DatasetError(
                "Globbed multifile datasets with '*' in filepath cannot be saved. "
                + "Create an alternate NetCDFDataset with a single .nc output file."
            )
        else:
            if self._protocol == "file":
                data.to_netcdf(path=self._filepath, **self._save_args)
            else:
                if self._temppath is None:
                    raise DatasetError("_temppath should have been set in __init__")
                temp_save_path = self._temppath / PurePosixPath(self._filepath).name
                data.to_netcdf(path=str(temp_save_path), **self._save_args)
                # Sync to remote storage
                self._fs.put_file(str(temp_save_path), self._filepath)

            self._invalidate_cache()

    def _describe(self) -> dict[str, Any]:
        return dict(
            filepath=self._filepath,
            protocol=self._protocol,
            load_args=self._load_args,
            save_args=self._save_args,
        )

    def _exists(self) -> bool:
        load_path = self._filepath

        if self._is_multifile:
            files = self._fs.glob(load_path)
            exists = True if files else False
        else:
            exists = self._fs.exists(load_path)

        return exists

    def _invalidate_cache(self):
        """Invalidate underlying filesystem caches."""
        self._fs.invalidate_cache(self._filepath)

    def __del__(self):
        """Cleanup temporary directory"""
        if self._temppath is not None:
            logger.info("Deleting local temporary files.")
            temp_filepath = self._temppath / PurePosixPath(self._filepath).stem
            if self._is_multifile:
                temp_files = glob(str(temp_filepath))
                for file in temp_files:
                    try:
                        Path(file).unlink()
                    except FileNotFoundError:  # pragma: no cover
                        pass  # pragma: no cover
            else:
                temp_filepath = (
                    str(temp_filepath) + "/" + PurePosixPath(self._filepath).name
                )
                try:
                    Path(temp_filepath).unlink()
                except FileNotFoundError:
                    pass
