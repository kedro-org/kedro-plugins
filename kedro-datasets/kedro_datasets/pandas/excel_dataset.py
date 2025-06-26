"""``ExcelDataset`` loads/saves data from/to a Excel file using an underlying
filesystem (e.g.: local, S3, GCS). It uses pandas to handle the Excel file.
"""
from __future__ import annotations

import logging
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any

import fsspec
import pandas as pd
from kedro.io.core import (
    PROTOCOL_DELIMITER,
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

from kedro_datasets._typing import TablePreview

logger = logging.getLogger(__name__)


class ExcelDataset(
    AbstractVersionedDataset[
        pd.DataFrame | dict[str, pd.DataFrame],
        pd.DataFrame | dict[str, pd.DataFrame],
    ]
):
    """``ExcelDataset`` loads/saves data from/to a Excel file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses pandas to handle the Excel file.

    To save a multi-sheet Excel file, no special ``save_args`` are required.
    Instead, return a dictionary of ``Dict[str, pd.DataFrame]`` where the string
    keys are your sheet names.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        rockets:
          type: pandas.ExcelDataset
          filepath: gcs://your_bucket/rockets.xlsx
          fs_args:
            project: my-project
          credentials: my_gcp_credentials
          save_args:
            sheet_name: Sheet1
          load_args:
            sheet_name: Sheet1

        shuttles:
          type: pandas.ExcelDataset
          filepath: data/01_raw/shuttles.xlsx
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> import pandas as pd
        >>> from kedro_datasets.pandas import ExcelDataset
        >>>
        >>> data = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>>
        >>> dataset = ExcelDataset(filepath=tmp_path / "test.xlsx")
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert data.equals(reloaded)

        For a multi-sheet Excel file, using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        trains:
          type: pandas.ExcelDataset
          filepath: data/02_intermediate/company/trains.xlsx
          load_args:
            sheet_name: [Sheet1, Sheet2, Sheet3]
        ```

        For a multi-sheet Excel file, using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> import pandas as pd
        >>> from kedro_datasets.pandas import ExcelDataset
        >>>
        >>> dataframe = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>> another_dataframe = pd.DataFrame({"x": [10, 20], "y": ["hello", "world"]})
        >>> multiframe = {"Sheet1": dataframe, "Sheet2": another_dataframe}
        >>>
        >>> dataset = ExcelDataset(filepath="test.xlsx", load_args={"sheet_name": None})
        >>> dataset.save(multiframe)
        >>> reloaded = dataset.load()
        >>> assert multiframe["Sheet1"].equals(reloaded["Sheet1"])
        >>> assert multiframe["Sheet2"].equals(reloaded["Sheet2"])
    """

    DEFAULT_LOAD_ARGS = {"engine": "openpyxl"}
    DEFAULT_SAVE_ARGS = {"index": False}
    DEFAULT_FS_ARGS: dict[str, Any] = {"open_args_save": {"mode": "wb"}}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        engine: str = "openpyxl",
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``ExcelDataset`` pointing to a concrete Excel file
        on a specific filesystem.

        Args:
            filepath: Filepath in POSIX format to a Excel file prefixed with a protocol like
                `s3://`. If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            engine: The engine used to write to Excel files. The default
                engine is 'openpyxl'.
            load_args: Pandas options for loading Excel files.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_excel.html
                All defaults are preserved, but "engine", which is set to "openpyxl".
                Supports multi-sheet Excel files (include `sheet_name = None` in `load_args`).
            save_args: Pandas options for saving Excel files.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_excel.html
                All defaults are preserved, but "index", which is set to False.
                If you would like to specify options for the `ExcelWriter`,
                you can include them under the "writer" key. Here you can
                find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.ExcelWriter.html
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``).
                Defaults are preserved, apart from the `open_args_save` `mode` which is set to `wb`.
                Note that the save method requires bytes, so any save mode provided should include "b" for bytes.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DatasetError: If versioning is enabled while in append mode.
        """
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._storage_options = {**_credentials, **_fs_args}
        self._fs = fsspec.filesystem(self._protocol, **self._storage_options)

        self.metadata = metadata

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        # Handle default load and save and fs arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}
        self._fs_open_args_load = {
            **self.DEFAULT_FS_ARGS.get("open_args_load", {}),
            **(_fs_open_args_load or {}),
        }
        self._fs_open_args_save = {
            **self.DEFAULT_FS_ARGS.get("open_args_save", {}),
            **(_fs_open_args_save or {}),
        }

        self._writer_args = self._save_args.pop("writer", {})  # type: ignore
        self._writer_args.setdefault("engine", engine or "openpyxl")  # type: ignore

        if version and self._writer_args.get("mode") == "a":  # type: ignore
            raise DatasetError(
                "'ExcelDataset' doesn't support versioning in append mode."
            )

        if "storage_options" in self._save_args or "storage_options" in self._load_args:
            logger.warning(
                "Dropping 'storage_options' for %s, "
                "please specify them under 'fs_args' or 'credentials'.",
                self._filepath,
            )
            self._save_args.pop("storage_options", None)
            self._load_args.pop("storage_options", None)

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "writer_args": self._writer_args,
            "version": self._version,
        }

    def load(self) -> pd.DataFrame | dict[str, pd.DataFrame]:
        load_path = str(self._get_load_path())
        if self._protocol == "file":
            # file:// protocol seems to misbehave on Windows
            # (<urlopen error file not on local host>),
            # so we don't join that back to the filepath;
            # storage_options also don't work with local paths
            return pd.read_excel(load_path, **self._load_args)

        load_path = f"{self._protocol}{PROTOCOL_DELIMITER}{load_path}"
        return pd.read_excel(
            load_path, storage_options=self._storage_options, **self._load_args
        )

    def save(self, data: pd.DataFrame | dict[str, pd.DataFrame]) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            with pd.ExcelWriter(fs_file, **self._writer_args) as writer:
                if isinstance(data, dict):
                    for sheet_name, sheet_data in data.items():
                        sheet_data.to_excel(
                            writer, sheet_name=sheet_name, **self._save_args
                        )
                else:
                    data.to_excel(writer, **self._save_args)

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

    def preview(self, nrows: int = 5) -> TablePreview:
        """
        Generate a preview of the dataset with a specified number of rows.

        Args:
            nrows: The number of rows to include in the preview. Defaults to 5.

        Returns:
            dict: A dictionary containing the data in a split format.
        """
        # Create a copy so it doesn't contaminate the original dataset
        dataset_copy = self._copy()
        dataset_copy._load_args["nrows"] = nrows  # type: ignore[attr-defined]
        data = dataset_copy.load()

        return data.to_dict(orient="split")
