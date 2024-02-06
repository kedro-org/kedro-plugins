"""``ExcelDataset`` loads/saves data from/to a Excel file using an underlying
filesystem (e.g.: local, S3, GCS). It uses pandas to handle the Excel file.
"""
import logging
from copy import deepcopy
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any, Union

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
        Union[pd.DataFrame, dict[str, pd.DataFrame]],
        Union[pd.DataFrame, dict[str, pd.DataFrame]],
    ]
):
    """``ExcelDataset`` loads/saves data from/to a Excel file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses pandas to handle the Excel file.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

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

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.pandas import ExcelDataset
        >>> import pandas as pd
        >>>
        >>> data = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>>
        >>> dataset = ExcelDataset(filepath=tmp_path / "test.xlsx")
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert data.equals(reloaded)

    To save a multi-sheet Excel file, no special ``save_args`` are required.
    Instead, return a dictionary of ``Dict[str, pd.DataFrame]`` where the string
    keys are your sheet names.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_
    for a multi-sheet Excel file:

    .. code-block:: yaml

        trains:
          type: pandas.ExcelDataset
          filepath: data/02_intermediate/company/trains.xlsx
          load_args:
            sheet_name: [Sheet1, Sheet2, Sheet3]

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_
    for a multi-sheet Excel file:

    .. code-block:: pycon

        >>> from kedro_datasets.pandas import ExcelDataset
        >>> import pandas as pd
        >>>
        >>> dataframe = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>> another_dataframe = pd.DataFrame({"x": [10, 20], "y": ["hello", "world"]})
        >>> multiframe = {"Sheet1": dataframe, "Sheet2": another_dataframe}
        >>> dataset = ExcelDataset(filepath="test.xlsx", load_args={"sheet_name": None})
        >>> dataset.save(multiframe)
        >>> reloaded = dataset.load()
        >>> assert multiframe["Sheet1"].equals(reloaded["Sheet1"])
        >>> assert multiframe["Sheet2"].equals(reloaded["Sheet2"])

    """

    DEFAULT_LOAD_ARGS = {"engine": "openpyxl"}
    DEFAULT_SAVE_ARGS = {"index": False}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        engine: str = "openpyxl",
        load_args: dict[str, Any] = None,
        save_args: dict[str, Any] = None,
        version: Version = None,
        credentials: dict[str, Any] = None,
        fs_args: dict[str, Any] = None,
        metadata: dict[str, Any] = None,
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
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DatasetError: If versioning is enabled while in append mode.
        """
        _fs_args = deepcopy(fs_args) or {}
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

        # Handle default load arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)

        # Handle default save arguments
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)
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

    def _load(self) -> Union[pd.DataFrame, dict[str, pd.DataFrame]]:
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

    def _save(self, data: Union[pd.DataFrame, dict[str, pd.DataFrame]]) -> None:
        output = BytesIO()
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        with pd.ExcelWriter(output, **self._writer_args) as writer:
            if isinstance(data, dict):
                for sheet_name, sheet_data in data.items():
                    sheet_data.to_excel(
                        writer, sheet_name=sheet_name, **self._save_args
                    )
            else:
                data.to_excel(writer, **self._save_args)

        with self._fs.open(save_path, mode="wb") as fs_file:
            fs_file.write(output.getvalue())

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
        dataset_copy._load_args["nrows"] = nrows
        data = dataset_copy.load()

        return data.to_dict(orient="split")
