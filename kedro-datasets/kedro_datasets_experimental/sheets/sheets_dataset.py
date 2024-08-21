"""Google sheets dataset loads and saves data to a Google Sheet.
"""
import pandas as pd

from typing import Any, Optional
from copy import deepcopy

from kedro.io.core import Version
from kedro.io.core import (
    PROTOCOL_DELIMITER,
    AbstractVersionedDataset,
    DatasetError,
    Version
)

import pygsheets
from pygsheets import Worksheet, Spreadsheet


class GoogleSheetsDataset(AbstractVersionedDataset[pd.DataFrame, pd.DataFrame]):
    """Dataset to load data from Google sheets.

    Reading and writing of specific columns is suported to enable iterative engineering/translator interaction. Currently,
    authentication is done through a GCP service account, added to a GCP project with the sheets and drive APIs enabled. The
    email of the service account should be added as an editor to the Sheet.

    Example usage for the
    `YAML API <https://docs.kedro.org/en/stable/data/\
    data_catalog_yaml_examples.html>`_:


    .. code-block:: yaml

        sheet:
          type: sheets.GoogleSheetsDataset
          key: <add key of sheet here>
          service_file:  conf/local/service-account.json

          # NOTE: Columns being written should exist in the sheet.
          save_args:
            sheet_name: data
            write_columns:  ["list", "of", "cols", "to", "write", "here"]

          load_args:
            sheet_name: data
            columns: ["list", "of", "cols", "to", "read", "here"]
    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        key: str,
        service_file: str,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``GoogleSheetsDataset``.

        Args:
            key: Google sheets key
            service_file: path to service accunt file.
            load_args: Arguments to pass to the load method.
            save_args: Arguments to pass to the save
            version: Version of the dataset.
            credentials: Credentials to connect to the Neo4J instance.
            metadata: Metadata to pass to neo4j connector.
            kwargs: Keyword Args passed to parent.
        """
        self._key = key
        self._service_file = service_file
        self._sheet = None

        super().__init__(
            filepath=None,
            version=version,
            exists_function=self._exists,
            glob_function=None,
        )

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

    def _init_sheet(self):
        """Function to initialize the spreadsheet.

        This is executed lazily to avoid loading credentials on python runtime launch which creates issues
        in unit tests.
        """
        if self._sheet is None:
            gc = pygsheets.authorize(service_file=self._service_file)
            self._sheet = gc.open_by_key(self._key)

    def _load(self) -> pd.DataFrame:
        self._init_sheet()

        sheet_name = self._load_args["sheet_name"]
        wks = self._get_wks_by_name(self._sheet, sheet_name)
        if wks is None:
            raise DatasetError(f"Sheet with name {sheet_name} not found!")

        df = wks.get_as_df()
        if (cols := self._load_args.get("columns", None)) is not None:
            df = df[cols]

        return df

    def _save(self, data: pd.DataFrame) -> None:
        self._init_sheet()

        sheet_name = self._save_args["sheet_name"]
        wks = self._get_wks_by_name(self._sheet, sheet_name)

        # Create the worksheet if not exists
        if wks is None:
            wks = self._sheet.add_worksheet(sheet_name)

        # Write columns
        for column in self._save_args["write_columns"]:
            col_idx = self._get_col_index(wks, column)

            if col_idx is None:
                raise DatasetError(
                    f"Sheet with {sheet_name} does not contain column {column}!"
                )

            wks.set_dataframe(data[[column]], (1, col_idx + 1))

    @staticmethod
    def _get_wks_by_name(
        spreadsheet: Spreadsheet, sheet_name: str
    ) -> Optional[Worksheet]:
        for wks in spreadsheet.worksheets():
            if wks.title == sheet_name:
                return wks

        return None

    @staticmethod
    def _get_col_index(sheet: Worksheet, col_name: str) -> Optional[int]:
        for idx, col in enumerate(sheet.get_row(1)):
            if col == col_name:
                return idx

        return None

    def _describe(self) -> dict[str, Any]:
        return {
            "key": self._key,
        }

    def _exists(self) -> bool:
        return False
