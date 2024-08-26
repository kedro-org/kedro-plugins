"""``BaseTableDataset`` implementation used to add the base for
``ManagedTableDataset`` and ``ExternalTableDataset``.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    VersionNotFoundError,
)
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException, ParseException

from kedro_datasets.spark.spark_dataset import _get_spark

logger = logging.getLogger(__name__)
pd.DataFrame.iteritems = pd.DataFrame.items


@dataclass(frozen=True)
class BaseTable:
    """Stores the definition of a base table.
    
    Acts as a base class for `ManagedTable` and `ExternalTable`.
    """

    # regex for tables, catalogs and schemas
    _NAMING_REGEX = r"\b[0-9a-zA-Z_-]{1,}\b"
    _VALID_WRITE_MODES = ["overwrite", "append"]
    _VALID_DATAFRAME_TYPES = ["spark", "pandas"]
    _VALID_FORMATS = ["delta", "parquet", "csv"]
    format: str
    database: str
    catalog: str | None
    table: str
    write_mode: str | None
    dataframe_type: str
    primary_key: str | list[str] | None
    owner_group: str | None
    partition_columns: str | list[str] | None
    json_schema: dict[str, Any] | None = None

    def __post_init__(self):
        """Run validation methods if declared.

        The validation method can be a simple check
        that raises DatasetError.

        The validation is performed by calling a function with the signature
        `validate_<field_name>(self, value) -> raises DatasetError`.
        """
        for name in self.__dataclass_fields__.keys():
            method = getattr(self, f"_validate_{name}", None)
            if method:
                method()

    def _validate_format(self):
        """Validates the format of the table.

        Raises:
            DatasetError: If an invalid `format` is passed.
        """
        if self.format not in self._VALID_FORMATS:
            valid_formats = ", ".join(self._VALID_FORMATS)
            raise DatasetError(
                f"Invalid `format` provided: {self.format}. "
                f"`format` must be one of: {valid_formats}"
            )

    def _validate_table(self):
        """Validates table name.

        Raises:
            DatasetError: If the table name does not conform to naming constraints.
        """
        if not re.fullmatch(self._NAMING_REGEX, self.table):
            raise DatasetError("table does not conform to naming")

    def _validate_database(self):
        """Validates database name.

        Raises:
            DatasetError: If the dataset name does not conform to naming constraints.
        """
        if not re.fullmatch(self._NAMING_REGEX, self.database):
            raise DatasetError("database does not conform to naming")

    def _validate_catalog(self):
        """Validates catalog name.

        Raises:
            DatasetError: If the catalog name does not conform to naming constraints.
        """
        if self.catalog:
            if not re.fullmatch(self._NAMING_REGEX, self.catalog):
                raise DatasetError("catalog does not conform to naming")

    def _validate_write_mode(self):
        """Validates the write mode.

        Raises:
            DatasetError: If an invalid `write_mode` is passed.
        """
        if (
            self.write_mode is not None
            and self.write_mode not in self._VALID_WRITE_MODES
        ):
            valid_modes = ", ".join(self._VALID_WRITE_MODES)
            raise DatasetError(
                f"Invalid `write_mode` provided: {self.write_mode}. "
                f"`write_mode` must be one of: {valid_modes}"
            )

    def _validate_dataframe_type(self):
        """Validates the dataframe type.

        Raises:
            DatasetError: If an invalid `dataframe_type` is passed
        """
        if self.dataframe_type not in self._VALID_DATAFRAME_TYPES:
            valid_types = ", ".join(self._VALID_DATAFRAME_TYPES)
            raise DatasetError(f"`dataframe_type` must be one of {valid_types}")

    def _validate_primary_key(self):
        """Validates the primary key of the table.

        Raises:
            DatasetError: If no `primary_key` is specified.
        """
        if self.primary_key is None or len(self.primary_key) == 0:
            if self.write_mode == "upsert":
                raise DatasetError(
                    f"`primary_key` must be provided for"
                    f"`write_mode` {self.write_mode}"
                )

    def full_table_location(self) -> str | None:
        """Returns the full table location.

        Returns:
            str | None : table location in the format catalog.database.table or None if database and table aren't defined
        """
        full_table_location = None
        if self.catalog and self.database and self.table:
            full_table_location = f"`{self.catalog}`.`{self.database}`.`{self.table}`"
        elif self.database and self.table:
            full_table_location = f"`{self.database}`.`{self.table}`"
        return full_table_location

    def schema(self) -> StructType | None:
        """Returns the Spark schema of the table if it exists.

        Returns:
            StructType:
        """
        schema = None
        try:
            if self.json_schema is not None:
                schema = StructType.fromJson(self.json_schema)
        except (KeyError, ValueError) as exc:
            raise DatasetError(exc) from exc
        return schema