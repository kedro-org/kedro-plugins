"""``ExternalTableDataset`` implementation to access external tables
in Databricks.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd
import pandas as pd
from kedro.io.core import (
    DatasetError
)

from kedro_datasets.databricks._base_table_dataset import BaseTable, BaseTableDataset

logger = logging.getLogger(__name__)
pd.DataFrame.iteritems = pd.DataFrame.items


@dataclass(frozen=True, kw_only=True)
class ExternalTable(BaseTable):
    """Stores the definition of an external table."""

    def _validate_existence_of_table(self) -> None:
        """Validates that a location is provided if the table does not exist.
        
        Raises:
            DatasetError: If the table does not exist and no location is provided.
        """
        if not self.exists() and not self.location:
            raise DatasetError(
                "If the external table does not exists, the `location` parameter must be provided. "
                "This should be valid path in an external location that has already been created."
            )
        
    def _validate_write_mode_for_format(self) -> None:
        """Validates that the write mode is compatible with the format.
        
        Raises:
            DatasetError: If the write mode is not compatible with the format.
        """
        if self.write_mode == "upsert" and self.format != "delta":
            raise DatasetError(
                f"Format '{self.format}' is not supported for upserts. "
                f"Please use 'delta' format."
            )
        
        if self.write_mode == "overwrite" and self.format != "delta" and not self.location:
            raise DatasetError(
                f"Format '{self.format}' is supported for overwrites only if the location is provided. "
                f"Please provide a valid path in an external location."
            )


class ExternalTableDataset(BaseTableDataset):
    """``ExternalTableDataset`` loads and saves data into external tables in Databricks.
    Load and save can be in Spark or Pandas dataframes, specified in dataframe_type.
    """

    def _create_table(
        self,
        table: str,
        catalog: str | None,
        database: str,
        format: str,
        write_mode: str | None,
        dataframe_type: str,
        primary_key: str | list[str] | None,
        json_schema: dict[str, Any] | None,
        partition_columns: list[str] | None,
        owner_group: str | None
    ) -> ExternalTable:
        """Creates a new ``ExternalTable`` instance with the provided attributes.

        Args:
            table: The name of the table.
            catalog: The catalog of the table.
            database: The database of the table.
            format: The format of the table.
            write_mode: The write mode for the table.
            dataframe_type: The type of dataframe.
            primary_key: The primary key of the table.
            json_schema: The JSON schema of the table.
            partition_columns: The partition columns of the table.
            owner_group: The owner group of the table.

        Returns:
            ``ExternalTable``: The new ``ExternalTable`` instance.
        """
        return ExternalTable(
            table=table,
            catalog=catalog,
            database=database,
            write_mode=write_mode,
            dataframe_type=dataframe_type,
            json_schema=json_schema,
            partition_columns=partition_columns,
            owner_group=owner_group,
            primary_key=primary_key,
            format=format
        )