"""``ExternalTableDataset`` implementation to access external tables
in Databricks.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd

from kedro_datasets.databricks._base_table_dataset import BaseTable, BaseTableDataset

logger = logging.getLogger(__name__)
pd.DataFrame.iteritems = pd.DataFrame.items


@dataclass(frozen=True, kw_only=True)
class ExternalTable(BaseTable):
    """Stores the definition of an external table"""


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
        """Creates a new ExternalTable instance with the provided attributes.

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
            ExternalTable: the new ExternalTable instance
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