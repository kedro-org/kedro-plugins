"""``ExternalTableDataset`` implementation to access external tables
in Databricks.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd
from kedro.io.core import DatasetError
from pyspark.sql import DataFrame

from kedro_datasets.databricks._base_table_dataset import BaseTable, BaseTableDataset

logger = logging.getLogger(__name__)
pd.DataFrame.iteritems = pd.DataFrame.items


@dataclass(frozen=True)
class ExternalTable(BaseTable):
    """Stores the definition of an external table."""

    def _validate_location(self) -> None:
        """Validates that a location is provided if the table does not exist.

        Raises:
            DatasetError: If the table does not exist and no location is provided.
        """
        if not self.exists() and not self.location:
            raise DatasetError(
                "If the external table does not exists, the `location` parameter must be provided. "
                "This should be valid path in an external location that has already been created."
            )

    def _validate_write_mode(self) -> None:
        """Validates that the write mode is compatible with the format.

        Raises:
            DatasetError: If the write mode is not compatible with the format.
        """
        super()._validate_write_mode()

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
    """`ExternalTableDataset` loads and saves data into external tables in Databricks.
    Load and save operations can use either Spark or Pandas DataFrames, specified via the `dataframe_type` argument.

    ### Example usage for the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

    ```yaml
    names_and_ages@spark:
        type: databricks.ExternalTableDataset
        format: parquet
        table: names_and_ages

    names_and_ages@pandas:
        type: databricks.ExternalTableDataset
        format: parquet
        table: names_and_ages
        dataframe_type: pandas
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

    ```python
    from kedro_datasets.databricks import ExternalTableDataset
    from pyspark.sql import SparkSession
    from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType
    import importlib_metadata

    DELTA_VERSION = importlib_metadata.version("delta-spark")
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]

    spark_df = (
        SparkSession.builder.config(
            "spark.jars.packages", f"io.delta:delta-core_2.12:{DELTA_VERSION}"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
        .createDataFrame(data, schema)
    )

    dataset = ExternalTableDataset(
        table="names_and_ages",
        write_mode="overwrite",
        location="abfss://container@storageaccount.dfs.core.windows.net/depts/cust"
    )

    dataset.save(spark_df)
    reloaded = dataset.load()
    assert Row(name="Bob", age=12) in reloaded.take(4)
    ```

    """

    def _create_table(  # noqa: PLR0913
        self,
        table: str,
        catalog: str | None,
        database: str,
        format: str,
        write_mode: str | None,
        location: str | None,
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
            location: The location of the table.
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
            location=location,
            dataframe_type=dataframe_type,
            json_schema=json_schema,
            partition_columns=partition_columns,
            owner_group=owner_group,
            primary_key=primary_key,
            format=format
        )

    def _save_overwrite(self, data: DataFrame) -> None:
        """Overwrites the data in the table with the data provided.
        Args:
            data (DataFrame): The Spark dataframe to overwrite the table with.
        """
        writer = data.write.format(self._table.format).mode("overwrite").option(
            "overwriteSchema", "true"
        )

        if self._table.partition_columns:
            writer.partitionBy(
                *self._table.partition_columns if isinstance(self._table.partition_columns, list) else self._table.partition_columns
            )

        if self._table.format == "delta" or (not self._table.exists()):
            if self._table.location:
                writer.option("path", self._table.location)

            writer.saveAsTable(self._table.full_table_location() or "")

        else:
            writer.save(self._table.location)
