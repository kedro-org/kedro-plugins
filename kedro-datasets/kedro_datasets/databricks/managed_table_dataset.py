"""``ManagedTableDataset`` implementation to access managed delta tables
in Databricks.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, ClassVar

import pandas as pd
from kedro.io.core import Version

from kedro_datasets.databricks._base_table_dataset import BaseTable, BaseTableDataset

logger = logging.getLogger(__name__)
pd.DataFrame.iteritems = pd.DataFrame.items


@dataclass(frozen=True)
class ManagedTable(BaseTable):
    """Stores the definition of a managed table."""

    _VALID_FORMATS: ClassVar[list[str]] = field(default=["delta"])


class ManagedTableDataset(BaseTableDataset):
    """``ManagedTableDataset`` loads and saves data into managed delta tables in Databricks.
    Load and save can be in Spark or Pandas dataframes, specified in dataframe_type.
    When saving data, you can specify one of three modes: overwrite, append,
    or upsert. Upsert requires you to specify the primary_column parameter which
    will be used as part of the join condition. This dataset works best with
    the databricks kedro starter. That starter comes with hooks that allow this
    dataset to function properly. Follow the instructions in that starter to
    setup your project for this dataset.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        names_and_ages@spark:
          type: databricks.ManagedTableDataset
          table: names_and_ages

        names_and_ages@pandas:
          type: databricks.ManagedTableDataset
          table: names_and_ages
          dataframe_type: pandas
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> import importlib.metadata
        >>>
        >>> from kedro_datasets.databricks import ManagedTableDataset
        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType
        >>>
        >>> DELTA_VERSION = importlib.metadata.version("delta-spark")
        >>> schema = StructType(
        ...     [StructField("name", StringType(), True), StructField("age", IntegerType(), True)]
        ... )
        >>> data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]
        >>> spark_df = (
        ...     SparkSession.builder.config(
        ...         "spark.jars.packages", f"io.delta:delta-core_2.12:{DELTA_VERSION}"
        ...     )
        ...     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        ...     .config(
        ...         "spark.sql.catalog.spark_catalog",
        ...         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        ...     )
        ...     .getOrCreate()
        ...     .createDataFrame(data, schema)
        ... )
        >>>
        >>> dataset = ManagedTableDataset(table="names_and_ages", write_mode="overwrite")
        >>> dataset.save(spark_df)
        >>> reloaded = dataset.load()

    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        table: str,
        catalog: str | None = None,
        database: str = "default",
        write_mode: str | None = None,
        dataframe_type: str = "spark",
        primary_key: str | list[str] | None = None,
        version: Version | None = None,
        # the following parameters are used by project hooks
        # to create or update table properties
        schema: dict[str, Any] | None = None,
        partition_columns: list[str] | None = None,
        owner_group: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``ManagedTableDataset``.

        Args:
            table: The name of the table.
            catalog: The name of the catalog in Unity.
                Defaults to None.
            database: The name of the database.
                (also referred to as schema). Defaults to "default".
            write_mode: the mode to write the data into the table. If not
                present, the dataset is read-only.
                Options are:["overwrite", "append", "upsert"].
                "upsert" mode requires primary_key field to be populated.
                Defaults to None.
            dataframe_type: "pandas" or "spark" dataframe.
                Defaults to "spark".
            primary_key: The primary key of the table.
                Can be in the form of a list. Defaults to None.
            version: kedro.io.core.Version instance to load the data.
                Defaults to None.
            schema: The schema of the table in JSON form.
                Dataframes will be truncated to match the schema if provided.
                Used by the hooks to create the table if the schema is provided.
                Defaults to None.
            partition_columns: The columns to use for partitioning the table.
                Used by the hooks. Defaults to None.
            owner_group: If table access control is enabled in your workspace,
                specifying owner_group will transfer ownership of the table and database to
                this owner. All databases should have the same owner_group. Defaults to None.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        Raises:
            DatasetError: Invalid configuration supplied (through ``ManagedTable`` validation).
        """
        super().__init__(
            database=database,
            catalog=catalog,
            table=table,
            write_mode=write_mode,
            dataframe_type=dataframe_type,
            version=version,
            schema=schema,
            partition_columns=partition_columns,
            metadata=metadata,
            primary_key=primary_key,
            owner_group=owner_group,
        )

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
        owner_group: str | None,
    ) -> ManagedTable:
        """Creates a new ``ManagedTable`` instance with the provided attributes.

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
            ``ManagedTable``: The new ``ManagedTable`` instance.
        """
        return ManagedTable(
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
            format=format,
        )

    def _describe(self) -> dict[str, str | list | None]:
        """Returns a description of the instance of the dataset.

        Returns:
            Dict[str, str]: Dict with the details of the dataset.
        """
        description = super()._describe()
        del description["format"]
        del description["location"]

        return description
