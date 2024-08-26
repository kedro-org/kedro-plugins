"""``ManagedTableDataset`` implementation to access managed delta tables
in Databricks.
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
from kedro_datasets.databricks._base_table_dataset import BaseTable, BaseTableDataset

logger = logging.getLogger(__name__)
pd.DataFrame.iteritems = pd.DataFrame.items


@dataclass(frozen=True)
class ManagedTable(BaseTable):
    """Stores the definition of a managed table"""

    _VALID_WRITE_MODES = ["overwrite", "upsert", "append"]
    primary_key: str | list[str] | None
    owner_group: str | None


class ManagedTableDataset(AbstractVersionedDataset):
    """``ManagedTableDataset`` loads and saves data into managed delta tables on Databricks.
    Load and save can be in Spark or Pandas dataframes, specified in dataframe_type.
    When saving data, you can specify one of three modes: overwrite(default), append,
    or upsert. Upsert requires you to specify the primary_column parameter which
    will be used as part of the join condition. This dataset works best with
    the databricks kedro starter. That starter comes with hooks that allow this
    dataset to function properly. Follow the instructions in that starter to
    setup your project for this dataset.

    Example usage for the
    `YAML API <https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        names_and_ages@spark:
          type: databricks.ManagedTableDataset
          table: names_and_ages

        names_and_ages@pandas:
          type: databricks.ManagedTableDataset
          table: names_and_ages
          dataframe_type: pandas

    Example usage for the
    `Python API <https://docs.kedro.org/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.databricks import ManagedTableDataset
        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType
        >>> import importlib_metadata
        >>>
        >>> DELTA_VERSION = importlib_metadata.version("delta-spark")
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
        >>> dataset = ManagedTableDataset(table="names_and_ages", write_mode="overwrite")
        >>> dataset.save(spark_df)
        >>> reloaded = dataset.load()
        >>> assert Row(name="Bob", age=12) in reloaded.take(4)
    """

    # this dataset cannot be used with ``ParallelRunner``,
    # therefore it has the attribute ``_SINGLE_PROCESS = True``
    # for parallelism within a Spark pipeline please consider
    # using ``ThreadRunner`` instead
    _SINGLE_PROCESS = True

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
            table: the name of the table
            catalog: the name of the catalog in Unity.
                Defaults to None.
            database: the name of the database.
                (also referred to as schema). Defaults to "default".
            write_mode: the mode to write the data into the table. If not
                present, the data set is read-only.
                Options are:["overwrite", "append", "upsert"].
                "upsert" mode requires primary_key field to be populated.
                Defaults to None.
            dataframe_type: "pandas" or "spark" dataframe.
                Defaults to "spark".
            primary_key: the primary key of the table.
                Can be in the form of a list. Defaults to None.
            version: kedro.io.core.Version instance to load the data.
                Defaults to None.
            schema: the schema of the table in JSON form.
                Dataframes will be truncated to match the schema if provided.
                Used by the hooks to create the table if the schema is provided
                Defaults to None.
            partition_columns: the columns to use for partitioning the table.
                Used by the hooks. Defaults to None.
            owner_group: if table access control is enabled in your workspace,
                specifying owner_group will transfer ownership of the table and database to
                this owner. All databases should have the same owner_group. Defaults to None.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        Raises:
            DatasetError: Invalid configuration supplied (through ManagedTable validation)
        """

        self._table = ManagedTable(
            database=database,
            catalog=catalog,
            table=table,
            write_mode=write_mode,
            dataframe_type=dataframe_type,
            primary_key=primary_key,
            owner_group=owner_group,
            partition_columns=partition_columns,
            json_schema=schema,
        )

        self._version = version
        self.metadata = metadata

        super().__init__(
            filepath=None,  # type: ignore[arg-type]
            version=version,
            exists_function=self._exists,  # type: ignore[arg-type]
        )

    def _load(self) -> DataFrame | pd.DataFrame:
        """Loads the version of data in the format defined in the init
        (spark|pandas dataframe)

        Raises:
            VersionNotFoundError: if the version defined in
                the init doesn't exist

        Returns:
            Union[DataFrame, pd.DataFrame]: Returns a dataframe
                in the format defined in the init
        """
        if self._version and self._version.load >= 0:
            try:
                data = (
                    _get_spark()
                    .read.format("delta")
                    .option("versionAsOf", self._version.load)
                    .table(self._table.full_table_location())
                )
            except Exception as exc:
                raise VersionNotFoundError(self._version.load) from exc
        else:
            data = _get_spark().table(self._table.full_table_location())
        if self._table.dataframe_type == "pandas":
            data = data.toPandas()
        return data

    def _save_append(self, data: DataFrame) -> None:
        """Saves the data to the table by appending it
        to the location defined in the init

        Args:
            data (DataFrame): the Spark dataframe to append to the table
        """
        data.write.format("delta").mode("append").saveAsTable(
            self._table.full_table_location() or ""
        )

    def _save_overwrite(self, data: DataFrame) -> None:
        """Overwrites the data in the table with the data provided.
        (this is the default save mode)

        Args:
            data (DataFrame): the Spark dataframe to overwrite the table with.
        """
        delta_table = data.write.format("delta")
        if self._table.write_mode == "overwrite":
            delta_table = delta_table.mode("overwrite").option(
                "overwriteSchema", "true"
            )
        delta_table.saveAsTable(self._table.full_table_location() or "")

    def _save_upsert(self, update_data: DataFrame) -> None:
        """Upserts the data by joining on primary_key columns or column.
        If table doesn't exist at save, the data is inserted to a new table.

        Args:
            update_data (DataFrame): the Spark dataframe to upsert
        """
        if self._exists():
            base_data = _get_spark().table(self._table.full_table_location())
            base_columns = base_data.columns
            update_columns = update_data.columns

            if set(update_columns) != set(base_columns):
                raise DatasetError(
                    f"Upsert requires tables to have identical columns. "
                    f"Delta table {self._table.full_table_location()} "
                    f"has columns: {base_columns}, whereas "
                    f"dataframe has columns {update_columns}"
                )

            where_expr = ""
            if isinstance(self._table.primary_key, str):
                where_expr = (
                    f"base.{self._table.primary_key}=update.{self._table.primary_key}"
                )
            elif isinstance(self._table.primary_key, list):
                where_expr = " AND ".join(
                    f"base.{col}=update.{col}" for col in self._table.primary_key
                )

            update_data.createOrReplaceTempView("update")
            _get_spark().conf.set("fullTableAddress", self._table.full_table_location())
            _get_spark().conf.set("whereExpr", where_expr)
            upsert_sql = """MERGE INTO ${fullTableAddress} base USING update ON ${whereExpr}
                WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"""
            _get_spark().sql(upsert_sql)
        else:
            self._save_append(update_data)

    def _save(self, data: DataFrame | pd.DataFrame) -> None:
        """Saves the data based on the write_mode and dataframe_type in the init.
        If write_mode is pandas, Spark dataframe is created first.
        If schema is provided, data is matched to schema before saving
        (columns will be sorted and truncated).

        Args:
            data (Any): Spark or pandas dataframe to save to the table location
        """
        if self._table.write_mode is None:
            raise DatasetError(
                "'save' can not be used in read-only mode. "
                "Change 'write_mode' value to `overwrite`, `upsert` or `append`."
            )
        # filter columns specified in schema and match their ordering
        schema = self._table.schema()
        if schema:
            cols = schema.fieldNames()
            if self._table.dataframe_type == "pandas":
                data = _get_spark().createDataFrame(
                    data.loc[:, cols], schema=self._table.schema()
                )
            else:
                data = data.select(*cols)
        elif self._table.dataframe_type == "pandas":
            data = _get_spark().createDataFrame(data)
        if self._table.write_mode == "overwrite":
            self._save_overwrite(data)
        elif self._table.write_mode == "upsert":
            self._save_upsert(data)
        elif self._table.write_mode == "append":
            self._save_append(data)

    def _describe(self) -> dict[str, str | list | None]:
        """Returns a description of the instance of ManagedTableDataset

        Returns:
            Dict[str, str]: Dict with the details of the dataset
        """
        return {
            "catalog": self._table.catalog,
            "database": self._table.database,
            "table": self._table.table,
            "write_mode": self._table.write_mode,
            "dataframe_type": self._table.dataframe_type,
            "primary_key": self._table.primary_key,
            "version": str(self._version),
            "owner_group": self._table.owner_group,
            "partition_columns": self._table.partition_columns,
        }

    def _exists(self) -> bool:
        """Checks to see if the table exists

        Returns:
            bool: boolean of whether the table defined
            in the dataset instance exists in the Spark session
        """
        if self._table.catalog:
            try:
                _get_spark().sql(f"USE CATALOG `{self._table.catalog}`")
            except (ParseException, AnalysisException) as exc:
                logger.warning(
                    "catalog %s not found or unity not enabled. Error message: %s",
                    self._table.catalog,
                    exc,
                )
        try:
            return (
                _get_spark()
                .sql(f"SHOW TABLES IN `{self._table.database}`")
                .filter(f"tableName = '{self._table.table}'")
                .count()
                > 0
            )
        except (ParseException, AnalysisException) as exc:
            logger.warning("error occured while trying to find table: %s", exc)
            return False
