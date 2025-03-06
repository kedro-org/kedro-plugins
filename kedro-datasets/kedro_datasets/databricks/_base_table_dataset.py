"""``BaseTableDataset`` implementation used to add the base for
``ManagedTableDataset`` and ``ExternalTableDataset``.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, ClassVar

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

from kedro_datasets._utils.spark_utils import get_spark

logger = logging.getLogger(__name__)
pd.DataFrame.iteritems = pd.DataFrame.items


@dataclass(frozen=True)
class BaseTable:
    """Stores the definition of a base table.

    Acts as the base class for `ManagedTable` and `ExternalTable`.
    """

    # regex for tables, catalogs and schemas
    _NAMING_REGEX: ClassVar[str] = r"\b[0-9a-zA-Z_-]{1,}\b"
    _VALID_WRITE_MODES: ClassVar[list[str]] = field(
        default=["overwrite", "upsert", "append"]
    )
    _VALID_DATAFRAME_TYPES: ClassVar[list[str]] = field(default=["spark", "pandas"])
    _VALID_FORMATS: ClassVar[list[str]] = field(
        default=["delta", "parquet", "csv", "json", "orc", "avro", "text"]
    )

    database: str
    catalog: str | None
    table: str
    write_mode: str | None
    location: str | None
    dataframe_type: str
    primary_key: str | list[str] | None
    owner_group: str | None
    partition_columns: str | list[str] | None
    format: str = "delta"
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
            raise DatasetError("Table does not conform to naming")

    def _validate_database(self):
        """Validates database name.

        Raises:
            DatasetError: If the dataset name does not conform to naming constraints.
        """
        if not re.fullmatch(self._NAMING_REGEX, self.database):
            raise DatasetError("Database does not conform to naming")

    def _validate_catalog(self):
        """Validates catalog name.

        Raises:
            DatasetError: If the catalog name does not conform to naming constraints.
        """
        if self.catalog:
            if not re.fullmatch(self._NAMING_REGEX, self.catalog):
                raise DatasetError("Catalog does not conform to naming")

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
            DatasetError: If an invalid `dataframe_type` is passed.
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
            str | None : Table location in the format catalog.database.table or None if database and table aren't defined.
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
            StructType: The schema of the table.
        """
        schema = None
        try:
            if self.json_schema is not None:
                schema = StructType.fromJson(self.json_schema)
        except (KeyError, ValueError) as exc:
            raise DatasetError(exc) from exc
        return schema

    def exists(self) -> bool:
        """Checks to see if the table exists.

        Returns:
            bool: Boolean of whether the table exists in the Spark session.
        """
        if self.catalog:
            try:
                get_spark().sql(f"USE CATALOG `{self.catalog}`")
            except (ParseException, AnalysisException) as exc:
                logger.warning(
                    "catalog %s not found or unity not enabled. Error message: %s",
                    self.catalog,
                    exc,
                )
        try:
            return (
                get_spark()
                .sql(f"SHOW TABLES IN `{self.database}`")
                .filter(f"tableName = '{self.table}'")
                .count()
                > 0
            )
        except (ParseException, AnalysisException) as exc:
            logger.warning("error occured while trying to find table: %s", exc)
            return False


class BaseTableDataset(AbstractVersionedDataset):
    """``BaseTableDataset`` loads and saves data into managed delta tables or external tables on Databricks.
    Load and save can be in Spark or Pandas dataframes, specified in dataframe_type.

    This dataset is not meant to be used directly. It is a base class for ``ManagedTableDataset`` and ``ExternalTableDataset``.
    """

    # datasets that inherit from this class cannot be used with ``ParallelRunner``,
    # therefore it has the attribute ``_SINGLE_PROCESS = True``
    # for parallelism within a Spark pipeline please consider
    # using ``ThreadRunner`` instead.
    _SINGLE_PROCESS = True

    def __init__(  # noqa: PLR0913
        self,
        *,
        table: str,
        catalog: str | None = None,
        database: str = "default",
        format: str = "delta",
        write_mode: str | None = None,
        location: str | None = None,
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
        """Creates a new instance of ``BaseTableDataset``.

        Args:
            table: The name of the table.
            catalog: The name of the catalog in Unity.
                Defaults to None.
            database: The name of the database.
                (also referred to as schema). Defaults to "default".
            format: The format of the table.
                Applicable only for external tables.
                Defaults to "delta".
            write_mode: The mode to write the data into the table. If not
                present, the data set is read-only.
                Options are:["overwrite", "append", "upsert"].
                "upsert" mode requires primary_key field to be populated.
                Defaults to None.
            location: The location of the table.
                Applicable only for external tables.
                Should be a valid path in an external location that has already been created.
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
            DatasetError: Invalid configuration supplied (through ``BaseTable`` validation).
        """
        self._table = self._create_table(
            table=table,
            catalog=catalog,
            database=database,
            format=format,
            write_mode=write_mode,
            location=location,
            dataframe_type=dataframe_type,
            primary_key=primary_key,
            json_schema=schema,
            partition_columns=partition_columns,
            owner_group=owner_group,
        )

        self.metadata = metadata
        self._version = version

        super().__init__(
            filepath=None,  # type: ignore[arg-type]
            version=version,
            exists_function=self._exists,  # type: ignore[arg-type]
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
    ) -> BaseTable:
        """Creates a ``BaseTable`` instance with the provided attributes.

        Args:
            table: The name of the table.
            catalog: The catalog of the table.
            database: The database of the table.
            format: The format of the table.
            location: The location of the table.
            write_mode: The write mode for the table.
            dataframe_type: The type of dataframe.
            primary_key: The primary key of the table.
            json_schema: The JSON schema of the table.
            partition_columns: The partition columns of the table.
            owner_group: The owner group of the table.

        Returns:
            ``BaseTable``: The new ``BaseTable`` instance.
        """
        return BaseTable(
            table=table,
            catalog=catalog,
            database=database,
            format=format,
            write_mode=write_mode,
            location=location,
            dataframe_type=dataframe_type,
            json_schema=json_schema,
            partition_columns=partition_columns,
            owner_group=owner_group,
            primary_key=primary_key,
        )

    def _load(self) -> DataFrame | pd.DataFrame:
        """Loads the version of data in the format defined in the init
        (spark|pandas dataframe).

        Raises:
            VersionNotFoundError: If the version defined in
                the init doesn't exist.

        Returns:
            Union[DataFrame, pd.DataFrame]: Returns a dataframe
                in the format defined in the init.
        """
        if self._version and self._version.load >= 0:
            try:
                data = (
                    get_spark()
                    .read.format("delta")
                    .option("versionAsOf", self._version.load)
                    .table(self._table.full_table_location())
                )
            except Exception as exc:
                raise VersionNotFoundError(self._version.load) from exc
        else:
            data = get_spark().table(self._table.full_table_location())
        if self._table.dataframe_type == "pandas":
            data = data.toPandas()
        return data

    def _save(self, data: DataFrame | pd.DataFrame) -> None:
        """Saves the data based on the write_mode and dataframe_type in the init.
        If write_mode is pandas, Spark dataframe is created first.
        If schema is provided, data is matched to schema before saving
        (columns will be sorted and truncated).

        Args:
            data (Any): Spark or pandas dataframe to save to the table location.
        """
        if self._table.write_mode is None:
            raise DatasetError(
                "'save' can not be used in read-only mode. "
                f"Change 'write_mode' value to {', '.join(self._table._VALID_WRITE_MODES)}"
            )
        # filter columns specified in schema and match their ordering
        schema = self._table.schema()
        if schema:
            cols = schema.fieldNames()
            if self._table.dataframe_type == "pandas":
                data = get_spark().createDataFrame(
                    data.loc[:, cols], schema=self._table.schema()
                )
            else:
                data = data.select(*cols)
        elif self._table.dataframe_type == "pandas":
            data = get_spark().createDataFrame(data)

        method = getattr(self, f"_save_{self._table.write_mode}", None)
        if method:
            method(data)

    def _save_append(self, data: DataFrame) -> None:
        """Saves the data to the table by appending it
        to the location defined in the init.

        Args:
            data (DataFrame): The Spark dataframe to append to the table.
        """
        writer = data.write.format(self._table.format).mode("append")

        if self._table.partition_columns:
            writer.partitionBy(
                *self._table.partition_columns
                if isinstance(self._table.partition_columns, list)
                else [self._table.partition_columns]
            )

        if self._table.location:
            writer.option("path", self._table.location)

        writer.saveAsTable(self._table.full_table_location() or "")

    def _save_overwrite(self, data: DataFrame) -> None:
        """Overwrites the data in the table with the data provided.

        Args:
            data (DataFrame): The Spark dataframe to overwrite the table with.
        """
        writer = (
            data.write.format(self._table.format)
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )

        if self._table.partition_columns:
            writer.partitionBy(
                *self._table.partition_columns
                if isinstance(self._table.partition_columns, list)
                else [self._table.partition_columns]
            )

        if self._table.location:
            writer.option("path", self._table.location)

        writer.saveAsTable(self._table.full_table_location() or "")

    def _save_upsert(self, update_data: DataFrame) -> None:
        """Upserts the data by joining on primary_key columns or column.
        If table doesn't exist at save, the data is inserted to a new table.

        Args:
            update_data (DataFrame): The Spark dataframe to upsert.
        """
        if self._exists():
            base_data = get_spark().table(self._table.full_table_location())
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
            get_spark().conf.set("fullTableAddress", self._table.full_table_location())
            get_spark().conf.set("whereExpr", where_expr)
            upsert_sql = """MERGE INTO ${fullTableAddress} base USING update ON ${whereExpr}
                WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"""
            get_spark().sql(upsert_sql)
        else:
            self._save_append(update_data)

    def _describe(self) -> dict[str, str | list | None]:
        """Returns a description of the instance of the dataset.

        Returns:
            Dict[str, str]: Dict with the details of the dataset.
        """
        return {
            "catalog": self._table.catalog,
            "database": self._table.database,
            "table": self._table.table,
            "format": self._table.format,
            "location": self._table.location,
            "write_mode": self._table.write_mode,
            "dataframe_type": self._table.dataframe_type,
            "primary_key": self._table.primary_key,
            "version": str(self._version),
            "owner_group": self._table.owner_group,
            "partition_columns": self._table.partition_columns,
        }

    def _exists(self) -> bool:
        """Checks to see if the table exists.

        Returns:
            bool: Boolean of whether the table defined
            in the dataset instance exists in the Spark session.
        """
        return self._table.exists()
