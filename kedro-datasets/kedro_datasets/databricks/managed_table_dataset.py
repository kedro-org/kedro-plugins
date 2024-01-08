"""``ManagedTableDataset`` implementation to access managed delta tables
in Databricks.
"""
import logging
import re
from dataclasses import dataclass
from typing import Any, Optional, Union

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


@dataclass(frozen=True)
class ManagedTable:
    """Stores the definition of a managed table"""

    # regex for tables, catalogs and schemas
    _NAMING_REGEX = r"\b[0-9a-zA-Z_-]{1,}\b"
    _VALID_WRITE_MODES = ["overwrite", "upsert", "append"]
    _VALID_DATAFRAME_TYPES = ["spark", "pandas"]
    database: str
    catalog: Optional[str]
    table: str
    write_mode: Union[str, None]
    dataframe_type: str
    primary_key: Optional[str]
    owner_group: str
    partition_columns: Union[str, list[str]]
    json_schema: StructType

    def __post_init__(self):
        """Run validation methods if declared.
        The validation method can be a simple check
        that raises DatasetError.
        The validation is performed by calling a function named:
            `validate_<field_name>(self, value) -> raises DatasetError`
        """
        for name in self.__dataclass_fields__.keys():
            method = getattr(self, f"_validate_{name}", None)
            if method:
                method()

    def _validate_table(self):
        """Validates table name

        Raises:
            DatasetError: If the table name does not conform to naming constraints.
        """
        if not re.fullmatch(self._NAMING_REGEX, self.table):
            raise DatasetError("table does not conform to naming")

    def _validate_database(self):
        """Validates database name

        Raises:
            DatasetError: If the dataset name does not conform to naming constraints.
        """
        if not re.fullmatch(self._NAMING_REGEX, self.database):
            raise DatasetError("database does not conform to naming")

    def _validate_catalog(self):
        """Validates catalog name

        Raises:
            DatasetError: If the catalog name does not conform to naming constraints.
        """
        if self.catalog:
            if not re.fullmatch(self._NAMING_REGEX, self.catalog):
                raise DatasetError("catalog does not conform to naming")

    def _validate_write_mode(self):
        """Validates the write mode

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
        """Validates the dataframe type

        Raises:
            DatasetError: If an invalid `dataframe_type` is passed
        """
        if self.dataframe_type not in self._VALID_DATAFRAME_TYPES:
            valid_types = ", ".join(self._VALID_DATAFRAME_TYPES)
            raise DatasetError(f"`dataframe_type` must be one of {valid_types}")

    def _validate_primary_key(self):
        """Validates the primary key of the table

        Raises:
            DatasetError: If no `primary_key` is specified.
        """
        if self.primary_key is None or len(self.primary_key) == 0:
            if self.write_mode == "upsert":
                raise DatasetError(
                    f"`primary_key` must be provided for"
                    f"`write_mode` {self.write_mode}"
                )

    def full_table_location(self) -> str:
        """Returns the full table location

        Returns:
            str: table location in the format catalog.database.table
        """
        full_table_location = None
        if self.catalog and self.database and self.table:
            full_table_location = f"`{self.catalog}`.`{self.database}`.`{self.table}`"
        elif self.database and self.table:
            full_table_location = f"`{self.database}`.`{self.table}`"
        return full_table_location

    def schema(self) -> StructType:
        """Returns the Spark schema of the table if it exists

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
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        names_and_ages@spark:
          type: databricks.ManagedTableDataset
          table: names_and_ages

        names_and_ages@pandas:
          type: databricks.ManagedTableDataset
          table: names_and_ages
          dataframe_type: pandas

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
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
        >>> spark_df = SparkSession.builder.config("spark.jars.packages", f"io.delta:delta-core_2.12:{DELTA_VERSION}").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog",).getOrCreate().createDataFrame(data, schema)
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
        catalog: str = None,
        database: str = "default",
        write_mode: Union[str, None] = None,
        dataframe_type: str = "spark",
        primary_key: Optional[Union[str, list[str]]] = None,
        version: Version = None,
        # the following parameters are used by project hooks
        # to create or update table properties
        schema: dict[str, Any] = None,
        partition_columns: list[str] = None,
        owner_group: str = None,
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

        super().__init__(
            filepath=None,
            version=version,
            exists_function=self._exists,
        )

    def _load(self) -> Union[DataFrame, pd.DataFrame]:
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
            self._table.full_table_location()
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
        delta_table.saveAsTable(self._table.full_table_location())

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

    def _save(self, data: Union[DataFrame, pd.DataFrame]) -> None:
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
        if self._table.schema():
            cols = self._table.schema().fieldNames()
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

    def _describe(self) -> dict[str, str]:
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
