"""``ManagedTableDataSet`` implementation to access managed delta tables
in Databricks.
"""
import logging
import re
from dataclasses import dataclass
from functools import partial
from operator import attrgetter
from typing import Any, Dict, List, Union

import pandas as pd
from cachetools import Cache, cachedmethod
from cachetools.keys import hashkey
from kedro.io.core import (
    AbstractVersionedDataSet,
    DataSetError,
    Version,
    VersionNotFoundError,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException, ParseException

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ManagedTable:  # pylint: disable=R0902
    """Stores the definition of a managed table"""

    # regex for tables, catalogs and schemas
    _NAMING_REGEX = r"\b[0-9a-zA-Z_-]{1,32}\b"
    _VALID_WRITE_MODES = ["overwrite", "upsert", "append"]
    _VALID_DATAFRAME_TYPES = ["spark", "pandas"]
    database: str
    catalog: str
    table: str
    write_mode: str
    dataframe_type: str
    primary_key: str
    owner_group: str
    partition_columns: Union[str, List[str]]
    json_schema: StructType

    def __post_init__(self):
        """Run validation methods if declared.
        The validation method can be a simple check
        that raises DataSetError.
        The validation is performed by calling a function named:
            `validate_<field_name>(self, value) -> raises DataSetError`
        """
        for name, _ in self.__dataclass_fields__.items():  # pylint: disable=E1101
            if method := getattr(self, f"validate_{name}", None):
                method()

    def validate_table(self):
        """validates table name

        Raises:
            DataSetError:
        """
        if not re.fullmatch(self._NAMING_REGEX, self.table):
            raise DataSetError("table does not conform to naming")

    def validate_database(self):
        """validates database name

        Raises:
            DataSetError: If the table name does not conform to naming constraints.
        """
        if self.database:
            if not re.fullmatch(self._NAMING_REGEX, self.database):
                raise DataSetError("database does not conform to naming")

    def validate_catalog(self):
        """validates catalog name

        Raises:
            DataSetError: If the catalog name does not conform to naming constraints.
        """
        if self.catalog:
            if not re.fullmatch(self._NAMING_REGEX, self.catalog):
                raise DataSetError("catalog does not conform to naming")

    def validate_write_mode(self):
        """validates the write mode

        Raises:
            DataSetError: If an invalid `write_mode` is passed.
        """
        if self.write_mode not in self._VALID_WRITE_MODES:
            valid_modes = ", ".join(self._VALID_WRITE_MODES)
            raise DataSetError(
                f"Invalid `write_mode` provided: {self.write_mode}. "
                f"`write_mode` must be one of: {valid_modes}"
            )

    def validate_dataframe_type(self):
        """validates the dataframe type

        Raises:
            DataSetError: If an invalid `dataframe_type` is passed
        """
        if self.dataframe_type not in self._VALID_DATAFRAME_TYPES:
            valid_types = ", ".join(self._VALID_DATAFRAME_TYPES)
            raise DataSetError(f"`dataframe_type` must be one of {valid_types}")

    def validate_primary_key(self):
        """validates the primary key of the table

        Raises:
            DataSetError: If no `primary_key` is specified.
        """
        if self.primary_key is None or len(self.primary_key) == 0:
            if self.write_mode == "upsert":
                raise DataSetError(
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
        elif self.table:
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
        except ParseException as exc:
            raise DataSetError(exc) from exc
        return schema


class ManagedTableDataSet(AbstractVersionedDataSet):
    """``ManagedTableDataSet`` loads and saves data into managed delta tables on Databricks.
        Load and save can be in Spark or Pandas dataframes, specified in dataframe_type.
        When saving data, you can specify one of three modes: overwtire(default), append,
        or upsert. Upsert requires you to specify the primary_column parameter which
        will be used as part of the join condition. This dataset works best with
        the databricks kedro starter. That starter comes with hooks that allow this
        dataset to function properly. Follow the instructions in that starter to
        setup your project for this dataset.

        Example usage for the
        `YAML API <https://kedro.readthedocs.io/en/stable/data/\
        data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

        .. code-block:: yaml

            names_and_ages@spark:
              type: databricks.ManagedTableDataSet
              table: names_and_ages

            names_and_ages@pandas:
              type: databricks.ManagedTableDataSet
              table: names_and_ages
              dataframe_type: pandas

        Example usage for the
        `Python API <https://kedro.readthedocs.io/en/stable/data/\
        data_catalog.html#use-the-data-catalog-with-the-code-api>`_:
        ::

            % pyspark --packages io.delta:delta-core_2.12:1.2.1
            --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
            --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

            >>> from pyspark.sql import SparkSession
            >>> from pyspark.sql.types import (StructField, StringType,
                                            IntegerType, StructType)
            >>> from kedro_datasets.databricks import ManagedTableDataSet
            >>> schema = StructType([StructField("name", StringType(), True),
                                    StructField("age", IntegerType(), True)])
            >>> data = [('Alex', 31), ('Bob', 12), ('Clarke', 65), ('Dave', 29)]
            >>> spark_df = SparkSession.builder.getOrCreate().createDataFrame(data, schema)
            >>> data_set = ManagedTableDataSet(table="names_and_ages")
            >>> data_set.save(spark_df)
            >>> reloaded = data_set.load()
            >>> reloaded.take(4)
        """

    # this dataset cannot be used with ``ParallelRunner``,
    # therefore it has the attribute ``_SINGLE_PROCESS = True``
    # for parallelism within a Spark pipeline please consider
    # using ``ThreadRunner`` instead
    _SINGLE_PROCESS = True

    def __init__(  # pylint: disable=R0913
        self,
        table: str,
        catalog: str = None,
        database: str = "default",
        write_mode: str = "overwrite",
        dataframe_type: str = "spark",
        primary_key: Union[str, List[str]] = None,
        version: Version = None,
        *,
        # the following parameters are used by project hooks
        # to create or update table properties
        schema: Dict[str, Any] = None,
        partition_columns: List[str] = None,
        owner_group: str = None,
    ) -> None:
        """Creates a new instance of ``ManagedTableDataSet``

        Args:
            table (str): the name of the table
            catalog (str, optional): the name of the catalog in Unity.
             Defaults to None.
            database (str, optional): the name of the database
             (also referred to as schema). Defaults to "default".
            write_mode (str, optional): the mode to write the data into the table.
             Options are:["overwrite", "append", "upsert"].
             "upsert" mode requires primary_key field to be populated.
             Defaults to "overwrite".
            dataframe_type (str, optional): "pandas" or "spark" dataframe.
             Defaults to "spark".
            primary_key (Union[str, List[str]], optional): the primary key of the table.
             Can be in the form of a list. Defaults to None.
            version (Version, optional): kedro.io.core.Version instance to load the data.
             Defaults to None.
            schema (Dict[str, Any], optional): the schema of the table in JSON form.
             Dataframes will be truncated to match the schema if provided.
             Used by the hooks to create the table if the schema is provided
             Defaults to None.
            partition_columns (List[str], optional): the columns to use for partitioning the table.
             Used by the hooks. Defaults to None.
            owner_group (str, optional): if table access control is enabled in your workspace,
             specifying owner_group will transfer ownership of the table and database to
             this owner. All databases should have the same owner_group. Defaults to None.
        Raises:
            DataSetError: Invalid configuration supplied (through ManagedTable validation)
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

        self._version_cache = Cache(maxsize=2)
        self._version = version

        super().__init__(
            filepath=None,
            version=version,
            exists_function=self._exists,
        )

    @cachedmethod(cache=attrgetter("_version_cache"), key=partial(hashkey, "load"))
    def _fetch_latest_load_version(self) -> int:
        # When load version is unpinned, fetch the most recent existing
        # version from the given path.
        latest_history = (
            self._get_spark()
            .sql(f"DESCRIBE HISTORY {self._table.full_table_location()} LIMIT 1")
            .collect()
        )
        if len(latest_history) != 1:
            raise VersionNotFoundError(
                f"Did not find any versions for {self._table.full_table_location()}"
            )
        return latest_history[0].version

    # 'key' is set to prevent cache key overlapping for load and save:
    # https://cachetools.readthedocs.io/en/stable/#cachetools.cachedmethod
    @cachedmethod(cache=attrgetter("_version_cache"), key=partial(hashkey, "save"))
    def _fetch_latest_save_version(self) -> int:
        """Generate and cache the current save version"""
        return None

    @staticmethod
    def _get_spark() -> SparkSession:
        return SparkSession.builder.getOrCreate()

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
                    self._get_spark()
                    .read.format("delta")
                    .option("versionAsOf", self._version.load)
                    .table(self._table.full_table_location())
                )
            except Exception as exc:
                raise VersionNotFoundError(self._version) from exc
        else:
            data = self._get_spark().table(self._table.full_table_location())
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
            base_data = self._get_spark().table(self._table.full_table_location())
            base_columns = base_data.columns
            update_columns = update_data.columns

            if set(update_columns) != set(base_columns):
                raise DataSetError(
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
            self._get_spark().conf.set(
                "fullTableAddress", self._table.full_table_location()
            )
            self._get_spark().conf.set("whereExpr", where_expr)
            upsert_sql = """MERGE INTO ${fullTableAddress} base USING update ON ${whereExpr}
                WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"""
            self._get_spark().sql(upsert_sql)
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
        # filter columns specified in schema and match their ordering
        if self._table.schema():
            cols = self._table.schema().fieldNames()
            if self._table.dataframe_type == "pandas":
                data = self._get_spark().createDataFrame(
                    data.loc[:, cols], schema=self._table.schema()
                )
            else:
                data = data.select(*cols)
        else:
            if self._table.dataframe_type == "pandas":
                data = self._get_spark().createDataFrame(data)
        if self._table.write_mode == "overwrite":
            self._save_overwrite(data)
        elif self._table.write_mode == "upsert":
            self._save_upsert(data)
        elif self._table.write_mode == "append":
            self._save_append(data)

    def _describe(self) -> Dict[str, str]:
        """Returns a description of the instance of ManagedTableDataSet

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
            "version": self._version,
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
                self._get_spark().sql(f"USE CATALOG {self._table.catalog}")
            except (ParseException, AnalysisException) as exc:
                logger.warning(
                    "catalog %s not found or unity not enabled. Error message: %s",
                    self._table.catalog,
                    exc,
                )
        try:
            return (
                self._get_spark()
                .sql(f"SHOW TABLES IN `{self._table.database}`")
                .filter(f"tableName = '{self._table.table}'")
                .count()
                > 0
            )
        except (ParseException, AnalysisException) as exc:
            logger.warning("error occured while trying to find table: %s", exc)
            return False
