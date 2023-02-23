import logging
from typing import Any, Dict, List, Union
import pandas as pd

from kedro.io.core import (
    AbstractVersionedDataSet,
    DataSetError,
    VersionNotFoundError,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException
from cachetools import Cache

logger = logging.getLogger(__name__)


class ManagedTableDataSet(AbstractVersionedDataSet):
    """``ManagedTableDataSet`` loads data into Unity managed tables."""

    # this dataset cannot be used with ``ParallelRunner``,
    # therefore it has the attribute ``_SINGLE_PROCESS = True``
    # for parallelism within a Spark pipeline please consider
    # using ``ThreadRunner`` instead
    _SINGLE_PROCESS = True
    _VALID_WRITE_MODES = ["overwrite", "upsert", "append"]
    _VALID_DATAFRAME_TYPES = ["spark", "pandas"]

    def __init__(
        self,
        table: str,
        catalog: str = None,
        database: str = "default",
        write_mode: str = "overwrite",
        dataframe_type: str = "spark",
        primary_key: Union[str, List[str]] = None,
        version: int = None,
        *,
        # the following parameters are used by the hook to create or update unity
        schema: Dict[str, Any] = None,  # pylint: disable=unused-argument
        partition_columns: List[str] = None,  # pylint: disable=unused-argument
        owner_group: str = None,
    ) -> None:
        """Creates a new instance of ``ManagedTableDataSet``."""

        self._database = database
        self._catalog = catalog
        self._table = table
        self._owner_group = owner_group
        self._partition_columns = partition_columns
        if catalog and database and table:
            self._full_table_address = f"{catalog}.{database}.{table}"
        elif table:
            self._full_table_address = f"{database}.{table}"

        if write_mode not in self._VALID_WRITE_MODES:
            valid_modes = ", ".join(self._VALID_WRITE_MODES)
            raise DataSetError(
                f"Invalid `write_mode` provided: {write_mode}. "
                f"`write_mode` must be one of: {valid_modes}"
            )
        self._write_mode = write_mode

        if dataframe_type not in self._VALID_DATAFRAME_TYPES:
            valid_types = ", ".join(self._VALID_DATAFRAME_TYPES)
            raise DataSetError(f"`dataframe_type` must be one of {valid_types}")
        self._dataframe_type = dataframe_type

        if primary_key is None or len(primary_key) == 0:
            if write_mode == "upsert":
                raise DataSetError(
                    f"`primary_key` must be provided for" f"`write_mode` {write_mode}"
                )

        self._primary_key = primary_key

        self._version = version
        self._version_cache = Cache(maxsize=2)

        self._schema = None
        if schema is not None:
            self._schema = StructType.fromJson(schema)

    def _get_spark(self) -> SparkSession:
        return (
            SparkSession.builder.config(
                "spark.jars.packages", "io.delta:delta-core_2.12:1.2.1"
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )

    def _load(self) -> Union[DataFrame, pd.DataFrame]:
        if self._version is not None and self._version >= 0:
            try:
                data = (
                    self._get_spark()
                    .read.format("delta")
                    .option("versionAsOf", self._version)
                    .table(self._full_table_address)
                )
            except:
                raise VersionNotFoundError
        else:
            data = self._get_spark().table(self._full_table_address)
        if self._dataframe_type == "pandas":
            data = data.toPandas()
        return data

    def _save_append(self, data: DataFrame) -> None:
        data.write.format("delta").mode("append").saveAsTable(self._full_table_address)

    def _save_overwrite(self, data: DataFrame) -> None:
        delta_table = data.write.format("delta")
        if self._write_mode == "overwrite":
            delta_table = delta_table.mode("overwrite").option(
                "overwriteSchema", "true"
            )
        delta_table.saveAsTable(self._full_table_address)

    def _save_upsert(self, update_data: DataFrame) -> None:
        if self._exists():
            base_data = self._get_spark().table(self._full_table_address)
            base_columns = base_data.columns
            update_columns = update_data.columns

            if set(update_columns) != set(base_columns):
                raise DataSetError(
                    f"Upsert requires tables to have identical columns. "
                    f"Delta table {self._full_table_address} "
                    f"has columns: {base_columns}, whereas "
                    f"dataframe has columns {update_columns}"
                )

            where_expr = ""
            if isinstance(self._primary_key, str):
                where_expr = f"base.{self._primary_key}=update.{self._primary_key}"
            elif isinstance(self._primary_key, list):
                where_expr = " AND ".join(
                    f"base.{col}=update.{col}" for col in self._primary_key
                )

            update_data.createOrReplaceTempView("update")

            upsert_sql = f"""MERGE INTO {self._full_table_address} base USING update
            ON {where_expr} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *
            """
            self._get_spark().sql(upsert_sql)
        else:
            self._save_append(update_data)

    def _save(self, data: Any) -> None:
        # filter columns specified in schema and match their ordering
        if self._schema:
            cols = self._schema.fieldNames()
            if self._dataframe_type == "pandas":
                data = self._get_spark().createDataFrame(
                    data.loc[:, cols], schema=self._schema
                )
            else:
                data = data.select(*cols)
        else:
            if self._dataframe_type == "pandas":
                data = self._get_spark().createDataFrame(data)
        if self._write_mode == "overwrite":
            self._save_overwrite(data)
        elif self._write_mode == "upsert":
            self._save_upsert(data)
        elif self._write_mode == "append":
            self._save_append(data)

    def _describe(self) -> Dict[str, str]:
        return dict(
            catalog=self._catalog,
            database=self._database,
            table=self._table,
            write_mode=self._write_mode,
            dataframe_type=self._dataframe_type,
            primary_key=self._primary_key,
            version=self._version,
            owner_group=self._owner_group,
        )

    def _exists(self) -> bool:
        if self._catalog:
            try:
                self._get_spark().sql(f"USE CATALOG {self._catalog}")
            except:
                logger.warn(f"catalog {self._catalog} not found")
        try:
            return (
                self._get_spark()
                .sql(f"SHOW TABLES IN `{self._database}`")
                .filter(f"tableName = '{self._table}'")
                .count()
                > 0
            )
        except:
            return False
