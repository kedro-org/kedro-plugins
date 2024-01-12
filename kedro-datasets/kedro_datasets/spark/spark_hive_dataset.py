"""``AbstractDataset`` implementation to access Spark dataframes using
``pyspark`` on Apache Hive.
"""
import pickle
from copy import deepcopy
from typing import Any

from kedro.io.core import AbstractDataset, DatasetError
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, row_number

from kedro_datasets.spark.spark_dataset import _get_spark


class SparkHiveDataset(AbstractDataset[DataFrame, DataFrame]):
    """``SparkHiveDataset`` loads and saves Spark dataframes stored on Hive.
    This data set also handles some incompatible file types such as using partitioned parquet on
    hive which will not normally allow upserts to existing data without a complete replacement
    of the existing file/partition.

    This Dataset has some key assumptions:

    - Schemas do not change during the pipeline run (defined PKs must be present for the
      duration of the pipeline).
    - Tables are not being externally modified during upserts. The upsert method is NOT ATOMIC
      to external changes to the target table while executing. Upsert methodology works by
      leveraging Spark DataFrame execution plan checkpointing.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        hive_dataset:
          type: spark.SparkHiveDataset
          database: hive_database
          table: table_name
          write_mode: overwrite

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import StructField, StringType, IntegerType, StructType
        >>>
        >>> from kedro_datasets.spark import SparkHiveDataset
        >>>
        >>> schema = StructType(
        ...     [StructField("name", StringType(), True), StructField("age", IntegerType(), True)]
        ... )
        >>>
        >>> data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]
        >>>
        >>> spark_df = SparkSession.builder.getOrCreate().createDataFrame(data, schema)
        >>>
        >>> dataset = SparkHiveDataset(
        ...     database="test_database", table="test_table", write_mode="overwrite"
        ... )
        >>> dataset.save(spark_df)
        >>> reloaded = dataset.load()
        >>>
        >>> reloaded.take(4)
    """

    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        database: str,
        table: str,
        write_mode: str = "errorifexists",
        table_pk: list[str] = None,
        save_args: dict[str, Any] = None,
        metadata: dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``SparkHiveDataset``.

        Args:
            database: The name of the hive database.
            table: The name of the table within the database.
            write_mode: ``insert``, ``upsert`` or ``overwrite`` are supported.
            table_pk: If performing an upsert, this identifies the primary key columns used to
                resolve preexisting data. Is required for ``write_mode="upsert"``.
            save_args: Optional mapping of any options,
                passed to the `DataFrameWriter.saveAsTable` as kwargs.
                Key example of this is `partitionBy` which allows data partitioning
                on a list of column names.
                Other `HiveOptions` can be found here:
                https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html#specifying-storage-format-for-hive-tables
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Note:
            For users leveraging the `upsert` functionality,
            a `checkpoint` directory must be set, e.g. using
            `spark.sparkContext.setCheckpointDir("/path/to/dir")`
            or directly in the Spark conf folder.

        Raises:
            DatasetError: Invalid configuration supplied
        """
        _write_modes = ["append", "error", "errorifexists", "upsert", "overwrite"]
        if write_mode not in _write_modes:
            valid_modes = ", ".join(_write_modes)
            raise DatasetError(
                f"Invalid 'write_mode' provided: {write_mode}. "
                f"'write_mode' must be one of: {valid_modes}"
            )
        if write_mode == "upsert" and not table_pk:
            raise DatasetError("'table_pk' must be set to utilise 'upsert' read mode")

        self._write_mode = write_mode
        self._table_pk = table_pk or []
        self._database = database
        self._table = table
        self._full_table_address = f"{database}.{table}"
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)
        self._format = self._save_args.pop("format", None) or "hive"
        self._eager_checkpoint = self._save_args.pop("eager_checkpoint", None) or True

        self.metadata = metadata

    def _describe(self) -> dict[str, Any]:
        return {
            "database": self._database,
            "table": self._table,
            "write_mode": self._write_mode,
            "table_pk": self._table_pk,
            "partition_by": self._save_args.get("partitionBy"),
            "format": self._format,
        }

    def _create_hive_table(self, data: DataFrame, mode: str = None):
        _mode: str = mode or self._write_mode
        data.write.saveAsTable(
            self._full_table_address,
            mode=_mode,
            format=self._format,
            **self._save_args,
        )

    def _load(self) -> DataFrame:
        return _get_spark().read.table(self._full_table_address)

    def _save(self, data: DataFrame) -> None:
        self._validate_save(data)
        if self._write_mode == "upsert":
            # check if _table_pk is a subset of df columns
            if not set(self._table_pk) <= set(self._load().columns):
                raise DatasetError(
                    f"Columns {str(self._table_pk)} selected as primary key(s) not found in "
                    f"table {self._full_table_address}"
                )
            self._upsert_save(data=data)
        else:
            self._create_hive_table(data=data)

    def _upsert_save(self, data: DataFrame) -> None:
        if not self._exists() or self._load().rdd.isEmpty():
            self._create_hive_table(data=data, mode="overwrite")
        else:
            _tmp_colname = "tmp_colname"
            _tmp_row = "tmp_row"
            _w = Window.partitionBy(*self._table_pk).orderBy(col(_tmp_colname).desc())
            df_old = self._load().select("*", lit(1).alias(_tmp_colname))
            df_new = data.select("*", lit(2).alias(_tmp_colname))
            df_stacked = df_new.unionByName(df_old).select(
                "*", row_number().over(_w).alias(_tmp_row)
            )
            df_filtered = (
                df_stacked.filter(col(_tmp_row) == 1)
                .drop(_tmp_colname, _tmp_row)
                .checkpoint(eager=self._eager_checkpoint)
            )
            self._create_hive_table(data=df_filtered, mode="overwrite")

    def _validate_save(self, data: DataFrame):
        # do not validate when the table doesn't exist
        # or if the `write_mode` is set to overwrite
        if (not self._exists()) or self._write_mode == "overwrite":
            return
        hive_dtypes = set(self._load().dtypes)
        data_dtypes = set(data.dtypes)
        if data_dtypes != hive_dtypes:
            new_cols = data_dtypes - hive_dtypes
            missing_cols = hive_dtypes - data_dtypes
            raise DatasetError(
                f"Dataset does not match hive table schema.\n"
                f"Present on insert only: {sorted(new_cols)}\n"
                f"Present on schema only: {sorted(missing_cols)}"
            )

    def _exists(self) -> bool:
        return (
            _get_spark()
            ._jsparkSession.catalog()
            .tableExists(self._database, self._table)
        )

    def __getstate__(self) -> None:
        raise pickle.PicklingError(
            "PySpark datasets objects cannot be pickled "
            "or serialised as Python objects."
        )
