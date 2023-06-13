"""SparkStreamingDataSet to load and save a PySpark Streaming DataFrame."""
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, Dict

from kedro.io.core import AbstractDataSet
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from kedro_datasets.spark.spark_dataset import (
    SparkDataSet,
    _split_filepath,
    _strip_dbfs_prefix,
)


class SparkStreamingDataSet(AbstractDataSet):
    """``SparkStreamingDataSet`` loads data to Spark Streaming Dataframe objects.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

    .. code-block:: yaml

        raw.new_inventory:
          type: spark.SparkStreamingDataSet
          filepath: data/01_raw/stream/inventory/
          file_format: json
          save_args:
            output_mode: append
            checkpoint: data/04_checkpoint/raw_new_inventory
            header: True
          load_args:
            schema:
                filepath: data/01_raw/schema/inventory_schema.json
    """
    DEFAULT_LOAD_ARGS = {}  # type: Dict[str, Any]
    DEFAULT_SAVE_ARGS = {}  # type: Dict[str, Any]

    def __init__(
        self,
        filepath: str = "",
        file_format: str = "",
        save_args: Dict[str, Any] = None,
        load_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of SparkStreamingDataSet.
        Args:
            filepath: Filepath in POSIX format to a Spark dataframe. When using Databricks
                specify ``filepath``s starting with ``/dbfs/``. For message brokers such as
                Kafka and all filepath is not required.
            file_format: File format used during load and save
                operations. These are formats supported by the running
                SparkContext include parquet, csv, delta. For a list of supported
                formats please refer to Apache Spark documentation at
                https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
            load_args: Load args passed to Spark DataFrameReader load method.
                It is dependent on the selected file format. You can find
                a list of read options for each supported format
                in Spark DataFrame read documentation:
                https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html,
                Please note that a schema is mandatory for a streaming DataFrame
                if ``schemaInference`` is not True.
            save_args: Save args passed to Spark DataFrame write options.
                Similar to load_args this is dependent on the selected file
                format. You can pass ``mode`` and ``partitionBy`` to specify
                your overwrite mode and partitioning respectively. You can find
                a list of options for each format in Spark DataFrame
                write documentation:
                https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
        """
        self._file_format = file_format
        self._save_args = save_args
        self._load_args = load_args

        fs_prefix, filepath = _split_filepath(filepath)

        self._fs_prefix = fs_prefix
        self._filepath = PurePosixPath(filepath)

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        # Handle schema load argument
        self._schema = self._load_args.pop("schema", None)
        if self._schema is not None:
            if isinstance(self._schema, dict):
                self._schema = SparkDataSet._load_schema_from_file(self._schema)

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes attributes of the dataset."""
        return {
            "filepath": self._fs_prefix + str(self._filepath),
            "file_format": self._file_format,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    @staticmethod
    def _get_spark():
        return SparkSession.builder.getOrCreate()

    def _load(self) -> DataFrame:
        """Loads data from filepath.
        If the connector type is kafka then no file_path is required, schema needs to be
        seperated from load_args.
        Returns:
            Data from filepath as pyspark dataframe.
        """
        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._filepath))
        data_stream_reader = (
            self._get_spark()
            .readStream.schema(self._schema)
            .format(self._file_format)
            .options(**self._load_args)
        )
        return data_stream_reader.load(load_path)

    def _save(self, data: DataFrame) -> None:
        """Saves pyspark dataframe.
        Args:
            data: PySpark streaming dataframe for saving
        """
        save_path = _strip_dbfs_prefix(self._fs_prefix + str(self._filepath))
        output_constructor = data.writeStream.format(self._file_format)

        (
            output_constructor.option(
                "checkpointLocation", self._save_args.pop("checkpoint")
            )
            .option("path", save_path)
            .outputMode(self._save_args.pop("output_mode"))
            .options(**self._save_args)
            .start()
        )

    def _exists(self) -> bool:
        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._filepath))

        try:
            self._get_spark().readStream.schema(self._schema).load(
                load_path, self._file_format
            )
        except AnalysisException as exception:
            if (
                exception.desc.startswith("Path does not exist:")
                or "is not a Streaming data" in exception.desc
            ):
                return False
            raise
        return True
