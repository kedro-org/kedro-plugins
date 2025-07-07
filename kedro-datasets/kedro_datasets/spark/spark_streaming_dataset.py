"""SparkStreamingDataset to load and save a PySpark Streaming DataFrame."""
from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any

from kedro.io.core import AbstractDataset
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from kedro_datasets._utils.databricks_utils import split_filepath, strip_dbfs_prefix
from kedro_datasets._utils.spark_utils import get_spark
from kedro_datasets.spark.spark_dataset import SparkDataset


class SparkStreamingDataset(AbstractDataset):
    """``SparkStreamingDataset`` loads data to Spark Streaming Dataframe objects.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        raw.new_inventory:
          type: spark.SparkStreamingDataset
          filepath: data/01_raw/stream/inventory/
          file_format: json
          save_args:
            output_mode: append
            checkpoint: data/04_checkpoint/raw_new_inventory
            header: True
          load_args:
            schema:
                filepath: data/01_raw/schema/inventory_schema.json
        ```

    """

    DEFAULT_LOAD_ARGS = {}  # type: dict[str, Any]
    DEFAULT_SAVE_ARGS = {}  # type: dict[str, Any]

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str = "",
        file_format: str = "",
        save_args: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of SparkStreamingDataset.

        Args:
            filepath: Filepath in POSIX format to a Spark dataframe. When using Databricks
                specify ``filepath``s starting with ``/dbfs/``. For message brokers such as
                Kafka and all filepath is not required.
            file_format: File format used during load and save operations.
                These are formats supported by the running SparkContext including parquet,
                csv, and delta. For a list of supported formats please refer to the Apache
                Spark documentation at
                https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
            load_args: Load args passed to Spark DataFrameReader load method.
                It is dependent on the selected file format. You can find a list of read options
                for each selected format in Spark DataFrame read documentation, see
                https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html.
                Please note that a schema is mandatory for a streaming DataFrame
                if ``schemaInference`` is not True.
            save_args: Save args passed to Spark DataFrameReader write options.
                Similar to load_args, this is dependent on the selected file format. You can pass
                ``mode`` and ``partitionBy`` to specify your overwrite mode and partitioning
                respectively. You can find a list of options for each selected format in
                Spark DataFrame write documentation, see
                https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        self._file_format = file_format
        self._save_args = save_args
        self._load_args = load_args
        self.metadata = metadata

        fs_prefix, filepath = split_filepath(filepath)

        self._fs_prefix = fs_prefix
        self._filepath = PurePosixPath(filepath)

        # Handle default load and save arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}

        # Handle schema load argument
        self._schema = self._load_args.pop("schema", None)
        if self._schema is not None:
            if isinstance(self._schema, dict):
                self._schema = SparkDataset._load_schema_from_file(self._schema)

    def _describe(self) -> dict[str, Any]:
        """Returns a dict that describes attributes of the dataset."""
        return {
            "filepath": self._fs_prefix + str(self._filepath),
            "file_format": self._file_format,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    def load(self) -> DataFrame:
        """Loads data from filepath.
        If the connector type is kafka then no file_path is required, schema needs to be
        seperated from load_args.
        Returns:
            Data from filepath as pyspark dataframe.
        """
        load_path = strip_dbfs_prefix(self._fs_prefix + str(self._filepath))
        data_stream_reader = (
            get_spark()
            .readStream.schema(self._schema)
            .format(self._file_format)
            .options(**self._load_args)
        )
        return data_stream_reader.load(load_path)

    def save(self, data: DataFrame) -> None:
        """Saves pyspark dataframe.
        Args:
            data: PySpark streaming dataframe for saving
        """
        save_path = strip_dbfs_prefix(self._fs_prefix + str(self._filepath))
        output_constructor = data.writeStream.format(self._file_format)
        output_mode = (
            self._save_args.pop("output_mode", None) if self._save_args else None
        )
        checkpoint = (
            self._save_args.pop("checkpoint", None) if self._save_args else None
        )
        (
            output_constructor.option("checkpointLocation", checkpoint)
            .option("path", save_path)
            .outputMode(str(output_mode))
            .options(**self._save_args or {})
            .start()
        )

    def _exists(self) -> bool:
        load_path = strip_dbfs_prefix(self._fs_prefix + str(self._filepath))

        try:
            get_spark().readStream.schema(self._schema).load(
                load_path, self._file_format
            )
        except AnalysisException as exception:
            # `AnalysisException.desc` is deprecated with pyspark >= 3.4
            message = exception.desc if hasattr(exception, "desc") else str(exception)
            if (
                "Path does not exist:" in message
                or "is not a Streaming data" in message
            ):
                return False
            raise
        return True
