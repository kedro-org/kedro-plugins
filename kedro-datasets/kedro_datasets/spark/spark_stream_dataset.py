"""SparkStreamingDataSet to load and save a PySpark Streaming DataFrame."""
from typing import Any, Dict

import pyspark
import yaml
from kedro.io import AbstractDataSet
from pyspark import SparkConf
from pyspark.sql import SparkSession
from yaml.loader import SafeLoader


class SparkStreamingDataSet(AbstractDataSet):
    """``SparkStreamingDataSet`` loads data into Spark Streaming Dataframe objects.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:
    .. code-block:: yaml

        raw.new_inventory:
            type: streaming.extras.datasets.spark_streaming_dataset.SparkStreamingDataSet
            filepath: data/01_raw/stream/inventory/
            file_format: json

        int.new_inventory:
            type: streaming.extras.datasets.spark_streaming_dataset.SparkStreamingDataSet
            filepath: data/02_intermediate/inventory/
            file_format: csv
            save_args:
                output_mode: append
                checkpoint: data/04_checkpoint/int_new_inventory
                header: True
            load_args:
                header: True

    """

    def __init__(
        self,
        filepath: str = "",
        file_format: str = "",
        save_args: Dict[str, str] = {},
        load_args: Dict[str, str] = {},
    ):
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
                https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
            save_args: Save args passed to Spark DataFrame write options.
                Similar to load_args this is dependent on the selected file
                format. You can pass ``mode`` and ``partitionBy`` to specify
                your overwrite mode and partitioning respectively. You can find
                a list of options for each format in Spark DataFrame
                write documentation:
                https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
        """
        self._filepath_ = filepath
        self.file_format = file_format
        self._save_args = save_args
        self._load_args = load_args
        self.output_format = [
            "kafka"
        ]  # message broker formats, such as Kafka, Kinesis, and others, require different methods for loading and saving.

        # read spark configuration from spark yml file and create a spark context
        with open("conf/base/spark.yml") as f:
            self.parameters = yaml.load(f, Loader=SafeLoader)
        self.spark_conf = SparkConf().setAll(self.parameters.items())

        # Initialise the spark session
        self.spark_session_conf = SparkSession.builder.config(conf=self.spark_conf)
        self.spark = self.spark_session_conf.getOrCreate()

    def _load(self) -> pyspark.sql.DataFrame:
        """Loads data from filepath.
        If the connector type is kafka then no file_path is required

        Returns:
            Data from filepath as pyspark dataframe.
        """
        input_constructor = self.spark.readStream.format(self.file_format).options(
            **self._load_args
        )
        return (
            input_constructor.load()
            if self.file_format
            in self.output_format  # if the connector type is message broker
            else input_constructor.load(self._filepath_)
        )

    def _save(self, data: pyspark.sql.DataFrame) -> None:
        """Saves pyspark dataframe.

        Args:
            data: PySpark streaming dataframe for saving

        """

        output_constructor = data.writeStream.format(self.file_format)

        # for message brokers path is not needed
        if self.file_format not in self.output_format:
            output_constructor = output_constructor.option("path", self._filepath_)

        (
            output_constructor.option(
                "checkpointLocation", self._save_args.pop("checkpoint")
            )
            .outputMode(self._save_args.pop("output_mode"))
            .options(**self._save_args)
            .start()
        )

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes attributes of the dataset."""
        return None
