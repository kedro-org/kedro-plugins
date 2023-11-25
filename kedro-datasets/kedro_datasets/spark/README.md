# Spark Streaming

``SparkStreamingDataset`` loads and saves data to streaming DataFrames.
See [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) for details.

To work with multiple streaming nodes, 2 hooks are required for:

- Integrating PySpark, see [Build a Kedro pipeline with PySpark](https://docs.kedro.org/en/stable/integrations/pyspark_integration.html) for details
- Running streaming query without termination unless exception

#### Supported file formats

Supported file formats are:

- Text
- CSV
- JSON
- ORC
- Parquet

#### Example SparkStreamsHook:

```python
from kedro.framework.hooks import hook_impl
from pyspark.sql import SparkSession


class SparkStreamsHook:
    @hook_impl
    def after_pipeline_run(self) -> None:
        """Starts a spark streaming await session
        once the pipeline reaches the last node
        """

        spark = SparkSession.builder.getOrCreate()
        spark.streams.awaitAnyTermination()
```
To make the application work with Kafka format, the respective spark configuration needs to be added to``conf/base/spark.yml``.

#### Example spark.yml:

```yaml
spark.driver.maxResultSize: 3g
spark.scheduler.mode: FAIR

```
