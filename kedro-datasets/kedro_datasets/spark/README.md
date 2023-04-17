# Spark Streaming

``SparkStreamingDataSet`` loads and saves data to streaming DataFrames.
See [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) for details.

To work with multiple streaming nodes, 2 hook are required for: 
    - Integrating Pyspark, see [Build a Kedro pipeline with PySpark](https://docs.kedro.org/en/stable/tools_integration/pyspark.html) for details
    - Running streaming query without termination unless exception

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
To make the application work with kafka format, respective spark configuration need to be added in ``conf/base/spark.yml``.

#### Example spark.yml:

```yaml
spark.driver.maxResultSize: 3g
spark.scheduler.mode: FAIR
spark.sql.streaming.schemaInference: True
spark.streaming.stopGracefullyOnShutdown: true # graceful shutdown guarantees (under some conditions, listed below in the post) that all received data is processed before destroying Spark context
spark.sql.streaming.stateStore.stateSchemaCheck: false # since schema is not mentioned explicitly
spark.jars.packages: org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 # spark and kafka configuraton for reading kafka files (not required if kafka is not used)

```
