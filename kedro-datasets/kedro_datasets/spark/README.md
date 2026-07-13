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

# SparkDatasetV2

`SparkDatasetV2` is a modernised Spark dataset with the following improvements over `SparkDataset`:

- **No hooks required** - Spark session is created automatically inside the dataset
- **Pandas auto-conversion** - Save Pandas DataFrames directly, they're converted to Spark automatically
- **Databricks Connect support** - Works with `DatabricksSession.builder.serverless(True)`
- **Spark Connect support** - Works with Spark 3.4+ remote connections via `SPARK_REMOTE`
- **Simplified dependencies** - Choose what you need: `spark-local`, `spark-s3`, `spark-gcs`, etc.

## Basic Usage
```yaml
# catalog.yml
my_data:
  type: spark.SparkDatasetV2
  filepath: data/output.parquet
```

## Saving Pandas DataFrames
```python
# nodes.py
import pandas as pd

def create_data() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2, 3]})

# SparkDatasetV2 automatically converts Pandas to Spark on save!
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` + `DATABRICKS_TOKEN` | Enables Databricks Connect |
| `SPARK_REMOTE` | Enables Spark Connect (e.g., `sc://localhost:15002`) |

## Supported Paths

- Local: `data/output.parquet`
- S3: `s3://bucket/path/data.parquet` (auto-converts to `s3a://`)
- GCS: `gs://bucket/path/data.parquet`
- Azure: `abfs://container@account.dfs.core.windows.net/path`
- Databricks DBFS: `/dbfs/path` or `dbfs:/path`
- Unity Catalog: `/Volumes/catalog/schema/volume/path`
