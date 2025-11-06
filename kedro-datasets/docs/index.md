# kedro_datasets

::: kedro_datasets

## Dataset Classes

Name | Description
------|-------------
[api.APIDataset](api/kedro_datasets/api.APIDataset.md) | ``APIDataset`` loads/saves data from/to HTTP(S) APIs. It uses the python requests library: <https://requests.readthedocs.io/en/latest/>
[biosequence.BioSequenceDataset](api/kedro_datasets/biosequence.BioSequenceDataset.md) | ``BioSequenceDataset`` loads and saves data to a sequence file.
[dask.CSVDataset](api/kedro_datasets/dask.CSVDataset.md) | ``CSVDataset`` loads and saves data to comma-separated value file(s). It uses Dask remote data services to handle the corresponding load and save operations.
[dask.ParquetDataset](api/kedro_datasets/dask.ParquetDataset.md) | ``ParquetDataset`` loads and saves data to parquet file(s). It uses Dask remote data services to handle the corresponding load and save operations.
[databricks.ManagedTableDataset](api/kedro_datasets/databricks.ManagedTableDataset.md) | ``ManagedTableDataset`` loads and saves data into managed delta tables in Databricks.
[email.EmailMessageDataset](api/kedro_datasets/email.EmailMessageDataset.md) | ``EmailMessageDataset`` loads/saves an email message from/to a file using an underlying filesystem (e.g.: local, S3, GCS). It uses the ``email`` package in the standard library to manage email messages.
[geopandas.GenericDataset](api/kedro_datasets/geopandas.GenericDataset.md) | ``GenericDataset`` loads/saves data to a file using an underlying filesystem (eg: local, S3, GCS). The underlying functionality is supported by geopandas, so it supports all allowed geopandas (pandas) options for loading and saving files.
[holoviews.HoloviewsWriter](api/kedro_datasets/holoviews.HoloviewsWriter.md) | ``HoloviewsWriter`` saves Holoviews objects to image file(s) in an underlying filesystem (e.g. local, S3, GCS).
[huggingface.HFDataset](api/kedro_datasets/huggingface.HFDataset.md) | ``HFDataset`` loads Hugging Face datasets using the `datasets` library.
[huggingface.HFTransformerPipelineDataset](api/kedro_datasets/huggingface.HFTransformerPipelineDataset.md) | ``HFTransformerPipelineDataset`` loads pretrained Hugging Face transformers using the `transformers` library.
[ibis.FileDataset](api/kedro_datasets/ibis.FileDataset.md) | ``FileDataset`` loads/saves data from/to a specified file format.
[ibis.TableDataset](api/kedro_datasets/ibis.TableDataset.md) | `TableDataset` loads/saves data from/to Ibis table expressions.
[json.JSONDataset](api/kedro_datasets/json.JSONDataset.md) | ``JSONDataset`` loads/saves data from/to a JSON file using an underlying filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file.
[matlab.MatlabDataset](api/kedro_datasets/matlab.MatlabDataset.md) | `MatlabDataSet` loads and saves data from/to a MATLAB file using scipy.io.
[matplotlib.MatplotlibDataset](api/kedro_datasets/matplotlib.MatplotlibDataset.md) | ``MatplotlibDataset`` saves one or more Matplotlib objects as image files to an underlying filesystem (e.g. local, S3, GCS).
[networkx.GMLDataset](api/kedro_datasets/networkx.GMLDataset.md) | ``GMLDataset`` loads and saves graphs to a GML file using an underlying filesystem (e.g.: local, S3, GCS). NetworkX is used to create GML data.
[networkx.GraphMLDataset](api/kedro_datasets/networkx.GraphMLDataset.md) | ``GraphMLDataset`` loads and saves graphs to a GraphML file using an underlying filesystem (e.g.: local, S3, GCS). NetworkX is used to create GraphML data.
[networkx.JSONDataset](api/kedro_datasets/networkx.JSONDataset.md) | NetworkX ``JSONDataset`` loads and saves graphs to a JSON file using an underlying filesystem (e.g.: local, S3, GCS). NetworkX is used to create JSON data.
[openxml.DocxDataset](api/kedro_datasets/openxml.DocxDataset.md) | ``DocxDataset`` loads/saves data from/to a .docx file using an underlying filesystem (e.g.: local, S3, GCS). It uses python-docx to handle the .docx file.
[openxml.PptxDataset](api/kedro_datasets/openxml.PptxDataset.md) | ``PptxDataset`` loads/saves data from/to a .pptx file using an underlying filesystem (e.g.: local, S3, GCS). It uses python-pptx to handle the .pptx file.
[pandas.CSVDataset](api/kedro_datasets/pandas.CSVDataset.md) | A dataset that loads and saves data to/from CSV files using pandas.
[pandas.DeltaTableDataset](api/kedro_datasets/pandas.DeltaTableDataset.md) | ``DeltaTableDataset`` loads/saves delta tables from/to a filesystem (e.g.: local, S3, GCS), Databricks unity catalog and AWS Glue catalog respectively. It handles load and save using a pandas dataframe.
[pandas.ExcelDataset](api/kedro_datasets/pandas.ExcelDataset.md) | ``ExcelDataset`` loads/saves data from/to a Excel file using an underlying filesystem (e.g.: local, S3, GCS). It uses pandas to handle the Excel file.
[pandas.FeatherDataset](api/kedro_datasets/pandas.FeatherDataset.md) | A dataset that loads and saves data to/from Feather files using pandas.
[pandas.GBQQueryDataset](api/kedro_datasets/pandas.GBQQueryDataset.md) | A dataset that loads data from a provided SQL query in Google BigQuery using pandas-gbq. It is read-only.
[pandas.GBQTableDataset](api/kedro_datasets/pandas.GBQTableDataset.md) | A dataset that loads and saves data to/from Google BigQuery tables using pandas-gbq.
[pandas.GenericDataset](api/kedro_datasets/pandas.GenericDataset.md) | ``GenericDataset`` loads/saves data from/to a data file using an underlying filesystem (e.g.: local, S3, GCS). It uses pandas to handle the type of read/write target.
[pandas.HDFDataset](api/kedro_datasets/pandas.HDFDataset.md) | A dataset that loads and saves data to/from HDF files using pandas.
[pandas.JSONDataset](api/kedro_datasets/pandas.JSONDataset.md) | A dataset that loads and saves data to/from JSON files using pandas.
[pandas.ParquetDataset](api/kedro_datasets/pandas.ParquetDataset.md) | A dataset that loads and saves data to/from Parquet files using pandas.
[pandas.SQLQueryDataset](api/kedro_datasets/pandas.SQLQueryDataset.md) | A dataset that loads data from a provided SQL query using pandas. It is read-only.
[pandas.SQLTableDataset](api/kedro_datasets/pandas.SQLTableDataset.md) | A dataset that loads data from a SQL table and saves a pandas DataFrame to a table.
[pandas.XMLDataset](api/kedro_datasets/pandas.XMLDataset.md) | A dataset that loads and saves data to/from XML files using pandas.
[partitions.IncrementalDataset](api/kedro_datasets/partitions.IncrementalDataset.md) | ``IncrementalDataset`` inherits from ``PartitionedDataset``, which loads and saves partitioned file-like data using the underlying dataset definition.
[partitions.PartitionedDataset](api/kedro_datasets/partitions.PartitionedDataset.md) | ``PartitionedDataset`` loads and saves partitioned file-like data using the underlying dataset definition. It also uses `fsspec` for filesystem level operations.
[pickle.PickleDataset](api/kedro_datasets/pickle.PickleDataset.md) | ``PickleDataset`` loads/saves data from/to a Pickle file using an underlying filesystem (e.g.: local, S3, GCS). The underlying functionality is supported by the specified backend library passed in (defaults to the ``pickle`` library), so it supports all allowed options for loading and saving pickle files.
[pillow.ImageDataset](api/kedro_datasets/pillow.ImageDataset.md) | ``ImageDataset`` loads/saves image data as `numpy` from an underlying filesystem (e.g.: local, S3, GCS). It uses Pillow to handle image file.
[plotly.HTMLDataset](api/kedro_datasets/plotly.HTMLDataset.md) | ``HTMLDataset`` saves a plotly figure to an HTML file using an underlying filesystem (e.g.: local, S3, GCS).
[plotly.JSONDataset](api/kedro_datasets/plotly.JSONDataset.md) | ``JSONDataset`` loads/saves a plotly figure from/to a JSON file using an underlying filesystem (e.g.: local, S3, GCS).
[plotly.PlotlyDataset](api/kedro_datasets/plotly.PlotlyDataset.md) | ``PlotlyDataset`` generates a plot from a pandas DataFrame and saves it to a JSON file using an underlying filesystem (e.g.: local, S3, GCS). It loads the JSON into a plotly figure.
[polars.CSVDataset](api/kedro_datasets/polars.CSVDataset.md) | ``CSVDataset`` loads/saves data from/to a CSV file using an underlying filesystem (e.g.: local, S3, GCS). It uses polars to handle the CSV file.
[polars.EagerPolarsDataset](api/kedro_datasets/polars.EagerPolarsDataset.md) | ``EagerPolarsDataset`` loads/saves data from/to a data file using an filesystem (e.g.: local, S3, GCS). It uses polars to handle the type of read/write target.
[polars.LazyPolarsDataset](api/kedro_datasets/polars.LazyPolarsDataset.md) | ``LazyPolarsDataset`` loads/saves data from/to a data file using an underlying filesystem (e.g.: local, S3, GCS). It uses polars to handle the type of read/write target.
[redis.PickleDataset](api/kedro_datasets/redis.PickleDataset.md) | ``PickleDataset`` loads/saves data from/to a Redis database. The underlying functionality is supported by the redis library, so it supports all allowed options for instantiating the redis app ``from_url`` and setting a value.
[snowflake.SnowparkTableDataset](api/kedro_datasets/snowflake.SnowparkTableDataset.md) | ``SnowparkTableDataset`` loads and saves Snowpark DataFrames. As of October 2024, the Snowpark connector works with Python 3.9, 3.10, and 3.11. Python 3.12 is not supported yet.
[spark.DeltaTableDataset](api/kedro_datasets/spark.DeltaTableDataset.md) | ``DeltaTableDataset`` loads data into DeltaTable objects.
[spark.GBQQueryDataset](api/kedro_datasets/spark.GBQQueryDataset.md) | ``GBQQueryDataset`` loads data from Google BigQuery with a SQL query using BigQuery Spark connector.
[spark.SparkDataset](api/kedro_datasets/spark.SparkDataset.md) | ``SparkDataset`` loads and saves Spark dataframes.
[spark.SparkHiveDataset](api/kedro_datasets/spark.SparkHiveDataset.md) |``SparkHiveDataset`` loads and saves Spark dataframes stored on Hive.
[spark.SparkJDBCDataset](api/kedro_datasets/spark.SparkJDBCDataset.md) |``SparkJDBCDataset`` loads data from a database table accessible via JDBC URL url and connection properties and saves the content of a PySpark DataFrame to an external database table via JDBC.
[spark.SparkStreamingDataset](api/kedro_datasets/spark.SparkStreamingDataset.md) |``SparkStreamingDataset`` loads data to Spark Streaming Dataframe objects.
[svmlight.SVMLightDataset](api/kedro_datasets/svmlight.SVMLightDataset.md) | ``SVMLightDataset`` loads/saves data from/to a svmlight/libsvm file using an underlying filesystem (e.g.: local, S3, GCS). It uses sklearn functions ``dump_svmlight_file`` to save and ``load_svmlight_file`` to load a file.
[tensorflow.TensorFlowModelDataset](api/kedro_datasets/tensorflow.TensorFlowModelDataset.md) | ``TensorFlowModelDataset`` loads and saves TensorFlow models. The underlying functionality is supported by, and passes input arguments through to, TensorFlow 2.X load_model and save_model methods.
[text.TextDataset](api/kedro_datasets/text.TextDataset.md) | ``TextDataset`` loads/saves data from/to a text file using an underlying filesystem (e.g.: local, S3, GCS).
[yaml.YAMLDataset](api/kedro_datasets/yaml.YAMLDataset.md) | ``YAMLDataset`` loads/saves data from/to a YAML file using an underlying filesystem (e.g.: local, S3, GCS). It uses PyYAML to handle the YAML file.
