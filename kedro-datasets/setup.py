from setuptools import setup

extras_require = {
    # Base requirements
    "pandas-base": ["pandas>=1.3, <3.0"],
    "plotly-base": ["plotly>=4.8.0, <6.0"],
    "hdfs-base": ["hdfs>=2.5.8, <3.0"],
    "s3fs-base": ["s3fs>=0.3.0, <0.5"],
    "delta-base": ["delta-spark~=1.2.1"],
    "spark-base": ["pyspark>=2.2, <4.0"],
    "polars-base": ["polars~=0.17.0"],

    "api": ["kedro-datasets[api.APIDataSet]"],
    "api.APIDataSet": ["requests~=2.20"],
    "biosequence": ["kedro-datasets[biosequence.BioSequenceDataSet]"],
    "biosequence.BioSequenceDataSet": ["biopython~=1.73"],
    "dask": ["kedro-datasets[dask.ParquetDataSet]"],
    "dask.ParquetDataSet": ["dask[complete]~=2021.10", "triad>=0.6.7, <1.0"],
    "databricks": ["kedro-datasets[databricks.ManagedTableDataSet]"],
    "databricks.ManagedTableDataSet": ["kedro-datasets[spark-base,pandas-base,delta-base]"],
    "geopandas": ["kedro-datasets[geopandas.GeoJSONDataSet]"],
    "geopandas.GeoJSONDataSet": ["geopandas>=0.6.0, <1.0", "pyproj~=3.0"],
    "holoviews": ["kedro-datasets[holoviews.HoloviewsWriter]"],
    "holoviews.HoloviewsWriter": ["holoviews~=1.13.0"],
    "matplotlib": ["kedro-datasets[matplotlib.MatplotlibWriter]"],
    "matplotlib.MatplotlibWriter": ["matplotlib>=3.0.3, <4.0"],
    "networkx": ["kedro-datasets[networkx.NetworkXDataSet]"],
    "networkx.NetworkXDataSet": ["networkx~=2.4"],
    "pandas": [
        "kedro-datasets[pandas.CSVDataSet,pandas.ExcelDataSet,pandas.FeatherDataSet,pandas.GBQTableDataSet,pandas.GBQQueryDataSet,pandas.HDFDataSet,pandas.JSONDataSet,pandas.ParquetDataSet,pandas.SQLTableDataSet,pandas.SQLQueryDataSet,pandas.XMLDataSet,pandas.GenericDataSet]"
    ],
    "pandas.CSVDataSet": ["kedro-datasets[pandas-base]"],
    "pandas.ExcelDataSet": ["kedro-datasets[pandas-base]", "openpyxl>=3.0.6, <4.0"],
    "pandas.FeatherDataSet": ["kedro-datasets[pandas-base]"],
    "pandas.GBQTableDataSet": [
        "kedro-datasets[pandas-base]",
        "pandas-gbq>=0.12.0, <0.18.0",
    ],
    "pandas.GBQQueryDataSet": [
        "kedro-datasets[pandas-base]",
        "pandas-gbq>=0.12.0, <0.18.0",
    ],
    "pandas.HDFDataSet": [
        "kedro-datasets[pandas-base]",
        "tables~=3.6.0; platform_system == 'Windows'",
        "tables~=3.6; platform_system != 'Windows'",
    ],
    "pandas.JSONDataSet": ["kedro-datasets[pandas-base]"],
    "pandas.ParquetDataSet": ["kedro-datasets[pandas-base]", "pyarrow>=6.0"],
    "pandas.SQLTableDataSet": ["kedro-datasets[pandas-base]", "SQLAlchemy>=1.4, <3.0"],
    "pandas.SQLQueryDataSet": [
        "kedro-datasets[pandas-base]",
        "SQLAlchemy>=1.4, <3.0",
        "pyodbc~=4.0",
    ],
    "pandas.XMLDataSet": ["kedro-datasets[pandas-base]", "lxml~=4.6"],
    "pandas.GenericDataSet": ["kedro-datasets[pandas-base]"],
    "pickle": ["kedro-datasets[pickle.PickleDataSet]"],
    "pickle.PickleDataSet": ["compress-pickle[lz4]~=2.1.0"],
    "pillow": ["kedro-datasets[pillow.ImageDataSet]"],
    "pillow.ImageDataSet": ["Pillow~=9.0"],
    "plotly": ["kedro-datasets[plotly.PlotlyDataSet,plotly.JSONDataSet]"],
    "plotly.PlotlyDataSet": ["kedro-datasets[pandas-base,plotly-base]"],
    "plotly.JSONDataSet": ["kedro-datasets[plotly-base]"],
    "polars": ["kedro-datasets[polars.CSVDataSet]"],
    "polars.CSVDataSet": ["kedro-datasets[polars-base]"],
    "redis": ["kedro-datasets[redis.PickleDataSet]"],
    "redis.PickleDataSet": ["redis~=4.1"],
    "snowflake": ["kedro-datasets[snowflake.SnowparkTableDataSet]"],
    "snowflake.SnowparkTableDataSet": [
        "snowflake-snowpark-python~=1.0.0",
        "pyarrow~=8.0",
    ],
    "spark": [
        "kedro-datasets[spark.SparkDataSet,spark.SparkHiveDataSet,spark.SparkJDBCDataSet,spark.DeltaTableDataSet]"
    ],
    "spark.SparkDataSet": ["kedro-datasets[spark-base,hdfs-base,s3fs-base]"],
    "spark.SparkHiveDataSet": ["kedro-datasets[spark-base,hdfs-base,s3fs-base]"],
    "spark.SparkJDBCDataSet": ["kedro-datasets[spark-base,hdfs-base,s3fs-base]"],
    "spark.DeltaTableDataSet": [
        "kedro-datasets[spark-base,hdfs-base,s3fs-base]",
        "delta-spark>=1.0, <3.0",
    ],
    "svmlight": ["kedro-datasets[svmlight.SVMLightDataSet]"],
    "svmlight.SVMLightDataSet": ["scikit-learn~=1.0.2", "scipy~=1.7.3"],
    "tensorflow": ["kedro-datasets[tensorflow.TensorFlowModelDataSet]"],
    "tensorflow.TensorFlowModelDataSet": [
        # currently only TensorFlow V2 supported for saving and loading.
        # V1 requires HDF5 and serialises differently
        "tensorflow~=2.0; platform_system != 'Darwin' or platform_machine != 'arm64'",
        # https://developer.apple.com/metal/tensorflow-plugin/
        "tensorflow-macos~=2.0; platform_system == 'Darwin' and platform_machine == 'arm64'",
    ],
    "video": ["kedro-datasets[video.VideoDataSet]"],
    "video.VideoDataSet": ["opencv-python~=4.5.5.64"],
    "yaml": ["kedro-datasets[yaml.YAMLDataSet]"],
    "yaml.YAMLDataSet": ["kedro-datasets[pandas-base]", "PyYAML>=4.2, <7.0"],
    "all": [
        "kedro-datasets[api,biosequence,dask,databricks,geopandas,holoviews,matplotlib,networkx,pickle,pillow,plotly,polars,redis,snowflake,spark,svmlight,tensorflow,video,yaml]"
    ],
    "docs": [
        # docutils>=0.17 changed the HTML
        # see https://github.com/readthedocs/sphinx_rtd_theme/issues/1115
        "docutils==0.16",
        "sphinx~=5.3.0",
        "sphinx_rtd_theme==1.2.0",
        # Regression on sphinx-autodoc-typehints 1.21
        # that creates some problematic docstrings
        "sphinx-autodoc-typehints==1.20.2",
        "sphinx_copybutton==0.3.1",
        "sphinx-notfound-page",
        "ipykernel>=5.3, <7.0",
        "sphinxcontrib-mermaid~=0.7.1",
        "myst-parser~=1.0.0",
        "Jinja2<3.1.0",
    ],
    "test": [
        "adlfs>=2021.7.1, <=2022.2",
        "bandit>=1.6.2, <2.0",
        "behave==1.2.6",
        "biopython~=1.73",
        "blacken-docs==1.9.2",
        "black~=22.0",
        "compress-pickle[lz4]~=1.2.0",
        "coverage[toml]",
        "dask[complete]",
        "delta-spark~=1.2.1",
        # 1.2.0 has a bug that breaks some of our tests: https://github.com/delta-io/delta/issues/1070
        "dill~=0.3.1",
        "filelock>=3.4.0, <4.0",
        "gcsfs>=2021.4, <=2022.1",
        "geopandas>=0.6.0, <1.0",
        "hdfs>=2.5.8, <3.0",
        "holoviews~=1.13.0",
        "import-linter[toml]==1.2.6",
        "ipython>=7.31.1, <8.0",
        "Jinja2<3.1.0",
        "joblib>=0.14",
        "jupyterlab~=3.0",
        "jupyter~=1.0",
        "lxml~=4.6",
        "matplotlib>=3.0.3, <3.4; python_version < '3.10'",  # 3.4.0 breaks holoviews
        "matplotlib>=3.5, <3.6; python_version == '3.10'",
        "memory_profiler>=0.50.0, <1.0",
        "moto==1.3.7; python_version < '3.10'",
        "moto==3.0.4; python_version == '3.10'",
        "networkx~=2.4",
        "opencv-python~=4.5.5.64",
        "openpyxl>=3.0.3, <4.0",
        "pandas-gbq>=0.12.0, <0.18.0",
        "pandas>=1.3, <2",  # 1.3 for read_xml/to_xml, <2 for compatibility with Spark < 3.4
        "Pillow~=9.0",
        "plotly>=4.8.0, <6.0",
        "polars~=0.15.13",
        "pre-commit>=2.9.2, <3.0",  # The hook `mypy` requires pre-commit version 2.9.2.
        "psutil==5.8.0",
        "pyarrow~=8.0",
        "pylint>=2.5.2, <3.0",
        "pyodbc~=4.0.35",
        "pyproj~=3.0",
        "pyspark>=2.2, <4.0",
        "pytest-cov~=3.0",
        "pytest-mock>=1.7.1, <2.0",
        "pytest-xdist[psutil]~=2.2.1",
        "pytest~=7.2",
        "redis~=4.1",
        "requests-mock~=1.6",
        "requests~=2.20",
        "s3fs>=0.3.0, <0.5",  # Needs to be at least 0.3.0 to make use of `cachable` attribute on S3FileSystem.
        "scikit-learn~=1.0.2",
        "scipy~=1.7.3",
        "snowflake-snowpark-python~=1.0.0; python_version == '3.8'",
        "SQLAlchemy>=1.4, <3.0",
        # The `Inspector.has_table()` method replaces the `Engine.has_table()` method in version 1.4.
        "tables~=3.7",
        "tensorflow-macos~=2.0; platform_system == 'Darwin' and platform_machine == 'arm64'",
        "tensorflow~=2.0; platform_system != 'Darwin' or platform_machine != 'arm64'",
        "triad>=0.6.7, <1.0",
        "trufflehog~=2.1",
        "xlsxwriter~=1.0",
    ],
}

setup(
    extras_require=extras_require,
)
