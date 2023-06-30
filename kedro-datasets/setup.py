from itertools import chain

from setuptools import setup

# at least 1.3 to be able to use XMLDataSet and pandas integration with fsspec
PANDAS = "pandas>=1.3, <3.0"
SPARK = "pyspark>=2.2, <4.0"
HDFS = "hdfs>=2.5.8, <3.0"
S3FS = "s3fs>=0.3.0, <0.5"
POLARS = "polars~=0.17.0"
DELTA = "delta-spark~=1.2.1"


def _collect_requirements(requires):
    return sorted(set(chain.from_iterable(requires.values())))


api_require = {"api.APIDataSet": ["requests~=2.20"]}
biosequence_require = {"biosequence.BioSequenceDataSet": ["biopython~=1.73"]}
dask_require = {
    "dask.ParquetDataSet": ["dask[complete]~=2021.10", "triad>=0.6.7, <1.0"]
}
databricks_require = {"databricks.ManagedTableDataSet": [SPARK, PANDAS, DELTA]}
geopandas_require = {
    "geopandas.GeoJSONDataSet": ["geopandas>=0.6.0, <1.0", "pyproj~=3.0"]
}
holoviews_require = {"holoviews.HoloviewsWriter": ["holoviews~=1.13.0"]}
matplotlib_require = {"matplotlib.MatplotlibWriter": ["matplotlib>=3.0.3, <4.0"]}
networkx_require = {"networkx.NetworkXDataSet": ["networkx~=2.4"]}
pandas_require = {
    "pandas.CSVDataSet": [PANDAS],
    "pandas.ExcelDataSet": [PANDAS, "openpyxl>=3.0.6, <4.0"],
    "pandas.FeatherDataSet": [PANDAS],
    "pandas.GBQTableDataSet": [PANDAS, "pandas-gbq>=0.12.0, <0.18.0"],
    "pandas.GBQQueryDataSet": [PANDAS, "pandas-gbq>=0.12.0, <0.18.0"],
    "pandas.HDFDataSet": [
        PANDAS,
        "tables~=3.6.0; platform_system == 'Windows'",
        "tables~=3.6; platform_system != 'Windows'",
    ],
    "pandas.JSONDataSet": [PANDAS],
    "pandas.ParquetDataSet": [PANDAS, "pyarrow>=6.0"],
    "pandas.SQLTableDataSet": [PANDAS, "SQLAlchemy>=1.4, <3.0"],
    "pandas.SQLQueryDataSet": [PANDAS, "SQLAlchemy>=1.4, <3.0", "pyodbc~=4.0"],
    "pandas.XMLDataSet": [PANDAS, "lxml~=4.6"],
    "pandas.GenericDataSet": [PANDAS],
}
pickle_require = {"pickle.PickleDataSet": ["compress-pickle[lz4]~=2.1.0"]}
pillow_require = {"pillow.ImageDataSet": ["Pillow~=9.0"]}
plotly_require = {
    "plotly.PlotlyDataSet": [PANDAS, "plotly>=4.8.0, <6.0"],
    "plotly.JSONDataSet": ["plotly>=4.8.0, <6.0"],
}
polars_require = {"polars.CSVDataSet": [POLARS]}
redis_require = {"redis.PickleDataSet": ["redis~=4.1"]}
snowflake_require = {
    "snowflake.SnowparkTableDataSet": [
        "snowflake-snowpark-python~=1.0.0",
        "pyarrow~=8.0",
    ]
}
spark_require = {
    "spark.SparkDataSet": [SPARK, HDFS, S3FS],
    "spark.SparkHiveDataSet": [SPARK, HDFS, S3FS],
    "spark.SparkJDBCDataSet": [SPARK, HDFS, S3FS],
    "spark.DeltaTableDataSet": [SPARK, HDFS, S3FS, "delta-spark>=1.0, <3.0"],
}
svmlight_require = {"svmlight.SVMLightDataSet": ["scikit-learn~=1.0.2", "scipy~=1.7.3"]}
tensorflow_require = {
    "tensorflow.TensorFlowModelDataSet": [
        # currently only TensorFlow V2 supported for saving and loading.
        # V1 requires HDF5 and serialises differently
        "tensorflow~=2.0; platform_system != 'Darwin' or platform_machine != 'arm64'",
        # https://developer.apple.com/metal/tensorflow-plugin/
        "tensorflow-macos~=2.0; platform_system == 'Darwin' and platform_machine == 'arm64'",
    ]
}
video_require = {"video.VideoDataSet": ["opencv-python~=4.5.5.64"]}
yaml_require = {"yaml.YAMLDataSet": [PANDAS, "PyYAML>=4.2, <7.0"]}

extras_require = {
    "api": _collect_requirements(api_require),
    "biosequence": _collect_requirements(biosequence_require),
    "dask": _collect_requirements(dask_require),
    "databricks": _collect_requirements(databricks_require),
    "geopandas": _collect_requirements(geopandas_require),
    "holoviews": _collect_requirements(holoviews_require),
    "matplotlib": _collect_requirements(matplotlib_require),
    "networkx": _collect_requirements(networkx_require),
    "pandas": _collect_requirements(pandas_require),
    "pickle": _collect_requirements(pickle_require),
    "pillow": _collect_requirements(pillow_require),
    "plotly": _collect_requirements(plotly_require),
    "polars": _collect_requirements(polars_require),
    "redis": _collect_requirements(redis_require),
    "snowflake": _collect_requirements(snowflake_require),
    "spark": _collect_requirements(spark_require),
    "svmlight": _collect_requirements(svmlight_require),
    "tensorflow": _collect_requirements(tensorflow_require),
    "video": _collect_requirements(video_require),
    "yaml": _collect_requirements(yaml_require),
    **api_require,
    **biosequence_require,
    **dask_require,
    **databricks_require,
    **geopandas_require,
    **holoviews_require,
    **matplotlib_require,
    **networkx_require,
    **pandas_require,
    **pickle_require,
    **pillow_require,
    **plotly_require,
    **polars_require,
    **snowflake_require,
    **spark_require,
    **svmlight_require,
    **tensorflow_require,
    **video_require,
    **yaml_require,
}

extras_require["all"] = _collect_requirements(extras_require)
extras_require["docs"] = [
    "docutils==0.16",
    "sphinx~=3.4.3",
    "sphinx_rtd_theme==0.4.1",
    "nbsphinx==0.8.1",
    "nbstripout~=0.4",
    "sphinx-autodoc-typehints==1.11.1",
    "sphinx_copybutton==0.3.1",
    "ipykernel>=5.3, <7.0",
    "myst-parser~=1.0.0",
]

setup(
    extras_require=extras_require,
)
