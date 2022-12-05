import re
from codecs import open
from itertools import chain
from os import path

from setuptools import find_packages, setup

name = "kedro-datasets"
here = path.abspath(path.dirname(__file__))

# at least 1.3 to be able to use XMLDataSet and pandas integration with fsspec
PANDAS = "pandas~=1.3"
SPARK = "pyspark>=2.2, <4.0"
HDFS = "hdfs>=2.5.8, <3.0"
S3FS = "s3fs>=0.3.0, <0.5"

with open("requirements.txt", "r", encoding="utf-8") as f:
    install_requires = [x.strip() for x in f if x.strip()]

with open("test_requirements.txt", "r", encoding="utf-8") as f:
    tests_require = [x.strip() for x in f if x.strip() and not x.startswith("-r")]

# get package version
package_name = name.replace("-", "_")
with open(path.join(here, package_name, "__init__.py"), encoding="utf-8") as f:
    version = re.search(r'__version__ = ["\']([^"\']+)', f.read()).group(1)

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    readme = f.read()


def _collect_requirements(requires):
    return sorted(set(chain.from_iterable(requires.values())))


api_require = {"api.APIDataSet": ["requests~=2.20"]}
biosequence_require = {"biosequence.BioSequenceDataSet": ["biopython~=1.73"]}
dask_require = {"dask.ParquetDataSet": ["dask[complete]~=2021.10", "triad>=0.6.7, <1.0"]}
geopandas_require = {
    "geopandas.GeoJSONDataSet": ["geopandas>=0.6.0, <1.0", "pyproj~=3.0"]
}
matplotlib_require = {"matplotlib.MatplotlibWriter": ["matplotlib>=3.0.3, <4.0"]}
holoviews_require = {"holoviews.HoloviewsWriter": ["holoviews~=1.13.0"]}
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
    "pandas.ParquetDataSet": [PANDAS, "pyarrow>=1.0, <7.0"],
    "pandas.SQLTableDataSet": [PANDAS, "SQLAlchemy~=1.2"],
    "pandas.SQLQueryDataSet": [PANDAS, "SQLAlchemy~=1.2"],
    "pandas.XMLDataSet": [PANDAS, "lxml~=4.6"],
    "pandas.GenericDataSet": [PANDAS],
}
pillow_require = {"pillow.ImageDataSet": ["Pillow~=9.0"]}
video_require = {
    "video.VideoDataSet": ["opencv-python~=4.5.5.64"]
}
plotly_require = {
    "plotly.PlotlyDataSet": [PANDAS, "plotly>=4.8.0, <6.0"],
    "plotly.JSONDataSet": ["plotly>=4.8.0, <6.0"],
}
redis_require = {"redis.PickleDataSet": ["redis~=4.1"]}
spark_require = {
    "spark.SparkDataSet": [SPARK, HDFS, S3FS],
    "spark.SparkHiveDataSet": [SPARK, HDFS, S3FS],
    "spark.SparkJDBCDataSet": [SPARK, HDFS, S3FS],
    "spark.DeltaTableDataSet": [SPARK, HDFS, S3FS, "delta-spark~=1.0"],
}
svmlight_require = {"svmlight.SVMLightDataSet": ["scikit-learn~=1.0.2", "scipy~=1.7.3"]}
tensorflow_required = {
    "tensorflow.TensorflowModelDataset": [
        # currently only TensorFlow V2 supported for saving and loading.
        # V1 requires HDF5 and serialises differently
        "tensorflow~=2.0"
    ]
}
yaml_require = {"yaml.YAMLDataSet": [PANDAS, "PyYAML>=4.2, <7.0"]}

extras_require = {
    "api": _collect_requirements(api_require),
    "biosequence": _collect_requirements(biosequence_require),
    "dask": _collect_requirements(dask_require),
    "docs": [
        "docutils==0.16",
        "sphinx~=3.4.3",
        "sphinx_rtd_theme==0.4.1",
        "nbsphinx==0.8.1",
        "nbstripout~=0.4",
        "sphinx-autodoc-typehints==1.11.1",
        "sphinx_copybutton==0.3.1",
        "ipykernel>=5.3, <7.0",
        "myst-parser~=0.17.2",
    ],
    "geopandas": _collect_requirements(geopandas_require),
    "matplotlib": _collect_requirements(matplotlib_require),
    "holoviews": _collect_requirements(holoviews_require),
    "networkx": _collect_requirements(networkx_require),
    "pandas": _collect_requirements(pandas_require),
    "pillow": _collect_requirements(pillow_require),
    "video": _collect_requirements(video_require),
    "plotly": _collect_requirements(plotly_require),
    "redis": _collect_requirements(redis_require),
    "spark": _collect_requirements(spark_require),
    "svmlight": _collect_requirements(svmlight_require),
    "tensorflow": _collect_requirements(tensorflow_required),
    "yaml": _collect_requirements(yaml_require),
    **api_require,
    **biosequence_require,
    **dask_require,
    **geopandas_require,
    **matplotlib_require,
    **holoviews_require,
    **networkx_require,
    **pandas_require,
    **pillow_require,
    **video_require,
    **plotly_require,
    **spark_require,
    **svmlight_require,
    **tensorflow_required,
    **yaml_require,
}

extras_require["all"] = _collect_requirements(extras_require)

setup(
    name=name,
    version=version,
    description="Kedro-Datasets is where you can find all of Kedro's data connectors.",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/kedro-org/kedro-plugins/tree/main/kedro-datasets",
    install_requires=install_requires,
    tests_require=tests_require,
    author="Kedro",
    python_requires=">=3.7, <3.11",
    license="Apache Software License (Apache 2.0)",
    packages=find_packages(exclude=["tests*"]),
    extras_require=extras_require,
)
