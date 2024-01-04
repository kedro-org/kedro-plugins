import gc
import re
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from kedro.io.core import DatasetError
from psutil import Popen
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from kedro_datasets.spark import SparkHiveDataset

TESTSPARKDIR = "test_spark_dir"


@pytest.fixture(scope="module")
def spark_session():
    try:
        with TemporaryDirectory(TESTSPARKDIR) as tmpdir:
            spark = (
                SparkSession.builder.config(
                    "spark.local.dir", (Path(tmpdir) / "spark_local").absolute()
                )
                .config(
                    "spark.sql.warehouse.dir", (Path(tmpdir) / "warehouse").absolute()
                )
                .config("spark.sql.catalogImplementation", "hive")
                .config(
                    "javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;"
                    f"databaseName={(Path(tmpdir) / 'warehouse_db').absolute()};"
                    f"create=true",
                )
                .enableHiveSupport()
                .getOrCreate()
            )
            spark.sparkContext.setCheckpointDir(
                str((Path(tmpdir) / "spark_checkpoint").absolute())
            )
            yield spark

            # This fixture should be a dependency of other fixtures dealing with spark hive data
            # in this module so that it always exits last and stops the spark session
            # after tests are finished.
            spark.stop()
    except PermissionError:  # pragma: no cover
        # On Windows machine TemporaryDirectory can't be removed because some
        # files are still used by Java process.
        pass

    # remove the cached JVM vars
    SparkContext._jvm = None
    SparkContext._gateway = None

    # py4j doesn't shutdown properly so kill the actual JVM process
    for obj in gc.get_objects():
        try:
            if isinstance(obj, Popen) and "pyspark" in obj.args[0]:
                obj.terminate()  # pragma: no cover
        except ReferenceError:  # pragma: no cover
            # gc.get_objects may return dead weak proxy objects that will raise
            # ReferenceError when you isinstance them
            pass


@pytest.fixture(scope="module", autouse=True)
def spark_test_databases(spark_session):
    """Setup spark test databases for all tests in this module."""
    dataset = _generate_spark_df_one()
    dataset.createOrReplaceTempView("tmp")
    databases = ["default_1", "default_2"]

    # Setup the databases and test table before testing
    for database in databases:
        spark_session.sql(f"create database {database}")
    spark_session.sql("use default_1")
    spark_session.sql("create table table_1 as select * from tmp")

    yield spark_session

    # Drop the databases after testing
    for database in databases:
        spark_session.sql(f"drop database {database} cascade")


def assert_df_equal(expected, result):
    def indexRDD(data_frame):
        return data_frame.rdd.zipWithIndex().map(lambda x: (x[1], x[0]))

    index_expected = indexRDD(expected)
    index_result = indexRDD(result)
    assert (
        index_expected.cogroup(index_result)
        .map(lambda x: tuple(map(list, x[1])))
        .filter(lambda x: x[0] != x[1])
        .take(1)
        == []
    )


def _generate_spark_df_one():
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]
    return SparkSession.builder.getOrCreate().createDataFrame(data, schema).coalesce(1)


def _generate_spark_df_upsert():
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    data = [("Alex", 99), ("Jeremy", 55)]
    return SparkSession.builder.getOrCreate().createDataFrame(data, schema).coalesce(1)


def _generate_spark_df_upsert_expected():
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    data = [("Alex", 99), ("Bob", 12), ("Clarke", 65), ("Dave", 29), ("Jeremy", 55)]
    return SparkSession.builder.getOrCreate().createDataFrame(data, schema).coalesce(1)


class TestSparkHiveDataset:
    def test_cant_pickle(self):
        import pickle

        with pytest.raises(pickle.PicklingError):
            pickle.dumps(
                SparkHiveDataset(
                    database="default_1", table="table_1", write_mode="overwrite"
                )
            )

    def test_read_existing_table(self):
        dataset = SparkHiveDataset(
            database="default_1", table="table_1", write_mode="overwrite", save_args={}
        )
        assert_df_equal(_generate_spark_df_one(), dataset.load())

    def test_overwrite_empty_table(self, spark_session):
        spark_session.sql(
            "create table default_1.test_overwrite_empty_table (name string, age integer)"
        ).take(1)
        dataset = SparkHiveDataset(
            database="default_1",
            table="test_overwrite_empty_table",
            write_mode="overwrite",
        )
        dataset.save(_generate_spark_df_one())
        assert_df_equal(dataset.load(), _generate_spark_df_one())

    def test_overwrite_not_empty_table(self, spark_session):
        spark_session.sql(
            "create table default_1.test_overwrite_full_table (name string, age integer)"
        ).take(1)
        dataset = SparkHiveDataset(
            database="default_1",
            table="test_overwrite_full_table",
            write_mode="overwrite",
        )
        dataset.save(_generate_spark_df_one())
        dataset.save(_generate_spark_df_one())
        assert_df_equal(dataset.load(), _generate_spark_df_one())

    def test_insert_not_empty_table(self, spark_session):
        spark_session.sql(
            "create table default_1.test_insert_not_empty_table (name string, age integer)"
        ).take(1)
        dataset = SparkHiveDataset(
            database="default_1",
            table="test_insert_not_empty_table",
            write_mode="append",
        )
        dataset.save(_generate_spark_df_one())
        dataset.save(_generate_spark_df_one())
        assert_df_equal(
            dataset.load(), _generate_spark_df_one().union(_generate_spark_df_one())
        )

    def test_upsert_config_err(self):
        # no pk provided should prompt config error
        with pytest.raises(
            DatasetError, match="'table_pk' must be set to utilise 'upsert' read mode"
        ):
            SparkHiveDataset(database="default_1", table="table_1", write_mode="upsert")

    def test_upsert_empty_table(self, spark_session):
        spark_session.sql(
            "create table default_1.test_upsert_empty_table (name string, age integer)"
        ).take(1)
        dataset = SparkHiveDataset(
            database="default_1",
            table="test_upsert_empty_table",
            write_mode="upsert",
            table_pk=["name"],
        )
        dataset.save(_generate_spark_df_one())
        assert_df_equal(
            dataset.load().sort("name"), _generate_spark_df_one().sort("name")
        )

    def test_upsert_not_empty_table(self, spark_session):
        spark_session.sql(
            "create table default_1.test_upsert_not_empty_table (name string, age integer)"
        ).take(1)
        dataset = SparkHiveDataset(
            database="default_1",
            table="test_upsert_not_empty_table",
            write_mode="upsert",
            table_pk=["name"],
        )
        dataset.save(_generate_spark_df_one())
        dataset.save(_generate_spark_df_upsert())

        assert_df_equal(
            dataset.load().sort("name"),
            _generate_spark_df_upsert_expected().sort("name"),
        )

    def test_invalid_pk_provided(self):
        _test_columns = ["column_doesnt_exist"]
        dataset = SparkHiveDataset(
            database="default_1",
            table="table_1",
            write_mode="upsert",
            table_pk=_test_columns,
        )
        with pytest.raises(
            DatasetError,
            match=re.escape(
                f"Columns {str(_test_columns)} selected as primary key(s) "
                f"not found in table default_1.table_1",
            ),
        ):
            dataset.save(_generate_spark_df_one())

    def test_invalid_write_mode_provided(self):
        pattern = (
            "Invalid 'write_mode' provided: not_a_write_mode. "
            "'write_mode' must be one of: "
            "append, error, errorifexists, upsert, overwrite"
        )
        with pytest.raises(DatasetError, match=re.escape(pattern)):
            SparkHiveDataset(
                database="default_1",
                table="table_1",
                write_mode="not_a_write_mode",
                table_pk=["name"],
            )

    def test_invalid_schema_insert(self, spark_session):
        spark_session.sql(
            "create table default_1.test_invalid_schema_insert "
            "(name string, additional_column_on_hive integer)"
        ).take(1)
        dataset = SparkHiveDataset(
            database="default_1",
            table="test_invalid_schema_insert",
            write_mode="append",
        )
        with pytest.raises(
            DatasetError,
            match=r"Dataset does not match hive table schema\.\n"
            r"Present on insert only: \[\('age', 'int'\)\]\n"
            r"Present on schema only: \[\('additional_column_on_hive', 'int'\)\]",
        ):
            dataset.save(_generate_spark_df_one())

    def test_insert_to_non_existent_table(self):
        dataset = SparkHiveDataset(
            database="default_1", table="table_not_yet_created", write_mode="append"
        )
        dataset.save(_generate_spark_df_one())
        assert_df_equal(
            dataset.load().sort("name"), _generate_spark_df_one().sort("name")
        )

    def test_read_from_non_existent_table(self):
        dataset = SparkHiveDataset(
            database="default_1", table="table_doesnt_exist", write_mode="append"
        )
        with pytest.raises(
            DatasetError,
            match=r"Failed while loading data from data set SparkHiveDataset"
            r"|table_doesnt_exist"
            r"|UnresolvedRelation",
        ):
            dataset.load()

    def test_save_delta_format(self, mocker):
        dataset = SparkHiveDataset(
            database="default_1", table="delta_table", save_args={"format": "delta"}
        )
        mocked_save = mocker.patch("pyspark.sql.DataFrameWriter.saveAsTable")
        dataset.save(_generate_spark_df_one())
        mocked_save.assert_called_with(
            "default_1.delta_table", mode="errorifexists", format="delta"
        )
        assert dataset._format == "delta"
