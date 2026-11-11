import pytest
from kedro.io.core import DatasetError

from kedro_datasets.spark import SparkJDBCDataset


@pytest.fixture
def spark_jdbc_args():
    return {"url": "dummy_url", "table": "dummy_table"}


@pytest.fixture
def spark_jdbc_args_credentials(spark_jdbc_args):
    args = spark_jdbc_args
    args.update({"credentials": {"user": "dummy_user", "password": "dummy_pw"}})
    return args


@pytest.fixture
def spark_jdbc_args_credentials_with_none_password(spark_jdbc_args):
    args = spark_jdbc_args
    args.update({"credentials": {"user": "dummy_user", "password": None}})
    return args


@pytest.fixture
def spark_jdbc_args_save_load(spark_jdbc_args):
    args = spark_jdbc_args
    connection_properties = {"properties": {"driver": "dummy_driver"}}
    args.update(
        {"save_args": connection_properties, "load_args": connection_properties}
    )
    return args


def test_missing_url():
    error_message = (
        "'url' argument cannot be empty. Please provide a JDBC"
        " URL of the form 'jdbc:subprotocol:subname'."
    )
    with pytest.raises(DatasetError, match=error_message):
        SparkJDBCDataset(table="dummy_table")


def test_missing_url_when_credentials_do_not_contain_url():
    error_message = (
        "'url' argument cannot be empty. Please provide a JDBC"
        " URL of the form 'jdbc:subprotocol:subname'."
    )
    with pytest.raises(DatasetError, match=error_message):
        SparkJDBCDataset(
            table="dummy_table",
            credentials={"user": "dummy_user", "password": "dummy_pw"},
        )


def test_missing_table():
    error_message = (
        "Either 'table' or 'query' must be provided. Please provide the name"
        " of the table to load or save data to, or a query to load data from."
    )
    with pytest.raises(DatasetError, match=error_message):
        SparkJDBCDataset(url="dummy_url", table=None)


def test_missing_table_and_query():
    error_message = (
        "Either 'table' or 'query' must be provided. Please provide the name"
        " of the table to load or save data to, or a query to load data from."
    )
    with pytest.raises(DatasetError, match=error_message):
        SparkJDBCDataset(url="dummy_url")


# A blank ``table`` is reported as blank even when a valid ``query`` is also
# given, matching the catalog schema's ``minLength`` on ``table``.
@pytest.mark.parametrize("query", [None, "SELECT 1 AS col1"])
@pytest.mark.parametrize("table", ["", "   "])
def test_blank_table(table, query):
    error_message = (
        "'table' argument cannot be empty. Please provide"
        " the name of the table to load or save data to."
    )
    with pytest.raises(DatasetError, match=error_message):
        SparkJDBCDataset(url="dummy_url", table=table, query=query)


@pytest.mark.parametrize("query", [None, "SELECT 1 AS col1"])
@pytest.mark.parametrize("table", [123, 1.5, ["dummy_table"], {"name": "t"}])
def test_non_string_table(table, query):
    pattern = (
        rf"'table' argument must be a string, got {type(table).__name__}\."
        r" Please provide the name of the table to load or save data to\."
    )
    with pytest.raises(DatasetError, match=pattern):
        SparkJDBCDataset(url="dummy_url", table=table, query=query)


def test_table_and_query_together():
    error_message = "'table' and 'query' arguments cannot be used together."
    with pytest.raises(DatasetError, match=error_message):
        SparkJDBCDataset(url="dummy_url", table="dummy_table", query="SELECT 1 AS col1")


@pytest.mark.parametrize("table", [None, "dummy_table"])
@pytest.mark.parametrize("query", ["", "   ", ";", " ; "])
def test_blank_query(query, table):
    error_message = (
        "'query' argument cannot be empty. Please provide a SQL query"
        " to load data from."
    )
    with pytest.raises(DatasetError, match=error_message):
        SparkJDBCDataset(url="dummy_url", table=table, query=query)


def test_save(mocker, spark_jdbc_args):
    mock_data = mocker.Mock()
    dataset = SparkJDBCDataset(**spark_jdbc_args)
    dataset.save(mock_data)
    mock_data.write.jdbc.assert_called_with("dummy_url", "dummy_table")


def test_save_credentials(mocker, spark_jdbc_args_credentials):
    mock_data = mocker.Mock()
    dataset = SparkJDBCDataset(**spark_jdbc_args_credentials)
    dataset.save(mock_data)
    mock_data.write.jdbc.assert_called_with(
        "dummy_url",
        "dummy_table",
        properties={"user": "dummy_user", "password": "dummy_pw"},
    )


def test_save_credentials_url(mocker):
    mock_data = mocker.Mock()
    credentials = {
        "url": "credentials_url",
        "user": "dummy_user",
        "password": "dummy_pw",
    }
    dataset = SparkJDBCDataset(table="dummy_table", credentials=credentials)

    dataset.save(mock_data)

    mock_data.write.jdbc.assert_called_with(
        "credentials_url",
        "dummy_table",
        properties={"user": "dummy_user", "password": "dummy_pw"},
    )
    assert credentials == {
        "url": "credentials_url",
        "user": "dummy_user",
        "password": "dummy_pw",
    }


def test_save_explicit_url_takes_precedence_over_credentials_url(mocker):
    mock_data = mocker.Mock()
    dataset = SparkJDBCDataset(
        url="dummy_url",
        table="dummy_table",
        credentials={
            "url": "credentials_url",
            "user": "dummy_user",
            "password": "dummy_pw",
        },
    )

    dataset.save(mock_data)

    mock_data.write.jdbc.assert_called_with(
        "dummy_url",
        "dummy_table",
        properties={"user": "dummy_user", "password": "dummy_pw"},
    )


def test_save_args(mocker, spark_jdbc_args_save_load):
    mock_data = mocker.Mock()
    dataset = SparkJDBCDataset(**spark_jdbc_args_save_load)
    dataset.save(mock_data)
    mock_data.write.jdbc.assert_called_with(
        "dummy_url", "dummy_table", properties={"driver": "dummy_driver"}
    )


def test_except_bad_credentials(mocker, spark_jdbc_args_credentials_with_none_password):
    pattern = r"Credential property 'password' cannot be None(.+)"
    with pytest.raises(DatasetError, match=pattern):
        mock_data = mocker.Mock()
        dataset = SparkJDBCDataset(**spark_jdbc_args_credentials_with_none_password)
        dataset.save(mock_data)


def test_load(mocker, spark_jdbc_args):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset.get_spark"
    ).return_value
    dataset = SparkJDBCDataset(**spark_jdbc_args)
    dataset.load()
    spark.read.jdbc.assert_called_with("dummy_url", "dummy_table")


def test_load_credentials(mocker, spark_jdbc_args_credentials):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset.get_spark"
    ).return_value
    dataset = SparkJDBCDataset(**spark_jdbc_args_credentials)
    dataset.load()
    spark.read.jdbc.assert_called_with(
        "dummy_url",
        "dummy_table",
        properties={"user": "dummy_user", "password": "dummy_pw"},
    )


def test_load_credentials_url(mocker):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset.get_spark"
    ).return_value
    credentials = {
        "url": "credentials_url",
        "user": "dummy_user",
        "password": "dummy_pw",
    }
    dataset = SparkJDBCDataset(table="dummy_table", credentials=credentials)

    dataset.load()

    spark.read.jdbc.assert_called_with(
        "credentials_url",
        "dummy_table",
        properties={"user": "dummy_user", "password": "dummy_pw"},
    )
    assert credentials == {
        "url": "credentials_url",
        "user": "dummy_user",
        "password": "dummy_pw",
    }


def test_load_explicit_url_takes_precedence_over_credentials_url(mocker):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset.get_spark"
    ).return_value
    dataset = SparkJDBCDataset(
        url="dummy_url",
        table="dummy_table",
        credentials={
            "url": "credentials_url",
            "user": "dummy_user",
            "password": "dummy_pw",
        },
    )

    dataset.load()

    spark.read.jdbc.assert_called_with(
        "dummy_url",
        "dummy_table",
        properties={"user": "dummy_user", "password": "dummy_pw"},
    )


def test_describe_does_not_include_url_from_credentials():
    dataset = SparkJDBCDataset(
        table="dummy_table",
        credentials={
            "url": "credentials_url",
            "user": "dummy_user",
            "password": "dummy_pw",
        },
    )

    described = dataset._describe()

    assert "url" not in described


def test_load_args(mocker, spark_jdbc_args_save_load):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset.get_spark"
    ).return_value
    dataset = SparkJDBCDataset(**spark_jdbc_args_save_load)
    dataset.load()
    spark.read.jdbc.assert_called_with(
        "dummy_url", "dummy_table", properties={"driver": "dummy_driver"}
    )


def _mock_query_reader(spark):
    reader = spark.read.format.return_value
    options_reader = reader.options.return_value
    url_reader = options_reader.option.return_value
    query_reader = url_reader.option.return_value
    return reader, options_reader, url_reader, query_reader


def test_load_query(mocker):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset.get_spark"
    ).return_value
    query = "SELECT col1, col2 FROM dummy_table WHERE col3 > 5"
    dataset = SparkJDBCDataset(url="dummy_url", query=query)

    loaded = dataset.load()

    reader, options_reader, url_reader, query_reader = _mock_query_reader(spark)
    spark.read.format.assert_called_once_with("jdbc")
    reader.options.assert_called_once_with()
    options_reader.option.assert_called_once_with("url", "dummy_url")
    url_reader.option.assert_called_once_with("query", query)
    query_reader.load.assert_called_once_with()
    assert loaded is query_reader.load.return_value
    spark.read.jdbc.assert_not_called()


def test_load_query_with_credentials_and_load_args(mocker):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset.get_spark"
    ).return_value
    query = "SELECT col1 FROM dummy_table"
    dataset = SparkJDBCDataset(
        query=query,
        credentials={
            "url": "credentials_url",
            "user": "dummy_user",
            "password": "dummy_pw",
        },
        load_args={"properties": {"driver": "dummy_driver", "fetchsize": 1000}},
    )

    dataset.load()

    reader, options_reader, url_reader, query_reader = _mock_query_reader(spark)
    reader.options.assert_called_once_with(
        driver="dummy_driver",
        fetchsize=1000,
        user="dummy_user",
        password="dummy_pw",
    )
    options_reader.option.assert_called_once_with("url", "credentials_url")
    url_reader.option.assert_called_once_with("query", query)
    query_reader.load.assert_called_once_with()


@pytest.mark.parametrize(
    "query",
    [
        "SELECT col1 FROM dummy_table;",
        "SELECT col1 FROM dummy_table ; ",
        "  SELECT col1 FROM dummy_table;;",
    ],
)
def test_load_query_strips_trailing_semicolon(mocker, query):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset.get_spark"
    ).return_value
    dataset = SparkJDBCDataset(url="dummy_url", query=query)

    dataset.load()

    _, _, url_reader, _ = _mock_query_reader(spark)
    url_reader.option.assert_called_once_with("query", "SELECT col1 FROM dummy_table")
    assert dataset._describe()["query"] == "SELECT col1 FROM dummy_table"


def test_query_with_unsupported_load_args():
    pattern = (
        r"Only 'properties' can be provided in 'load_args' when 'query' is used,"
        r" got: 'column', 'numPartitions', 'predicates'\."
    )
    with pytest.raises(DatasetError, match=pattern):
        SparkJDBCDataset(
            url="dummy_url",
            query="SELECT col1 FROM dummy_table",
            load_args={
                "column": "col1",
                "numPartitions": 4,
                "predicates": ["col1 > 5"],
                "properties": {"driver": "dummy_driver"},
            },
        )


@pytest.mark.parametrize(
    ("properties", "reported"),
    [
        ({"dbtable": "dummy_table"}, "'dbtable'"),
        ({"query": "SELECT 1"}, "'query'"),
        ({"url": "other_url"}, "'url'"),
        ({"partitionColumn": "col1"}, "'partitionColumn'"),
        (
            {"lowerBound": 0, "upperBound": 10, "numPartitions": 2},
            "'lowerBound', 'numPartitions', 'upperBound'",
        ),
        ({"DBTABLE": "dummy_table", "driver": "dummy_driver"}, "'DBTABLE'"),
    ],
)
def test_query_with_incompatible_load_properties(properties, reported):
    pattern = (
        r"'properties' in 'load_args' and 'credentials' cannot contain reader options"
        r" that conflict with 'query' \('dbtable', 'query', 'url', 'partitionColumn',"
        rf" 'lowerBound', 'upperBound', 'numPartitions'\), got: {reported}\."
    )
    with pytest.raises(DatasetError, match=pattern):
        SparkJDBCDataset(
            url="dummy_url",
            query="SELECT col1 FROM dummy_table",
            load_args={"properties": properties},
        )


@pytest.mark.parametrize(
    "save_args", [{}, {"properties": {"driver": "dummy_driver"}}, {"mode": "append"}]
)
def test_query_with_save_args(save_args):
    pattern = r"'save_args' cannot be used together with 'query'"
    with pytest.raises(DatasetError, match=pattern):
        SparkJDBCDataset(
            url="dummy_url", query="SELECT col1 FROM dummy_table", save_args=save_args
        )


def test_save_query(mocker):
    mock_data = mocker.Mock()
    dataset = SparkJDBCDataset(url="dummy_url", query="SELECT col1 FROM dummy_table")
    pattern = r"'save' is not supported when 'query' is provided\."
    with pytest.raises(DatasetError, match=pattern):
        dataset.save(mock_data)
    mock_data.write.jdbc.assert_not_called()


def test_describe_query():
    query = "SELECT col1 FROM dummy_table"
    dataset = SparkJDBCDataset(
        url="dummy_url",
        query=query,
        credentials={"user": "dummy_user", "password": "dummy_pw"},
    )

    described = dataset._describe()

    assert described["query"] == query
    assert described["table"] is None
    assert described["load_args"] == {"properties": {}}
    assert described["save_args"] == {}


@pytest.mark.parametrize(
    ("credentials", "reported"),
    [
        ({"dbtable": "dummy_table"}, "'dbtable'"),
        ({"numPartitions": "4"}, "'numPartitions'"),
        ({"QUERY": "SELECT 1", "user": "dummy_user"}, "'QUERY'"),
    ],
)
def test_query_with_incompatible_credentials(credentials, reported):
    pattern = (
        r"'properties' in 'load_args' and 'credentials' cannot contain reader options"
        rf" that conflict with 'query' \(.*\), got: {reported}\."
    )
    with pytest.raises(DatasetError, match=pattern):
        SparkJDBCDataset(
            url="dummy_url",
            query="SELECT col1 FROM dummy_table",
            credentials=credentials,
        )


@pytest.mark.parametrize("query", [123, 1.5, ["SELECT 1"], {"sql": "SELECT 1"}])
def test_non_string_query(query):
    pattern = (
        rf"'query' argument must be a string, got {type(query).__name__}\."
        r" Please provide a SQL query to load data from\."
    )
    with pytest.raises(DatasetError, match=pattern):
        SparkJDBCDataset(url="dummy_url", query=query)


@pytest.mark.parametrize(
    "credentials", [None, {"user": "dummy_user", "password": "dummy_pw"}]
)
def test_load_query_with_none_properties(mocker, credentials):
    spark = mocker.patch(
        "kedro_datasets.spark.spark_jdbc_dataset.get_spark"
    ).return_value
    query = "SELECT col1 FROM dummy_table"
    dataset = SparkJDBCDataset(
        url="dummy_url",
        query=query,
        credentials=credentials,
        load_args={"properties": None},
    )

    dataset.load()
    dataset._describe()

    reader, options_reader, url_reader, query_reader = _mock_query_reader(spark)
    reader.options.assert_called_once_with(**(credentials or {}))
    options_reader.option.assert_called_once_with("url", "dummy_url")
    url_reader.option.assert_called_once_with("query", query)
    query_reader.load.assert_called_once_with()
