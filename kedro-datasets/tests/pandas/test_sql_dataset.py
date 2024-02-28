from pathlib import PosixPath
from unittest.mock import ANY

import pandas as pd
import pytest
import sqlalchemy
from kedro.io.core import DatasetError

import kedro_datasets
from kedro_datasets.pandas import SQLQueryDataset, SQLTableDataset

TABLE_NAME = "table_a"
CONNECTION = "sqlite:///kedro.db"
MSSQL_CONNECTION = "mssql+pyodbc://?odbc_connect=DRIVER%3DODBC+Driver+for+SQL"
SQL_QUERY = "SELECT * FROM table_a"
EXECUTION_OPTIONS = {"stream_results": True}
FAKE_CONN_STR = "some_sql://scott:tiger@localhost/foo"
ERROR_PREFIX = (
    r"A module\/driver is missing when connecting to your SQL server\.(.|\n)*"
)


@pytest.fixture(autouse=True)
def cleanup_engines():
    yield
    SQLTableDataset.engines = {}
    SQLQueryDataset.engines = {}


@pytest.fixture
def dummy_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


@pytest.fixture
def sql_file(tmp_path: PosixPath):
    file = tmp_path / "test.sql"
    file.write_text(SQL_QUERY)
    return file.as_posix()


@pytest.fixture(params=[{}])
def table_dataset(request):
    kwargs = {"table_name": TABLE_NAME, "credentials": {"con": CONNECTION}}
    kwargs.update(request.param)
    return SQLTableDataset(**kwargs)


@pytest.fixture(params=[{}])
def query_dataset(request):
    kwargs = {"sql": SQL_QUERY, "credentials": {"con": CONNECTION}}
    kwargs.update(request.param)
    return SQLQueryDataset(**kwargs)


@pytest.fixture(params=[{}])
def query_file_dataset(request, sql_file):
    kwargs = {"filepath": sql_file, "credentials": {"con": CONNECTION}}
    kwargs.update(request.param)
    return SQLQueryDataset(**kwargs)


class TestSQLTableDataset:
    _unknown_conn = "mysql+unknown_module://scott:tiger@localhost/foo"

    @staticmethod
    def _assert_sqlalchemy_called_once(*args):
        _callable = sqlalchemy.engine.reflection.Inspector.has_table
        if args:
            _callable.assert_called_once_with(*args)
        else:
            assert _callable.call_count == 1

    def test_empty_table_name(self):
        """Check the error when instantiating with an empty table"""
        pattern = r"'table\_name' argument cannot be empty\."
        with pytest.raises(DatasetError, match=pattern):
            SQLTableDataset(table_name="", credentials={"con": CONNECTION})

    def test_empty_connection(self):
        """Check the error when instantiating with an empty
        connection string"""
        pattern = (
            r"'con' argument cannot be empty\. "
            r"Please provide a SQLAlchemy connection string\."
        )
        with pytest.raises(DatasetError, match=pattern):
            SQLTableDataset(table_name=TABLE_NAME, credentials={"con": ""})

    def test_driver_missing(self, mocker):
        """Check the error when the sql driver is missing"""
        mocker.patch(
            "kedro_datasets.pandas.sql_dataset.create_engine",
            side_effect=ImportError("No module named 'mysqldb'"),
        )
        with pytest.raises(DatasetError, match=ERROR_PREFIX + "mysqlclient"):
            SQLTableDataset(
                table_name=TABLE_NAME, credentials={"con": CONNECTION}
            ).exists()

    def test_unknown_sql(self):
        """Check the error when unknown sql dialect is provided;
        this means the error is raised on catalog creation, rather
        than on load or save operation.
        """
        pattern = r"The SQL dialect in your connection is not supported by SQLAlchemy"
        with pytest.raises(DatasetError, match=pattern):
            SQLTableDataset(
                table_name=TABLE_NAME, credentials={"con": FAKE_CONN_STR}
            ).exists()

    def test_unknown_module(self, mocker):
        """Test that if an unknown module/driver is encountered by SQLAlchemy
        then the error should contain the original error message"""
        mocker.patch(
            "kedro_datasets.pandas.sql_dataset.create_engine",
            side_effect=ImportError("No module named 'unknown_module'"),
        )
        pattern = ERROR_PREFIX + r"No module named \'unknown\_module\'"
        with pytest.raises(DatasetError, match=pattern):
            SQLTableDataset(
                table_name=TABLE_NAME, credentials={"con": CONNECTION}
            ).exists()

    def test_str_representation_table(self, table_dataset):
        """Test the data set instance string representation"""
        str_repr = str(table_dataset)
        assert (
            "SQLTableDataset(load_args={}, save_args={'index': False}, "
            f"table_name={TABLE_NAME})" in str_repr
        )
        assert CONNECTION not in str(str_repr)

    def test_table_exists(self, mocker, table_dataset):
        """Test `exists` method invocation"""
        mocker.patch(
            "sqlalchemy.engine.reflection.Inspector.has_table", return_value=False
        )
        assert not table_dataset.exists()
        self._assert_sqlalchemy_called_once(TABLE_NAME, None)

    @pytest.mark.parametrize(
        "table_dataset", [{"load_args": {"schema": "ingested"}}], indirect=True
    )
    def test_table_exists_schema(self, mocker, table_dataset):
        """Test `exists` method invocation with DB schema provided"""
        mocker.patch(
            "sqlalchemy.engine.reflection.Inspector.has_table", return_value=False
        )
        assert not table_dataset.exists()
        self._assert_sqlalchemy_called_once(TABLE_NAME, "ingested")

    def test_table_exists_mocked(self, mocker, table_dataset):
        """Test `exists` method invocation with mocked list of tables"""
        mocker.patch(
            "sqlalchemy.engine.reflection.Inspector.has_table", return_value=True
        )
        assert table_dataset.exists()
        self._assert_sqlalchemy_called_once(TABLE_NAME, None)

    def test_load_sql_params(self, mocker, table_dataset):
        """Test `load` method invocation"""
        mocker.patch("pandas.read_sql_table")
        table_dataset.load()
        pd.read_sql_table.assert_called_once_with(
            table_name=TABLE_NAME, con=table_dataset.engines[CONNECTION]
        )

    def test_save_default_index(self, mocker, table_dataset, dummy_dataframe):
        """Test `save` method invocation"""
        mocker.patch.object(dummy_dataframe, "to_sql")
        table_dataset.save(dummy_dataframe)
        dummy_dataframe.to_sql.assert_called_once_with(
            name=TABLE_NAME, con=table_dataset.engines[CONNECTION], index=False
        )

    @pytest.mark.parametrize(
        "table_dataset", [{"save_args": {"index": True}}], indirect=True
    )
    def test_save_overwrite_index(self, mocker, table_dataset, dummy_dataframe):
        """Test writing DataFrame index as a column"""
        mocker.patch.object(dummy_dataframe, "to_sql")
        table_dataset.save(dummy_dataframe)
        dummy_dataframe.to_sql.assert_called_once_with(
            name=TABLE_NAME, con=table_dataset.engines[CONNECTION], index=True
        )

    @pytest.mark.parametrize(
        "table_dataset", [{"save_args": {"name": "TABLE_B"}}], indirect=True
    )
    def test_save_ignore_table_name_override(
        self, mocker, table_dataset, dummy_dataframe
    ):
        """Test that putting the table name is `save_args` does not have any
        effect"""
        mocker.patch.object(dummy_dataframe, "to_sql")
        table_dataset.save(dummy_dataframe)
        dummy_dataframe.to_sql.assert_called_once_with(
            name=TABLE_NAME, con=table_dataset.engines[CONNECTION], index=False
        )

    def test_additional_params(self, mocker):
        """Check additional parametes are sent to engine"""
        mocker.patch(
            "kedro_datasets.pandas.sql_dataset.create_engine",
        )
        additional_params = {"param1": "1", "param2": "2"}
        credentials = {"con": CONNECTION, **additional_params}
        SQLTableDataset(table_name=TABLE_NAME, credentials=credentials).engine
        kedro_datasets.pandas.sql_dataset.create_engine.assert_called_once_with(
            CONNECTION, **additional_params
        )


class TestSQLTableDatasetSingleConnection:
    def test_single_connection(self, dummy_dataframe, mocker):
        """Test to make sure multiple instances use the same connection object."""
        mocker.patch("pandas.read_sql_table")
        dummy_to_sql = mocker.patch.object(dummy_dataframe, "to_sql")
        kwargs = {"table_name": TABLE_NAME, "credentials": {"con": CONNECTION}}

        first = SQLTableDataset(**kwargs)
        assert not first.exists()  # Do something to create the `Engine`
        unique_connection = first.engines[CONNECTION]
        datasets = [SQLTableDataset(**kwargs) for _ in range(10)]

        for ds in datasets:
            ds.save(dummy_dataframe)
            engine = ds.engines[CONNECTION]
            assert engine is unique_connection

        expected_call = mocker.call(name=TABLE_NAME, con=unique_connection, index=False)
        dummy_to_sql.assert_has_calls([expected_call] * 10)

        for ds in datasets:
            ds.load()
            engine = ds.engines[CONNECTION]
            assert engine is unique_connection

    def test_create_connection_only_once(self, mocker):
        """Test that two datasets that need to connect to the same db
        (but different tables, for example) only create a connection once.
        """
        mock_engine = mocker.patch("kedro_datasets.pandas.sql_dataset.create_engine")
        mock_inspector = mocker.patch(
            "kedro_datasets.pandas.sql_dataset.inspect"
        ).return_value
        mock_inspector.has_table.return_value = False
        first = SQLTableDataset(table_name=TABLE_NAME, credentials={"con": CONNECTION})
        assert not first.exists()  # Do something to create the `Engine`
        assert len(first.engines) == 1

        second = SQLTableDataset(
            table_name="other_table", credentials={"con": CONNECTION}
        )
        assert not second.exists()  # Do something to fetch the `Engine`
        assert len(second.engines) == 1
        assert len(first.engines) == 1

        mock_engine.assert_called_once_with(CONNECTION)

    def test_multiple_connections(self, mocker):
        """Test that two datasets that need to connect to different dbs
        only create one connection per db.
        """
        mock_engine = mocker.patch("kedro_datasets.pandas.sql_dataset.create_engine")
        mock_inspector = mocker.patch(
            "kedro_datasets.pandas.sql_dataset.inspect"
        ).return_value
        mock_inspector.has_table.return_value = False
        first = SQLTableDataset(table_name=TABLE_NAME, credentials={"con": CONNECTION})
        assert not first.exists()  # Do something to create the `Engine`
        assert len(first.engines) == 1

        second_con = f"other_{CONNECTION}"
        second = SQLTableDataset(table_name=TABLE_NAME, credentials={"con": second_con})
        assert not second.exists()  # Do something to create the `Engine`
        assert len(second.engines) == 2
        assert len(first.engines) == 2

        expected_calls = [mocker.call(CONNECTION), mocker.call(second_con)]
        assert mock_engine.call_args_list == expected_calls


class TestSQLQueryDataset:
    def test_empty_query_error(self):
        """Check the error when instantiating with empty query or file"""
        pattern = (
            r"'sql' and 'filepath' arguments cannot both be empty\."
            r"Please provide a sql query or path to a sql query file\."
        )
        with pytest.raises(DatasetError, match=pattern):
            SQLQueryDataset(sql="", filepath="", credentials={"con": CONNECTION})

    def test_empty_con_error(self):
        """Check the error when instantiating with empty connection string"""
        pattern = (
            r"'con' argument cannot be empty\. Please provide "
            r"a SQLAlchemy connection string"
        )
        with pytest.raises(DatasetError, match=pattern):
            SQLQueryDataset(sql=SQL_QUERY, credentials={"con": ""})

    @pytest.mark.parametrize(
        "query_dataset, has_execution_options",
        [
            ({"execution_options": EXECUTION_OPTIONS}, True),
            ({"execution_options": {}}, False),
            ({}, False),
        ],
        indirect=["query_dataset"],
    )
    def test_load(self, mocker, query_dataset, has_execution_options):
        """Test `load` method invocation"""
        mocker.patch("pandas.read_sql_query")
        query_dataset.load()

        # Check that data was loaded with the expected query, connection string and
        # execution options:
        pd.read_sql_query.assert_called_once_with(sql=SQL_QUERY, con=ANY)
        con_arg = pd.read_sql_query.call_args_list[0][1]["con"]
        assert str(con_arg.url) == CONNECTION
        assert len(con_arg.get_execution_options()) == bool(has_execution_options)
        if has_execution_options:
            assert con_arg.get_execution_options() == EXECUTION_OPTIONS

    @pytest.mark.parametrize(
        "query_file_dataset, has_execution_options",
        [
            ({"execution_options": EXECUTION_OPTIONS}, True),
            ({"execution_options": {}}, False),
            ({}, False),
        ],
        indirect=["query_file_dataset"],
    )
    def test_load_query_file(self, mocker, query_file_dataset, has_execution_options):
        """Test `load` method with a query file"""
        mocker.patch("pandas.read_sql_query")
        query_file_dataset.load()

        # Check that data was loaded with the expected query, connection string and
        # execution options:
        pd.read_sql_query.assert_called_once_with(sql=SQL_QUERY, con=ANY)
        con_arg = pd.read_sql_query.call_args_list[0][1]["con"]
        assert str(con_arg.url) == CONNECTION
        assert len(con_arg.get_execution_options()) == bool(has_execution_options)
        if has_execution_options:
            assert con_arg.get_execution_options() == EXECUTION_OPTIONS

    def test_load_driver_missing(self, mocker):
        """Test that if an unknown module/driver is encountered by SQLAlchemy
        then the error should contain the original error message"""
        _err = ImportError("No module named 'mysqldb'")
        mocker.patch(
            "kedro_datasets.pandas.sql_dataset.create_engine", side_effect=_err
        )
        with pytest.raises(DatasetError, match=ERROR_PREFIX + "mysqlclient"):
            SQLQueryDataset(sql=SQL_QUERY, credentials={"con": CONNECTION}).load()

    def test_invalid_module(self, mocker):
        """Test that if an unknown module/driver is encountered by SQLAlchemy
        then the error should contain the original error message"""
        _err = ImportError("Invalid module some_module")
        mocker.patch(
            "kedro_datasets.pandas.sql_dataset.create_engine", side_effect=_err
        )
        pattern = ERROR_PREFIX + r"Invalid module some\_module"
        with pytest.raises(DatasetError, match=pattern):
            SQLQueryDataset(sql=SQL_QUERY, credentials={"con": CONNECTION}).load()

    def test_load_unknown_module(self, mocker):
        """Test that if an unknown module/driver is encountered by SQLAlchemy
        then the error should contain the original error message"""
        _err = ImportError("No module named 'unknown_module'")
        mocker.patch(
            "kedro_datasets.pandas.sql_dataset.create_engine", side_effect=_err
        )
        pattern = ERROR_PREFIX + r"No module named \'unknown\_module\'"
        with pytest.raises(DatasetError, match=pattern):
            SQLQueryDataset(sql=SQL_QUERY, credentials={"con": CONNECTION}).load()

    def test_load_unknown_sql(self):
        """Check the error when unknown SQL dialect is provided
        in the connection string"""
        pattern = r"The SQL dialect in your connection is not supported by SQLAlchemy"
        with pytest.raises(DatasetError, match=pattern):
            SQLQueryDataset(sql=SQL_QUERY, credentials={"con": FAKE_CONN_STR}).load()

    def test_save_error(self, query_dataset, dummy_dataframe):
        """Check the error when trying to save to the data set"""
        pattern = r"'save' is not supported on SQLQueryDataset"
        with pytest.raises(DatasetError, match=pattern):
            query_dataset.save(dummy_dataframe)

    def test_str_representation_sql(self, query_dataset, sql_file):
        """Test the data set instance string representation"""
        str_repr = str(query_dataset)
        assert (
            "SQLQueryDataset(execution_options={}, filepath=None, "
            f"load_args={{}}, sql={SQL_QUERY})" in str_repr
        )
        assert CONNECTION not in str_repr
        assert sql_file not in str_repr

    def test_str_representation_filepath(self, query_file_dataset, sql_file):
        """Test the data set instance string representation with filepath arg."""
        str_repr = str(query_file_dataset)
        assert (
            f"SQLQueryDataset(execution_options={{}}, filepath={str(sql_file)}, "
            "load_args={}, sql=None)" in str_repr
        )
        assert CONNECTION not in str_repr
        assert SQL_QUERY not in str_repr

    def test_sql_and_filepath_args(self, sql_file):
        """Test that an error is raised when both `sql` and `filepath` args are given."""
        pattern = (
            r"'sql' and 'filepath' arguments cannot both be provided."
            r"Please only provide one."
        )
        with pytest.raises(DatasetError, match=pattern):
            SQLQueryDataset(sql=SQL_QUERY, filepath=sql_file)

    def test_create_connection_only_once(self, mocker):
        """Test that two datasets that need to connect to the same db (but different
        tables and execution options, for example) only create a connection once.
        """
        mock_engine = mocker.patch("kedro_datasets.pandas.sql_dataset.create_engine")
        first = SQLQueryDataset(sql=SQL_QUERY, credentials={"con": CONNECTION})
        first.load()  # Do something to create the `Engine`
        assert len(first.engines) == 1

        # second engine has identical params to the first one
        # => no new engine should be created
        second = SQLQueryDataset(sql=SQL_QUERY, credentials={"con": CONNECTION})
        second.load()  # Do something to fetch the `Engine`
        mock_engine.assert_called_once_with(CONNECTION)
        assert second.engines == first.engines
        assert len(first.engines) == 1

        # third engine only differs by its query execution options
        # => no new engine should be created
        third = SQLQueryDataset(
            sql="a different query",
            credentials={"con": CONNECTION},
            execution_options=EXECUTION_OPTIONS,
        )
        third.load()  # Do something to fetch the `Engine`
        assert mock_engine.call_count == 1
        assert third.engines == first.engines
        assert len(first.engines) == 1

        # fourth engine has a different connection string
        # => a new engine has to be created
        fourth = SQLQueryDataset(
            sql=SQL_QUERY, credentials={"con": "an other connection string"}
        )
        fourth.load()  # Do something to create the `Engine`
        assert mock_engine.call_count == 2
        assert fourth.engines == first.engines
        assert len(first.engines) == 2

    def test_adapt_mssql_date_params_called(self, mocker):
        """Test that the adapt_mssql_date_params
        function is called when mssql backend is used.
        """
        mock_adapt_mssql_date_params = mocker.patch(
            "kedro_datasets.pandas.sql_dataset.SQLQueryDataset.adapt_mssql_date_params"
        )
        mock_engine = mocker.patch("kedro_datasets.pandas.sql_dataset.create_engine")
        ds = SQLQueryDataset(sql=SQL_QUERY, credentials={"con": MSSQL_CONNECTION})
        ds.load()  # Do something to create the `Engine`
        mock_engine.assert_called_once_with(MSSQL_CONNECTION)
        assert mock_adapt_mssql_date_params.call_count == 1
        assert len(ds.engines) == 1

    def test_adapt_mssql_date_params(self, mocker):
        """Test that the adapt_mssql_date_params
        function transforms the params as expected, i.e.
        making datetime date into the format %Y-%m-%dT%H:%M:%S
        and ignoring the other values.
        """
        mocker.patch("kedro_datasets.pandas.sql_dataset.create_engine")
        load_args = {
            "params": ["2023-01-01", "2023-01-01T20:26", "2023", "test", 1.0, 100]
        }
        ds = SQLQueryDataset(
            sql=SQL_QUERY, credentials={"con": MSSQL_CONNECTION}, load_args=load_args
        )
        assert ds._load_args["params"] == [
            "2023-01-01T00:00:00",
            "2023-01-01T20:26",
            "2023",
            "test",
            1.0,
            100,
        ]

    def test_adapt_mssql_date_params_wrong_input(self, mocker):
        """Test that the adapt_mssql_date_params
        function fails with the correct error message
        when given a wrong input
        """
        mocker.patch("kedro_datasets.pandas.sql_dataset.create_engine")
        load_args = {"params": {"value": 1000}}
        pattern = (
            "Unrecognized `params` format. It can be only a `list`, "
            "got <class 'dict'>"
        )
        with pytest.raises(DatasetError, match=pattern):
            SQLQueryDataset(
                sql=SQL_QUERY,
                credentials={"con": MSSQL_CONNECTION},
                load_args=load_args,
            )

    def test_additional_params(self, mocker):
        """Check additional parametes are sent to engine"""
        mocker.patch(
            "kedro_datasets.pandas.sql_dataset.create_engine",
        )
        additional_params = {"param1": "1", "param2": "2"}
        credentials = {"con": CONNECTION, **additional_params}
        SQLQueryDataset(sql=SQL_QUERY, credentials=credentials).engine
        kedro_datasets.pandas.sql_dataset.create_engine.assert_called_once_with(
            CONNECTION, **additional_params
        )
