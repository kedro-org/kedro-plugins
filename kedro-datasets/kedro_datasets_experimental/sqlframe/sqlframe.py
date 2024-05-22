import importlib
import logging
from typing import Any, Generator, Optional
from kedro.io import AbstractDataset
from sqlframe.base.dataframe import _BaseDataFrame
from sqlframe.standalone.session import _BaseSession

logger = logging.getLogger(__name__)


def _getattr_case_insensitive(obj: object, attr_name: str) -> callable:
    """
    Used to retrieve session object when capitalization is not know
    ahead of time. Take the backend name `duckdb` as an example
    importing the class `from sqlframe.duckdb import DuckDBSession`
    is near impossible to predict ahead of time.
    """
    attr_name_lower = attr_name.lower()
    for attr in dir(obj):
        if attr.lower() == attr_name_lower:
            attribute = getattr(obj, attr)
            logger.debug(f"Retrieved {obj.__name__}.{attr}")
            return attribute
    raise AttributeError(
        f"'{type(obj).__name__}' object has no attribute '{attr_name}'"
    )


class TableDataset(AbstractDataset[_BaseDataFrame, _BaseDataFrame]):
    """``TableDataSet`` loads/saves data from/to sqlframe table expressions

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    (In the following example one must use a OmegaConf resolver to create a
    live connection object, these are registered in ``settings.py`` and imported
    from ``kedro_datasets_experimental.sqlframe.resolvers``)

    .. code-block:: yaml

        my_table:
            type: kedro_datasets_experimental.sqlframe.TableDataset
            backend: duckdb
            table_name: "my_table"
            connection: ${duckdb_conn:data/01_raw/my_database}

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> import duckdb
        >>> from sqlframe.duckdb import DuckDBSession
        >>>
        >>> conn = duckdb.connect("test.duckdb") # or `:memory:`
        >>> session = DuckDBSession(conn)
        >>> df = session.createDataFrame([{"id": 1, "fname": "Jack"}])
        >>> dataset = TableDataset(
        >>>     backend="duckdb",
        >>>     table_name="test"
        >>> )
        >>> dataset.save(df)
        >>> dataset.load().show()

    .. code-block:: pycon

        >>> from psycopg2 import connect
        >>> from sqlframe.postgres import PostgresSession
        >>>
        >>> conn = connect(
        >>>    dbname="postgres",
        >>>    user="postgres",
        >>>    password="password",
        >>>    host="localhost",
        >>>    port="5432",
        >>> )
        >>> session = PostgresSession(conn=conn)
        >>> df = session.createDataFrame([{"id": 1, "fname": "Jack"}])
        >>> dataset = TableDataset(
        >>>     backend="postgres",
        >>>     table_name="test"
        >>> )
        >>> dataset.save(df)
        >>> dataset.load().show()

    """

    def __init__(
        self,
        *,
        backend: str,
        table_name: str,
        connection: Optional[Generator] = None,
        load_args: Optional[dict[str, any]] = None,
        save_args: Optional[dict[str, any]] = None,
    ):
        """``TableDataset`` creates a spark-like session for the associated
        database backend e.g. DuckDB or BigQuery. The connection Generator is used
        to provide a live connection object. It needs to be a generator if provided
        since this allows us to initialise lazily with OmegaConf and pass through the
        DataCatalog initialization process which includes a non-picklable deepcopy.

        Args:
            backend: The SQL backend to use
            table_name: The table to read
            connection: The lazily defined connection object
                e.g. duckdb.DuckDBPyConnection or similar
            load_args: Arbitrary load args provided to ``session.read.table``
            save_args: Arbitrary save args provided  ``DataFrame.write.saveAsTable``
        """
        self.backend = backend
        module = importlib.import_module(f"sqlframe.{backend}")
        session_class = _getattr_case_insensitive(module, f"{backend}session")
        self._connection = connection
        self._table_name = table_name
        self._load_args = load_args if load_args else {}
        self._save_args = save_args if save_args else {}

        if connection:
            self.session: _BaseSession = session_class(next(self._connection()))
        else:
            self.session: _BaseSession = session_class()

    def _load(self) -> _BaseDataFrame:
        """Read table from backend"""
        return self.session.read.table(self._table_name, **self._load_args)

    def _save(self, data: _BaseDataFrame) -> None:
        """Write table from backend"""
        return data.write.saveAsTable(name=self._table_name, **self._save_args)

    def _describe(self) -> dict[str, str]:
        return {
            "backend": str(self.session.__class__.__name__),
            "load_args": self._load_args,
            "save_args": self._save_args,
            "table": self._table_name,
        }


class FileDataset(AbstractDataset[_BaseDataFrame, _BaseDataFrame]):
    """``FileDataSet`` loads/saves data from/to filepaths using various DataFrame
    back-ends

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        my_csv:
            type: kedro_datasets_experimental.sqlframe.FileDataset
            backend: duckdb
            filepath: "01_data/raw/post_count.csv"
            file_format: csv
            save_args:
                mode: 'w'

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets_experimental.sqlframe import FileDataset
        >>>
        >>> FileDataset(
        >>>     backend='duckdb',
        >>>     filepath='l_post_count.csv',
        >>>     file_format='csv'
        >>> ).load().show()

    """

    def __init__(
        self,
        backend: str,
        filepath: str,
        file_format: str = "parquet",
        connection: Optional[Generator] = None,
        load_args: Optional[dict[str, any]] = None,
        save_args: Optional[dict[str, any]] = None,
    ) -> None:
        """``FileDataset`` creates a spark-like session for the associated
        database backend e.g. DuckDB or BigQuery. The connection Generator is used
        to provide a live connection object. It needs to be a generator if provided
        since this allows us to initialise lazily with OmegaConf and pass through the
        DataCatalog initialization process which includes a non-picklable deepcopy.

        Args:
            backend: The SQL backend to use
            filepath: The filepath in question
            file_format: csv, json, parquet or others provided as attributes 
                of the writer object
            connection: The lazily defined connection object
                e.g. duckdb.DuckDBPyConnection or similar
            load_args: Arbitrary load args provided to ``session.read.{file_format}``
            save_args: Arbitrary save args provided  ``DataFrame.write.{file_format}``
        """
        self.backend = backend
        module = importlib.import_module(f"sqlframe.{backend}")
        session_class = _getattr_case_insensitive(module, f"{backend}session")
        self._connection = connection
        self._filepath = filepath
        self._file_format = file_format
        self._load_args = load_args if load_args else {}
        self._save_args = save_args if save_args else {}
        if connection:
            self.session: _BaseSession = session_class(next(self._connection()))
        else:
            self.session: _BaseSession = session_class()

    def _load(self) -> _BaseDataFrame:
        """Read path from backend"""
        reader = getattr(self.session.read, self._file_format.lower())
        return reader(path=self._filepath, **self._load_args)

    def _save(self, data: _BaseDataFrame) -> None:
        """Write path from backend"""
        writer = getattr(data.write, self._file_format)
        writer(path=self._file_format, **self._save_args)

    def _describe(self) -> dict[str, str]:
        return {
            "backend": self.backend,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "filepath": self._filepath,
            "file_format": self._file_format,
        }
