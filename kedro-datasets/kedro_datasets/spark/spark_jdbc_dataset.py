"""SparkJDBCDataset to load and save a PySpark DataFrame via JDBC."""
from __future__ import annotations

from typing import Any

from kedro.io.core import AbstractDataset, DatasetError
from pyspark.sql import DataFrame

from kedro_datasets._utils.spark_utils import get_spark


class SparkJDBCDataset(AbstractDataset[DataFrame, DataFrame]):
    """``SparkJDBCDataset`` loads data from a database table accessible
    via JDBC URL url and connection properties and saves the content of
    a PySpark DataFrame to an external database table via JDBC.  It uses
    ``pyspark.sql.DataFrameReader`` and ``pyspark.sql.DataFrameWriter``
    internally, so it supports all allowed PySpark options on ``jdbc``.
    Instead of a table, a SQL ``query`` can be provided to load the result
    of that query; such a dataset is read-only.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        weather:
          type: spark.SparkJDBCDataset
          table: weather_table
          credentials: db_credentials
          load_args:
            properties:
              driver: org.postgresql.Driver
          save_args:
            properties:
              driver: org.postgresql.Driver

        weather_summary:
          type: spark.SparkJDBCDataset
          query: SELECT city, AVG(temperature) AS avg_temperature FROM weather_table GROUP BY city
          credentials: db_credentials
          load_args:
            properties:
              driver: org.postgresql.Driver

        # credentials.yml
        db_credentials:
          url: jdbc:postgresql://localhost/test
          user: scott
          password: tiger
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> import pandas as pd
        >>> from kedro_datasets.spark import SparkJDBCDataset
        >>> from pyspark.sql import SparkSession
        >>>
        >>> spark = SparkSession.builder.getOrCreate()
        >>> data = spark.createDataFrame(
        ...     pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        ... )
        >>>
        >>> url = "jdbc:postgresql://localhost/test"
        >>> table = "table_a"
        >>> connection_properties = {"driver": "org.postgresql.Driver"}
        >>> dataset = SparkJDBCDataset(
        ...     url=url,
        ...     table=table,
        ...     credentials={"user": "scott", "password": "tiger"},
        ...     load_args={"properties": connection_properties},
        ...     save_args={"properties": connection_properties},
        ... )
        >>>
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert data.toPandas().equals(reloaded.toPandas())

    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        table: str | None = None,
        query: str | None = None,
        url: str | None = None,
        credentials: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new ``SparkJDBCDataset``.

        Args:
            url: A JDBC URL of the form ``jdbc:subprotocol:subname``. When not
                provided, the URL can be supplied as ``url`` in ``credentials``
                (mirrors the ``pandas.SQLTableDataset`` ``credentials.con``
                convention for keeping connection endpoints out of
                ``catalog.yml``).
            table: The name of the table to load or save data to.
                Cannot be used together with ``query``.
            query: A SQL query to load data from, passed to Spark's JDBC
                ``query`` option. The dataset is read-only when ``query``
                is provided, and ``load_args`` may only contain
                ``properties``. Cannot be used together with ``table``.
            credentials: A dictionary of JDBC database connection arguments.
                Normally at least properties ``user`` and ``password`` with
                their corresponding values.  It updates ``properties``
                parameter in ``load_args`` and ``save_args`` in case it is
                provided.
            load_args: Provided to underlying PySpark ``jdbc`` function along
                with the JDBC URL and the name of the table. To find all
                supported arguments, see here:
                https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.jdbc.html
            save_args: Provided to underlying PySpark ``jdbc`` function along
                with the JDBC URL and the name of the table. To find all
                supported arguments, see here:
                https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.jdbc.html
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DatasetError: When ``url`` is empty, when neither or both of
                ``table`` and ``query`` are provided, when ``load_args``
                contains arguments other than ``properties`` together with
                ``query``, or when a property is provided with a None value.
        """

        credentials = credentials or {}
        url = url or credentials.get("url")

        if not url:
            raise DatasetError(
                "'url' argument cannot be empty. Please "
                "provide a JDBC URL of the form "
                "'jdbc:subprotocol:subname'."
            )

        if table and query:
            raise DatasetError(
                "'table' and 'query' arguments cannot be used together. "
                "Please provide either the name of the table to load or "
                "save data to, or a query to load data from."
            )

        if not table and not query:
            raise DatasetError(
                "'table' argument cannot be empty. Please "
                "provide the name of the table to load or save "
                "data to, or a 'query' to load data from."
            )

        self._url = url
        self._table = table or None
        self._query = query or None

        self.metadata = metadata

        # Handle default load and save arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}

        if self._query:
            unsupported_load_args = sorted(set(self._load_args) - {"properties"})
            if unsupported_load_args:
                raise DatasetError(
                    "Only 'properties' can be provided in 'load_args' when "
                    "'query' is used, got: "
                    f"{', '.join(repr(arg) for arg in unsupported_load_args)}."
                )

        # Update properties in load_args and save_args with credentials.
        credentials_properties = {
            cred_key: cred_value
            for cred_key, cred_value in credentials.items()
            if cred_key != "url"
        }
        if credentials_properties:
            # Check credentials for bad inputs.
            for cred_key, cred_value in credentials_properties.items():
                if cred_value is None:
                    raise DatasetError(
                        f"Credential property '{cred_key}' cannot be None. "
                        f"Please provide a value."
                    )

            load_properties = self._load_args.get("properties", {})
            save_properties = self._save_args.get("properties", {})
            self._load_args["properties"] = {
                **load_properties,
                **credentials_properties,
            }
            self._save_args["properties"] = {
                **save_properties,
                **credentials_properties,
            }

    def _describe(self) -> dict[str, Any]:
        load_args = self._load_args
        save_args = self._save_args

        # Remove user and password values from load and save properties.
        if "properties" in load_args:
            load_properties = load_args["properties"].copy()
            load_properties.pop("user", None)
            load_properties.pop("password", None)
            load_args = {**load_args, "properties": load_properties}
        if "properties" in save_args:
            save_properties = save_args["properties"].copy()
            save_properties.pop("user", None)
            save_properties.pop("password", None)
            save_args = {**save_args, "properties": save_properties}

        return {
            "table": self._table,
            "query": self._query,
            "load_args": load_args,
            "save_args": save_args,
        }

    def load(self) -> DataFrame:
        if self._table is None:
            return (
                get_spark()
                .read.format("jdbc")
                .options(**self._load_args.get("properties", {}))
                .option("url", self._url)
                .option("query", self._query)
                .load()
            )
        return get_spark().read.jdbc(self._url, self._table, **self._load_args)

    def save(self, data: DataFrame) -> None:
        if self._table is None:
            raise DatasetError(
                "'save' is not supported when 'query' is provided. Please "
                "provide 'table' instead to save data to a database table."
            )
        return data.write.jdbc(self._url, self._table, **self._save_args)
