"""``DeltaTableDataset`` loads/saves delta tables from/to a filesystem (e.g.: local,
S3, GCS), Databricks unity catalog and AWS Glue catalog respectively. It handles
load and save using a pandas dataframe.
"""
from copy import deepcopy
from typing import Any, Optional

import pandas as pd
from deltalake import DataCatalog, DeltaTable, Metadata
from deltalake.exceptions import TableNotFoundError
from deltalake.writer import write_deltalake
from kedro.io.core import AbstractDataset, DatasetError


class DeltaTableDataset(AbstractDataset):
    """``DeltaTableDataset`` loads/saves delta tables from/to a filesystem (e.g.: local,
    S3, GCS), Databricks unity catalog and AWS Glue catalog respectively. It handles
    load and save using a pandas dataframe. When saving data, you can specify one of two
    modes: overwrite(default), append. If you wish to alter the schema as a part of
    overwrite, pass overwrite_schema=True. You can overwrite a specific partition by using
    mode=overwrite together with partition_filters. This will remove all files within the
    matching partition and insert your data as new files.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        boats_filesystem:
          type: pandas.DeltaTableDataset
          filepath: data/01_raw/boats
          credentials: dev_creds
          load_args:
            version: 7
          save_args:
            mode: overwrite

        boats_databricks_unity_catalog:
          type: pandas.DeltaTableDataset
          credentials: dev_creds
          catalog_type: UNITY
          database: simple_database
          table: simple_table
          save_args:
            mode: overwrite

        trucks_aws_glue_catalog:
          type: pandas.DeltaTableDataset
          credentials: dev_creds
          catalog_type: AWS
          catalog_name: main
          database: db_schema
          table: db_table
          save_args:
            mode: overwrite

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.pandas import DeltaTableDataset
        >>> import pandas as pd
        >>>
        >>> data = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>> dataset = DeltaTableDataset(filepath=tmp_path / "test")
        >>>
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert data.equals(reloaded)
        >>>
        >>> new_data = pd.DataFrame({"col1": [7, 8], "col2": [9, 10], "col3": [11, 12]})
        >>> dataset.save(new_data)
        >>> assert isinstance(dataset.get_loaded_version(), int)

    """

    DEFAULT_WRITE_MODE = "overwrite"
    ACCEPTED_WRITE_MODES = ("overwrite", "append")

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {"mode": DEFAULT_WRITE_MODE}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: Optional[str] = None,
        catalog_type: Optional[DataCatalog] = None,
        catalog_name: Optional[str] = None,
        database: Optional[str] = None,
        table: Optional[str] = None,
        load_args: Optional[dict[str, Any]] = None,
        save_args: Optional[dict[str, Any]] = None,
        credentials: Optional[dict[str, Any]] = None,
        fs_args: Optional[dict[str, Any]] = None,
    ) -> None:
        """Creates a new instance of ``DeltaTableDataset``

        Args:
            filepath (str): Filepath to a delta lake file with the following accepted protocol:
                ``S3``: `s3://<bucket>/<path>`, `s3a://<bucket>/<path>`
                ``Azure``: `az://<container>/<path>`, `adl://<container>/<path>`,
                `abfs://<container>/<path>`
                ``GCS``: `gs://<bucket>/<path>`
                If any of the prefix above is not provided, `file` protocol (local filesystem)
                will be used.
            catalog_type (DataCatalog, Optional): `AWS` or `UNITY` if filepath is not provided.
                Defaults to None.
            catalog_name (str, Optional): the name of catalog in AWS Glue or Databricks Unity.
                Defaults to None.
            database (str, Optional): the name of the database (also referred to as schema).
                Defaults to None.
            table (str, Optional): the name of the table.
            load_args (Dict[str, Any], Optional): Additional options for loading file(s)
                into DeltaTableDataset. `load_args` accepts `version` to load the appropriate
                version when loading from a filesystem.
            save_args (Dict[str, Any], Optional): Additional saving options for saving into
                Delta lake. Here you can find all available arguments:
                https://delta-io.github.io/delta-rs/python/api_reference.html#writing-deltatables
            credentials (Dict[str, Any], Optional): Credentials required to get access to
                the underlying filesystem. E.g. for ``GCSFileSystem`` it should look like
                `{"token": None}`.
            fs_args (Dict[str, Any], Optional): Extra arguments to pass into underlying
                filesystem class constructor.
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``).
        Raises:
            DatasetError: Invalid configuration supplied (through DeltaTableDataset validation)

        """
        self._filepath = filepath
        self._catalog_type = catalog_type
        self._catalog_name = catalog_name
        self._database = database
        self._table = table
        self._fs_args = deepcopy(fs_args) or {}
        self._credentials = deepcopy(credentials) or {}

        # DeltaTable cannot be instantiated from an empty directory
        # for the first time creation from filepath, we need to delay the instantiation
        self.is_empty_dir: bool = False
        self._delta_table: Optional[DeltaTable] = None

        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args:
            self._load_args.update(load_args)

        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args:
            self._save_args.update(save_args)

        write_mode = self._save_args.get("mode", None)
        if write_mode not in self.ACCEPTED_WRITE_MODES:
            raise DatasetError(
                f"Write mode {write_mode} is not supported, "
                f"Please use any of the following accepted modes "
                f"{self.ACCEPTED_WRITE_MODES}"
            )

        self._version = self._load_args.get("version", None)

        if self._filepath and self._catalog_type:
            raise DatasetError(
                "DeltaTableDataset can either load from "
                "filepath or catalog_type. Please provide "
                "one of either filepath or catalog_type."
            )

        if self._filepath:
            try:
                self._delta_table = DeltaTable(
                    table_uri=self._filepath,
                    storage_options=self.fs_args,
                    version=self._version,
                )
            except TableNotFoundError:
                self.is_empty_dir = True
        else:
            self._delta_table = DeltaTable.from_data_catalog(
                data_catalog=DataCatalog[self._catalog_type],
                data_catalog_id=self._catalog_name,
                database_name=self._database,
                table_name=self._table,
            )

    @property
    def fs_args(self) -> dict[str, Any]:
        """Appends and returns filesystem credentials to fs_args."""
        fs_args = deepcopy(self._fs_args)
        fs_args.update(self._credentials)
        return fs_args

    @property
    def schema(self) -> dict[str, Any]:
        """Returns the schema of the DeltaTableDataset as a dictionary."""
        return self._delta_table.schema().json()

    @property
    def metadata(self) -> Metadata:
        """Returns the metadata of the DeltaTableDataset as a dictionary.
        Metadata contains the following:
        1. A unique id
        2. A name, if provided
        3. A description, if provided
        4. The list of partition_columns.
        5. The created_time of the table
        6. A map of table configuration. This includes fields such as delta.appendOnly,
        which if true indicates the table is not meant to have data deleted from it.

        Returns: Metadata object containing the above metadata attributes.
        """
        return self._delta_table.metadata()

    @property
    def history(self) -> list[dict[str, Any]]:
        """Returns the history of actions on DeltaTableDataset as a list of dictionaries."""
        return self._delta_table.history()

    def get_loaded_version(self) -> int:
        """Returns the version of the DeltaTableDataset that is currently loaded."""
        return self._delta_table.version()

    def _load(self) -> pd.DataFrame:
        return self._delta_table.to_pandas()

    def _save(self, data: pd.DataFrame) -> None:
        if self.is_empty_dir:
            # first time creation of delta table
            write_deltalake(
                self._filepath,
                data,
                storage_options=self.fs_args,
                **self._save_args,
            )
            self.is_empty_dir = False
            self._delta_table = DeltaTable(
                table_uri=self._filepath,
                storage_options=self.fs_args,
                version=self._version,
            )
        else:
            write_deltalake(
                self._delta_table,
                data,
                storage_options=self.fs_args,
                **self._save_args,
            )

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "catalog_type": self._catalog_type,
            "catalog_name": self._catalog_name,
            "database": self._database,
            "table": self._table,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }
