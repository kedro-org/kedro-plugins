from copy import deepcopy
from typing import Any, Dict, List, Optional

import pandas as pd
from deltalake import DataCatalog, DeltaTable, Metadata
from deltalake._internal import TableNotFoundError
from deltalake.writer import write_deltalake
from kedro.io.core import AbstractDataSet, DataSetError


class DeltaTableDataSet(AbstractDataSet):
    DEFAULT_WRITE_MODE = "overwrite"
    ACCEPTED_WRITE_MODES = ("overwrite", "append")

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {"mode": DEFAULT_WRITE_MODE}

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filepath: Optional[str] = None,
        catalog_type: Optional[DataCatalog] = None,
        catalog_name: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        load_args: Optional[Dict[str, Any]] = None,
        save_args: Optional[Dict[str, Any]] = None,
        credentials: Optional[Dict[str, Any]] = None,
        fs_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._filepath = filepath
        self._catalog_type = catalog_type
        self._catalog_name = catalog_name
        self._database = database
        self._schema = schema
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
            raise DataSetError(
                f"Write mode {write_mode} is not supported, "
                f"Please use any of the following accepted modes "
                f"{self.ACCEPTED_WRITE_MODES}"
            )

        self._version = self._load_args.get("version", None)

        if self._filepath and self._catalog_type:
            raise DataSetError(
                "DeltaTableDataSet can either load from "
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
            if self._catalog_type == DataCatalog.AWS:
                self._delta_table = DeltaTable.from_data_catalog(
                    data_catalog=self._catalog_type,
                    database_name=self._database,
                    table_name=self._table,
                )
            else:
                self._delta_table = DeltaTable.from_data_catalog(
                    data_catalog=self._catalog_type,
                    data_catalog_id=self._catalog_name,
                    database_name=self._schema,
                    table_name=self._table,
                )

    @property
    def fs_args(self) -> Dict[str, Any]:
        fs_args = deepcopy(self._fs_args)
        fs_args.update(self._credentials)
        return fs_args

    @property
    def schema(self) -> Dict[str, Any]:
        return self._delta_table.schema().json()

    @property
    def metadata(self) -> Metadata:
        return self._delta_table.metadata()

    @property
    def history(self) -> List[Dict[str, Any]]:
        return self._delta_table.history()

    def get_loaded_version(self) -> int:
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

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._filepath,
            "catalog_type": self._catalog_type,
            "database": self._database,
            "table": self._table,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }
