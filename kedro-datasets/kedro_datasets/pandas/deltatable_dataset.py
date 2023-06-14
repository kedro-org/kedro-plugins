from copy import deepcopy
from typing import Any, Dict, List, Optional, Literal

import json
import pandas as pd
from deltalake import DeltaTable, Metadata, DataCatalog
from deltalake.writer import write_deltalake
from kedro.io.core import AbstractDataSet, DataSetError


class DeltaTableDataSet(AbstractDataSet):
    DEFAULT_WRITE_MODE = "overwrite"
    ACCEPTED_WRITE_MODES = ("overwrite", "append")

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {"mode": DEFAULT_WRITE_MODE}

    CATALOG = {"AWS": DataCatalog.AWS, "UNITY": DataCatalog.UNITY}

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filepath: Optional[str] = None,
        catalog: Optional[Literal["AWS", "UNITY"]] = None,
        database: Optional[str] = None,
        table: Optional[str] = None,
        load_args: Optional[Dict[str, Any]] = None,
        save_args: Optional[Dict[str, Any]] = None,
        credentials: Optional[Dict[str, Any]] = None,
        fs_args: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._filepath = filepath
        self._catalog = catalog
        self._database = database
        self._table = table
        self._fs_args = deepcopy(fs_args) or {}
        self._credentials = deepcopy(credentials) or {}

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

        if self._filepath and self._catalog:
            raise DataSetError(
                "DeltaTableDataSet can either load from "
                "filepath or catalog. Please provide "
                "one of either filepath or catalog."
            )

        if self._filepath:
            self._delta_table = DeltaTable(
                table_uri=self._filepath,
                storage_options=self.fs_args,
                version=self._version,
            )
        else:
            catalog = self.CATALOG[catalog]
            self._delta_table = DeltaTable.from_data_catalog(
                data_catalog=catalog,
                database_name=self._database,
                table_name=self._table,
            )

    @property
    def fs_args(self) -> Dict[str, Any]:
        fs_args = deepcopy(self._fs_args)
        fs_args.update(self._credentials)
        return fs_args

    @property
    def schema(self) -> Dict[str, Any]:
        return json.loads(self._delta_table.schema().to_json())

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
        write_deltalake(
            self._delta_table,
            data,
            storage_options=self.fs_args,
            **self._save_args,
        )

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._filepath,
            "catalog": self._catalog,
            "database": self._database,
            "table": self._table,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }
