from copy import deepcopy
from typing import Any, Dict, List

import pandas as pd
from deltalake import DeltaTable, Metadata
from deltalake.writer import write_deltalake
from kedro.io.core import AbstractDataSet, DataSetError


class DeltaTableDataSet(AbstractDataSet):
    DEFAULT_WRITE_MODE = "overwrite"
    ACCEPTED_WRITE_MODES = (
        "overwrite",
        "append"
    )

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {
        "mode": DEFAULT_WRITE_MODE
    }

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filepath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: int = None,
        credentials: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
    ) -> None:
        self._filepath = filepath
        self._fs_args = deepcopy(fs_args) or {}
        self._credentials = deepcopy(credentials) or {}
        self._version = version

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

    @property
    def fs_args(self) -> Dict[str, Any]:
        fs_args = deepcopy(self._fs_args)
        fs_args.update(self._credentials)
        return fs_args

    def delta_table(self) -> DeltaTable:
        delta_table = DeltaTable(
            self._filepath,
            storage_options=self.fs_args,
            version=self._version
        )
        return delta_table

    @property
    def schema(self) -> Dict[str, Any]:
        import json
        return json.loads(
            self.delta_table().schema().to_json()
        )

    @property
    def metadata(self) -> Metadata:
        return self.delta_table().metadata()

    @property
    def history(self) -> List[Dict[str, Any]]:
        return self.delta_table().history()

    def get_loaded_version(self) -> int:
        return self.delta_table().version()

    def _load(self) -> pd.DataFrame:
        return self.delta_table().to_pandas()

    def _save(self, data: pd.DataFrame) -> None:
        write_deltalake(
            self._filepath,
            data,
            storage_options=self.fs_args,
            **self._save_args,
        )

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._filepath,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }
