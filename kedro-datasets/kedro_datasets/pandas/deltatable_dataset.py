import logging
from copy import deepcopy
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from kedro.io.core import AbstractDataSet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _split_filepath(filepath: str) -> Tuple[str, str]:
    split_ = filepath.split("://", 1)
    if len(split_) == 2:
        return split_[0] + "://", split_[1]
    return "", split_[0]


class DeltaTableDataSet(AbstractDataSet):
    DEFAULT_WRITE_MODE = "overwrite"

    def __init__(  # pylint: disable=too-many-arguments
        self,
        filepath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        credentials: Dict[str, Any] = None,
    ) -> None:
        self._read_path = filepath
        self._credentials = deepcopy(credentials) or {}

        fs_prefix, filepath = _split_filepath(filepath)
        split = filepath.split("/", 1)
        self._write_path = (
            "abfs://"
            + split[0]
            + "@"
            + self._credentials["account_name"]
            + ".dfs.core.windows.net/"
            + split[1]
        )

        self._storage_options = {
            "account_name": self._credentials["account_name"],
            "account_key": self._credentials["account_key"],
        }

        self._load_args = deepcopy(load_args) or {}
        self._save_args = deepcopy(save_args) or {}

        self._write_mode = self._save_args.get(
            "mode",
            DeltaTableDataSet.DEFAULT_WRITE_MODE,
        )

        self._load_version = self._load_args.get("version", None)
        if self._load_version:
            self._load_version = int(self._load_version)

        self._delta_table = DeltaTable(
            self._read_path,
            storage_options=self._storage_options,
            version=self._load_version,
        )

    def get_loaded_version(self) -> int:
        return self._delta_table.version()

    def _load(self) -> pd.DataFrame:
        df = self._delta_table.to_pandas()
        logger.info(f"Loaded {len(df)} rows from {self._read_path}")
        return df

    def _save(self, data: pd.DataFrame) -> None:
        write_deltalake(
            self._write_path,
            data,
            storage_options=self._storage_options,
            mode=self._write_mode,
        )
        logger.info(f"Saved {len(data)} rows to {self._write_path}")

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._read_path,
            "versions": int(len(self._delta_table.history())) - 1,
            "load_version": self.get_loaded_version(),
            "read_path": self._read_path,
            "write_path": self._write_path,
            "write_mode": self._write_mode,
        }
