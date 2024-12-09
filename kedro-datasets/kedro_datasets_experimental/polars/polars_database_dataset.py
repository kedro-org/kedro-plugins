import copy
from typing import Any, NoReturn

import fsspec
import polars as pl
from kedro.io.core import (
    AbstractDataset,
    DatasetError,
    get_protocol_and_path,
)


class PolarsDatabaseDataset(AbstractDataset[None, pl.DataFrame]):
    def __init__(  # noqa: PLR0913
        self,
        sql: str | None = None,
        credentials: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        filepath: str | None = None,
        execution_options: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if sql and filepath:
            raise DatasetError(
                "'sql' and 'filepath' arguments cannot both be provided."
                "Please only provide one."
            )

        if not (sql or filepath):
            raise DatasetError(
                "'sql' and 'filepath' arguments cannot both be empty."
                "Please provide a sql query or path to a sql query file."
            )

        if not (credentials and "con" in credentials and credentials["con"]):
            raise DatasetError(
                "'con' argument cannot be empty. Please "
                "provide a SQLAlchemy connection string."
            )

        default_load_args: dict[str, Any] = {}

        self._load_args = (
            {**default_load_args, **load_args}
            if load_args is not None
            else default_load_args
        )

        self.metadata = metadata

        # load sql query from file
        if sql:
            self._load_args["sql"] = sql
            self._filepath = None
        else:
            # filesystem for loading sql file
            _fs_args = copy.deepcopy(fs_args) or {}
            _fs_credentials = _fs_args.pop("credentials", {})
            protocol, path = get_protocol_and_path(str(filepath))

            self._protocol = protocol
            self._fs = fsspec.filesystem(self._protocol, **_fs_credentials, **_fs_args)
            self._filepath = path
        self._connection_str = credentials["con"]
        self._connection_args = {
            k: credentials[k] for k in credentials.keys() if k != "con"
        }
        self._execution_options = execution_options or {}
        if "mssql" in self._connection_str:
            self.adapt_mssql_date_params()

    def load(self) -> pl.DataFrame:
        pass

    def save(self, data: None) -> NoReturn:
        pass