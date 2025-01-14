import copy
from pathlib import PurePosixPath
from typing import Any, NoReturn

import polars as pl

from kedro_datasets.pandas.sql_dataset import SQLQueryDataset, get_filepath_str


class PolarsDatabaseDataset(SQLQueryDataset):

    def __init__(  # noqa: PLR0913
        self,
        sql: str | None = None,
        credentials: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        filepath: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new ``PolarsDatabaseDataset``."""
        super().__init__(
            sql=sql,
            credentials=credentials,
            load_args=load_args,
            fs_args=fs_args,
            filepath=filepath,
            metadata=metadata,
        )

    def load(self) -> pl.DataFrame:
        load_args = copy.deepcopy(self._load_args)

        if self._filepath:
            load_path = get_filepath_str(PurePosixPath(self._filepath), self._protocol)
            with self._fs.open(load_path, mode="r") as fs_file:
                query = fs_file.read()
        else:
            query = load_args.pop("sql")

        return pl.read_database(
            query=query,
            connection=self._connection_str,
            **load_args
        )

    def save(self, data: None) -> NoReturn:
        pass