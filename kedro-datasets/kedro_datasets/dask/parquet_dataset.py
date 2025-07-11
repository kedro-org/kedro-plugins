"""``ParquetDataset`` is a dataset used to load and save data to parquet files using Dask
dataframe"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import dask.dataframe as dd
import fsspec
import triad
from kedro.io.core import AbstractDataset, get_protocol_and_path


class ParquetDataset(AbstractDataset[dd.DataFrame, dd.DataFrame]):
    """``ParquetDataset`` loads and saves data to parquet file(s). It uses Dask
    remote data services to handle the corresponding load and save operations:
    https://docs.dask.org/en/stable/how-to/connect-to-remote-data.html

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        cars:
          type: dask.ParquetDataset
          filepath: s3://bucket_name/path/to/folder
          save_args:
            compression: GZIP
          credentials:
            client_kwargs:
              aws_access_key_id: YOUR_KEY
              aws_secret_access_key: YOUR_SECRET
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> import dask.dataframe as dd
        >>> import pandas as pd
        >>> import numpy as np
        >>> from kedro_datasets.dask import ParquetDataset
        >>>
        >>> data = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [6, 7]})
        >>> ddf = dd.from_pandas(data, npartitions=2)
        >>>
        >>> dataset = ParquetDataset(
        ...     filepath=tmp_path / "path/to/folder", save_args={"compression": "GZIP"}
        ... )
        >>> dataset.save(ddf)
        >>> reloaded = dataset.load()
        >>> assert np.array_equal(ddf.compute(), reloaded.compute())

        The output schema can also be explicitly specified using
        [Triad](https://triad.readthedocs.io/en/latest/api/triad.collections.html#module-triad.collections.schema).
        This is processed to map specific columns to
        [PyArrow field types](https://arrow.apache.org/docs/python/api/datatypes.html) or schema. For instance:

        ```yaml
        parquet_dataset:
          type: dask.ParquetDataset
          filepath: "s3://bucket_name/path/to/folder"
          credentials:
            client_kwargs:
              aws_access_key_id: YOUR_KEY
              aws_secret_access_key: "YOUR SECRET"
          save_args:
            compression: GZIP
            schema:
              col1: [int32]
              col2: [int32]
              col3: [[int32]]
        ```

    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {"write_index": False}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``ParquetDataset`` pointing to concrete
        parquet files.

        Args:
            filepath: Filepath in POSIX format to a parquet file
                parquet collection or the directory of a multipart parquet.
            load_args: Additional loading options `dask.dataframe.read_parquet`:
                https://docs.dask.org/en/stable/generated/dask.dataframe.read_parquet.html
            save_args: Additional saving options for `dask.dataframe.to_parquet`:
                https://docs.dask.org/en/stable/generated/dask.dataframe.to_parquet.html
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Optional parameters to the backend file system driver:
                https://docs.dask.org/en/stable/how-to/connect-to-remote-data.html#optional-parameters
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        self._filepath = filepath
        self._fs_args = deepcopy(fs_args or {})
        self._credentials = deepcopy(credentials or {})

        self.metadata = metadata

        # Handle default load and save arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}

    @property
    def fs_args(self) -> dict[str, Any]:
        """Property of optional file system parameters.

        Returns:
            A dictionary of backend file system parameters, including credentials.
        """
        fs_args = deepcopy(self._fs_args)
        fs_args.update(self._credentials)
        return fs_args

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    def load(self) -> dd.DataFrame:
        return dd.read_parquet(
            self._filepath, storage_options=self.fs_args, **self._load_args
        )

    def save(self, data: dd.DataFrame) -> None:
        self._process_schema()
        data.to_parquet(
            path=self._filepath, storage_options=self.fs_args, **self._save_args
        )

    def _process_schema(self) -> None:
        """This method processes the schema in the catalog.yml or the API, if provided.
        This assumes that the schema is specified using Triad's grammar for
        schema definition.

        When the value of the `schema` variable is a string, it is assumed that
        it corresponds to the full schema specification for the data.

        Alternatively, if the `schema` is specified as a dictionary, then only the
        columns that are specified will be strictly mapped to a field type. The other
        unspecified columns, if present, will be inferred from the data.

        This method converts the Triad-parsed schema into a pyarrow schema.
        The output directly supports Dask's specifications for providing a schema
        when saving to a parquet file.

        Note that if a `pa.Schema` object is passed directly in the `schema` argument, no
        processing will be done. Additionally, the behavior when passing a `pa.Schema`
        object is assumed to be consistent with how Dask sees it. That is, it should fully
        define the  schema for all fields.
        """
        schema = self._save_args.get("schema")

        if isinstance(schema, dict):
            # The schema may contain values of different types, e.g., pa.DataType, Python types,
            # strings, etc. The latter requires a transformation, then we use triad handle all
            # other value types.

            # Create a schema from values that triad can handle directly
            triad_schema = triad.Schema(
                {k: v for k, v in schema.items() if not isinstance(v, str)}
            )

            # Handle the schema keys that are represented as string and add them to the triad schema
            triad_schema.update(
                triad.Schema(
                    ",".join(
                        [f"{k}:{v}" for k, v in schema.items() if isinstance(v, str)]
                    )
                )
            )

            # Update the schema argument with the normalized schema
            self._save_args["schema"].update(
                {col: field.type for col, field in triad_schema.items()}
            )

        elif isinstance(schema, str):
            self._save_args["schema"] = triad.Schema(schema).pyarrow_schema

    def _exists(self) -> bool:
        protocol = get_protocol_and_path(self._filepath)[0]
        file_system = fsspec.filesystem(protocol=protocol, **self.fs_args)
        return file_system.exists(self._filepath)
