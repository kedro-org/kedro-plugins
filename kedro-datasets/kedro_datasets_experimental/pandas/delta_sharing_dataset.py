from __future__ import annotations

from typing import Any

import delta_sharing
import pandas as pd
from kedro.io.core import AbstractDataset, DatasetError

from kedro_datasets._typing import TablePreview


class DeltaSharingDataset(AbstractDataset):
    """``DeltaSharingDataset`` loads data from a Delta Sharing shared table using the Delta Sharing open protocol.
    This dataset handles loading data into a Pandas DataFrame. Saving to Delta Sharing is not supported.

    Delta Sharing is an open protocol for secure real-time exchange of large datasets, which enables
    organizations to share data in real time regardless of which computing platforms they use. It is a
    simple REST protocol that securely shares access to part of a cloud dataset and leverages modern cloud
    storage systems, such as S3, ADLS, or GCS, to reliably transfer data.

    Example usage for the YAML API:

    .. code-block:: yaml

        my_delta_sharing_dataset:
          type: pandas.DeltaSharingDataset
          share: <share-name>
          schema: <schema-name>
          table: <table-name>
          credentials:
            profile_file: <profile-file-path>
          load_args:
            version: <version>
            limit: <limit>
            use_delta_format: <use_delta_format>

    Example usage for the Python API:

    .. code-block:: pycon

        >>> from kedro_datasets import DeltaSharingDataset
        >>> import pandas as pd
        >>>
        >>> credentials = {
        ...     "profile_file": "conf/local/config.share"
        ... }
        >>> load_args = {
        ...     "version": 1,
        ...     "limit": 10,
        ...     "use_delta_format": True
        ... }
        >>> dataset = DeltaSharingDataset(
        ...     share="example_share",
        ...     schema="example_schema",
        ...     table="example_table",
        ...     credentials=credentials,
        ...     load_args=load_args
        ... )
        >>> data = dataset.load()
        >>> print(data)
    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {
        "version": None,  # Load the latest version by default
        "limit": None,  # No limit by default
        "use_delta_format": False  # Default to not using Delta format
    }

    def __init__( # noqa: PLR0913
        self,
        *,
        share: str,
        schema: str,
        table: str,
        credentials: dict[str, Any],
        load_args: dict[str, Any] | None = None
    ) -> None:
        """Creates a new instance of ``DeltaSharingDataset``.

        Args:
            share (str): Share name in Delta Sharing.
            schema (str): Schema name in Delta Sharing.
            table (str): Table name in Delta Sharing.
            credentials (dict[str, Any]): Credentials for accessing the Delta Sharing profile. Must include:
                - `profile_file` (str): Path to the Delta Sharing profile file.
            load_args (dict[str, Any], optional): Additional options for loading data.
                - `version` (int, optional): A non-negative integer specifying the version of the table snapshot to load.
                  Defaults to None, which loads the latest version. This parameter allows you to access historical versions of the shared table.
                - `limit` (int, optional): A non-negative integer specifying the maximum number of rows to load.
                - `use_delta_format` (bool, optional): Whether to use the Delta format for loading data. Defaults to False.

        Raises:
            DatasetError: If the profile file is not specified in credentials.
        """
        self.share = share
        self.schema = schema
        self.table = table
        self.profile_file = credentials.get("profile_file")

        # Merge the provided load_args with DEFAULT_LOAD_ARGS
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self.version = self._load_args["version"]
        self.limit = self._load_args["limit"]
        self.use_delta_format = self._load_args["use_delta_format"]

        if not self.profile_file:
            raise DatasetError("Profile file must be specified in credentials.")

        self.table_path = f"{self.share}.{self.schema}.{self.table}"
        self.table_url = f"{self.profile_file}#{self.table_path}"

    def _load(self) -> pd.DataFrame:
        """Load data from the Delta Sharing shared table into a Pandas DataFrame.

        Returns:
            pd.DataFrame: Loaded data.
        """
        return delta_sharing.load_as_pandas(
            self.table_url,
            version=self.version,
            limit=self.limit,
            use_delta_format=self.use_delta_format
        )

    def _save(self, data: pd.DataFrame) -> None:
        """Saving to Delta Sharing shared tables is not supported.

        Args:
            data (pd.DataFrame): Data to save.

        Raises:
            NotImplementedError: Saving to Delta Sharing shared tables is not supported.
        """
        raise NotImplementedError("Saving to Delta Sharing shared tables is not supported.")

    def preview(self, nrows: int = 5) -> TablePreview:
        """Generate a preview of the dataset with a specified number of rows.

        Args:
            nrows (int, optional): The number of rows to include in the preview. Defaults to 5.

        Returns:
            TablePreview: A dictionary containing the data in a split format.
        """
        dataset_copy = self._copy()
        dataset_copy.limit = nrows
        data = dataset_copy._load()
        return data.to_dict(orient="split")

    def _describe(self) -> dict[str, Any]:
        """Describe the dataset configuration.

        Returns:
            dict[str, Any]: Dataset configuration.
        """
        return {
            "share": self.share,
            "schema": self.schema,
            "table": self.table,
            "profile_file": self.profile_file,
            "version": self.version,
            "limit": self.limit,
            "use_delta_format": self.use_delta_format
        }
