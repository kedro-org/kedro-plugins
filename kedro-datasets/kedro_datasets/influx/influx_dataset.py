"""``InfluxQueryDataset`` to load and save data to a SQL backend.

Currently supported influxdb versions: 2.x
"""

import copy
import hashlib
import json
import logging
from typing import Any, Dict, List

from influxdb_client import InfluxDBClient
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from .._io import AbstractDataset, DatasetError

__all__ = ["InfluxQueryDataset"]

logger = logging.getLogger(__name__)


# source https://www.doc.ic.ac.uk/~nuric/coding/how-to-hash-a-dictionary-in-python.html
def dict_hash(dictionary: Dict[str, Any]) -> str:
    """MD5 hash of a dictionary."""
    dhash = hashlib.md5()
    encoded = json.dumps(dictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()


class InfluxQueryDataset(AbstractDataset):
    """`ÌnfluxQueryDataset`` loads and saves data from/to an InfluxDB running
    server version 2.x.
    
    Data loading is done by executing a provided query. If the query returns
    mutiple results, only the first result is returned and a warning is logged.

    When writing data, the dataset expects a dataframe with a DatetimeIndex.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        influx_query_dataset:
          type: influx.InfluxQueryDataset
          credentials: db_credentials
          query:

    Sample database credentials entry in ``credentials.yml``:

    .. code-block:: yaml

        db_credentials:
          url: localhost:8086
          token: my_token_key
          org: my_org_hash
    """

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {}
    clients: Dict[str, InfluxDBClient] = {}

    def __init__(
        self,
        credentials: Dict[str, Any],
        query: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
    ):
        """Creates a new ``InfluxDBClient`` instance.

        Args:
            credentials: A dictionary with the credentials to connect to the
            database. Must contain the keys ``token``, ``org`` and
            ``url`` or ``host`` and ``port``.

        Raises:
            DatasetError: When credentials are incomplete.
        """

        if not "host" in credentials and (
            not "url" in credentials or not "port" in credentials
        ):
            raise DatasetError("Credentials must contain an url or host and port keys.")

        if not "token" in credentials:
            raise DatasetError("Credentials must contain a token key.")

        if not "org" in credentials:
            raise DatasetError("Credentials must contain an org key.")

        self._org = credentials["org"]
        self._query = query
        self._credentials = credentials

        if "url" not in credentials:
            self._credentials["url"] = f'{credentials["host"]}:{credentials["port"]}'

        self.create_connection(self._credentials)

        self._load_args = copy.deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = copy.deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

    @classmethod
    def create_connection(cls, credentials: Dict[str, Any]) -> None:
        con_hash = dict_hash(credentials)

        if con_hash not in cls.clients:
            cls.clients[con_hash] = InfluxDBClient(
                url=credentials["url"],
                token=credentials["token"],
                org=credentials["org"],
            )
            logger.debug("Created InfluxDB client.")

    def _load(self) -> pd.DataFrame:
        """Loads data from an influx query as a pandas dataframe using the
        ``query_data_frame`` method of the ``query_api`` from ``InfluxDBClient``.

        If ``load_args`` are provided they are passed to the ``query_data_frame``.

        Returns:
            Data from the influx query as a dataframe. If the query returns
            mutiple results, only the first result is returned and a warning is
            logged.
        """

        con_hash = dict_hash(self._credentials)

        try:
            logger.debug(f"Loading data from InfluxDB with query: {self._query}")
            result = (
                self.clients[con_hash]
                .query_api()
                .query_data_frame(self._query, **self._load_args)
            )
            if isinstance(result, list):
                logger.warning(
                    f"Querie returned more than one result. Returning first result."
                )
                return result[0]
            return result
        except Exception as ex:
            raise DatasetError(
                f"Failed to load data from InfluxDB with query: {self._query}.\n{ex}"
            )

    def _save(
        self, data: pd.DataFrame, bucket: str, measurement: str, columns: List[str]
    ) -> None:
        """Saves selected columns of a dataframe to the specified bucket and
        measurement using the ``write`` method of ``write_api`` from ``InfluxDBClient``.

        If ``save_args`` are provided they are passed to the ``write`` method.

        Args:
            data: Dataframe to save.
            bucket: Bucket to save the data to.
            measurement: Measurement to save the data to.
            columns: Columns to save.

        Raises:
            DatasetError: When the dataframe does not have a DatetimeIndex or
            when writing back to the database fails.
        """
        if not is_datetime64_any_dtype(data.index):
            raise DatasetError("DataFrame needs a DatetimeIndex.")

        con_hash = dict_hash(self._credentials)
        try:
            self.clients[con_hash].write_api().write(
                bucket=bucket,
                org=self._org,
                record=data,
                data_frame_measurement_name=measurement,
                data_frame_tag_columns=columns,
                **self._save_args,
            )
        except Exception as ex:
            raise DatasetError(f"Failed to save data to InfluxDB.\n{ex}")

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset"""
        return {"query": self._query, "credentials": self._credentials}
