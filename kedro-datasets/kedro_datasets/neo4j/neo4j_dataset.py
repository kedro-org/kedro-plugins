"""``Neo4jQueryDataSet`` loads data from a provided Cypher query.
"""

import copy
import hashlib
import json
import logging
from typing import Any, Dict

import pandas as pd
from neo4j import GraphDatabase, Driver

from .._io import AbstractDataset, DatasetError

__all__ = ["Neo4jCypherDataset"]

logger = logging.getLogger(__name__)


# source https://www.doc.ic.ac.uk/~nuric/coding/how-to-hash-a-dictionary-in-python.html
def dict_hash(dictionary: Dict[str, Any]) -> str:
    """MD5 hash of a dictionary."""
    dhash = hashlib.md5()
    encoded = json.dumps(dictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()


class Neo4jCypherDataset(AbstractDataset[pd.DataFrame, pd.DataFrame]):
    """``Neo4jQueryDataSet`` loads data from a provided Cypher query.

    It does not support save method so it is a read-only data set.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        neo4j_dataset:
          type: neo4j.Neo4jCypherDataset
          credentials: db_credentials
          cypher: MATCH (n:Person) RETURN n.name as name, n.age as age

    Sample database credentials entry in ``credentials.yml``:

    .. code-block:: yaml

        db_credentials:
          url: neo4j://localhost:8086
          user: neo4j
          password: neo4j
    """

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {}
    clients: Dict[str, Driver] = {}

    def __init__(
        self,
        credentials: Dict[str, Any] = None,
        cypher: str = None,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new ``Neo4JCypherDataset`` instance.

        Args:
            credentials: A dictionary with the credentials to connect to the
            database. Must contain the keys ``user``, ``password`` and
            ``url`` or ``host`` and ``port``.
            cypher: A cypher query string.
            load_args: Additional arguments passed on to ``neo4j.Session.run``.


        Raises:
            DatasetError: When credentials are incomplete.
        """

        if credentials is None:
            raise DatasetError("`credentials` argument must be a dictionary")

        self._credentials = credentials

        if not "url" in credentials and (
            not "host" in credentials or not "port" in credentials
        ):
            raise DatasetError("Credentials must contain an url or host and port keys")

        if "url" not in credentials:
            self._credentials["url"] = f'{credentials["host"]}:{credentials["port"]}'

        if "user" not in credentials or "password" not in credentials:
            raise DatasetError(
                "`credentials` argument must be None or dictionary"
                "like {'user': <user>, 'password': <password>}"
            )

        self.create_connection(self._credentials)

        if cypher is None:
            raise DatasetError("`cypher` argument must be a valid cypher query string.")
        self._cypher = cypher

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
            cls.clients[con_hash] = GraphDatabase.driver(
                uri=credentials["url"],
                auth=(credentials["user"], credentials["password"]),
            )
            logger.debug("Created Neo4J client.")
        if cls.clients[con_hash].verify_authentication():
            logger.debug("Verified Neo4J client authentication.")
        else:
            raise DatasetError("Could not verify Neo4J client authentication.")

    def _describe(self) -> Dict[str, Any]:
        return {
            "cypher": self._cypher,
            "url": self._credentials["url"],
            "auth": (self._credentials["user"], self._credentials["password"]),
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    def _load(self):
        con_hash = dict_hash(self._credentials)
        result = None

        try:
            with self.clients[con_hash].session() as session:
                result = session.run(self._cypher, **self._load_args).data()
                # convert list of results to pandas dataframe if only one key is provided
                if len(result[0].keys()) == 1:
                    key = list(result[0].keys())[0]
                    result = list(map(lambda x: x[key], result))
                result = pd.DataFrame.from_records(result)
            return result
        except Exception as ex:
            raise DatasetError(
                f"Failed to load data from Neo4J with cypher: {self._cypher}.\n{ex}"
            )

    def _save(self, data: pd.DataFrame) -> None:
        raise DatasetError("`save` is not supported on Neo4jQueryDataSet")
