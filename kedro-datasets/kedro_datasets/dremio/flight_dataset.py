""""
A module for reading Dremio Flight data.This module provides a class,
`FlightDataset`, that can be used to read Dremio Flight data."""
import copy
from pathlib import PurePosixPath
from typing import Any, Dict, List, NoReturn, Tuple

import fsspec
from kedro.io.core import (
    AbstractDataSet,
    DataSetError,
    get_filepath_str,
    get_protocol_and_path,
)
from pandas import DataFrame, concat
from pyarrow import flight
from pyarrow.flight import (
    FlightServerError,
    FlightUnauthenticatedError,
    FlightUnavailableError,
)


def process_credentials(credentials: Dict[str, str or None]) -> Dict[str, str or None]:
    """
    Extracts thostname, protocol, username, password, tls and disable_server_verification
    from credentials dictionary

    Args:
        credentials: Dictionary of kedro dataset credentials

    Returns:
        Dictionary containing the hostname, protocol, port, username, password,
        tls and disable_server_verification
    """
    if credentials is None:
        raise ValueError("Credentials dict can not be empty")

    uri, username, password, tls, disable_server_verification = (
        credentials.get("con"),
        credentials.get("username"),
        credentials.get("password"),
        credentials.get("tls"),
        credentials.get("disable_server_verification"),
    )

    if not uri:
        raise ValueError("Dremio URI can not be empty")

    if "://" in uri:
        protocol, uri = uri.split("://")
    else:
        protocol = "grpc+tls" if tls else "grpc+tcp"

    if "@" in uri:
        if username or password:
            raise ValueError(
                "Dremio URI must not include username and password "
                "if they were supplied explicitly."
            )
        userinfo, hostinfo = uri.split("@")
        username, password = userinfo.split(":")
    else:
        hostinfo = uri

    hostname, port = hostinfo.split(":") if ":" in hostinfo else (hostinfo, None)

    return {
        "protocol": protocol,
        "hostname": hostname,
        "port": port,
        "username": username,
        "password": password,
        "disable_server_verification": disable_server_verification,
        "tls": tls,
    }


class HttpClientAuthHandler(flight.ClientAuthHandler):
    """
    A client auth handler that uses HTTP Basic Auth to authenticate with a server.

    Args:
        username: The username to use for authentication.
        password: The password to use for authentication.

    Methods:
        authenticate: Called to authenticate with the server.
        get_token: Returns the token that was received from the server.

    Example:
        >>> handler = HttpClientAuthHandler("username", "password")
        >>> handler.authenticate(outgoing, incoming)
        >>> token = handler.get_token()
    """

    def __init__(self, username, password):
        super(flight.ClientAuthHandler, self).__init__()
        self.basic_auth = flight.BasicAuth(username, password)
        self.token = None

    def authenticate(
        self,
        outgoing: Any,
        incoming: Any,
    ) -> None:
        """
        Called to authenticate with the server.

        Args:
            outgoing: The outgoing stream.
            incoming: The incoming stream.
        """
        auth = self.basic_auth.serialize()
        outgoing.write(auth)
        self.token = incoming.read()

    def get_token(self) -> bytes:
        """
        Returns the token that was received from the server.

        Returns:
            The token.
        """
        return self.token


class ClientMiddleware(flight.ClientMiddleware):
    """
    A middleware that extracts the authorization header from an RPC response and sets it as
    the call credential for future RPCs.

    Args:
        factory: The factory that created this middleware instance.

    Methods:
        received_headers: Called after an RPC is received.

    Example:
        >>> middleware = ClientMiddleware(factory)
        >>> middleware.received_headers(headers)
    """

    def __init__(self, factory: flight.ClientMiddlewareFactory):
        self.factory = factory

    def received_headers(self, headers: Dict[str, Any]):
        """
        Called after an RPC is received.

        Args:
            headers: The RPC response headers.

        """
        auth_header_key = "authorization"
        authorization_headers = []
        for key in headers:
            if key.lower() == auth_header_key:
                authorization_headers = headers.get(auth_header_key)

        if len(authorization_headers) > 0:
            self.factory.set_call_credential(
                [b"authorization", authorization_headers[0].encode("utf-8")]
            )


class ClientMiddlewareFactory(flight.ClientMiddlewareFactory):
    """
    A factory for new middleware instances.

    Args:
        call_credential: A list of call credentials.

    Methods:
        start_call: Called at the start of an RPC.
        set_call_credential: Sets the call credential.

    Example:
        >>> factory = ClientMiddlewareFactory()
        >>> factory.set_call_credential(['credential1', 'credential2'])
        >>> middleware = factory.start_call(None)
    """

    def __init__(self) -> None:
        self.call_credential = []

    def start_call(self, info) -> ClientMiddleware:
        """
        Called at the start of an RPC.

        Returns:
            A ClientMiddleware instance.
        """
        return ClientMiddleware(self)

    def set_call_credential(self, call_credential: List[bytes]) -> None:
        """
        Sets the call credential.

        Args:
            call_credential: A list of call credentials.
        """
        self.call_credential = call_credential


class DremioFlightDataSet(
    AbstractDataSet[None, DataFrame or flight.FlightStreamReader]
):
    """``DremioFlightDataSet`` loads data from a py arrow flight.
    Since it uses ``pyarrow.flight`` internally,behind the scenes,
    when instantiating ``DremioFlightDataSet`` one needs to pass a compatible connection
    string either in ``credentials`` (see the example code snippet below) or in
    ``load_args`.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

    .. code-block:: yaml

        shuttles_table_dataset:
          type: pandas.DremioFlightDataSet
          credentials: flight_credentials
          table_name: shuttles
          load_args:
            schema: dwschema
          save_args:
            schema: dwschema
            if_exists: replace

    Sample database credentials entry in ``credentials.yml``:

    .. code-block:: yaml

        flight_credentials:
          con: grpc+tls://{username}:{password}@localhost:{port}

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-code-api>`_:
    ::
        >>> from kedro_datasets.dremio import DremioFlightDataSet
        >>> credentials = {
        >>>     "con": "postgresql://scott:tiger@localhost/test"
        >>> }
        >>> data_set = DremioFlightDataSet(sql="select * from dual",
        >>>                            credentials=credentials)
        >>> data = data_set.load()

    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        sql: str = None,
        credentials: Dict[str, Any] = None,
        load_args: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
        filepath: str = None,
    ) -> None:
        """Creates a new ``DremioFlightDataSet``.

        Args:
            sql: The sql query statement.
            credentials: A dictionary with a ``Flight`` connection string.
                Users are supposed to provide the connection string 'con'
                through credentials. It overwrites `con` parameter in
                ``load_args`` and ``save_args`` in case it is provided. To find
                all supported connection string formats, see here:
                https://docs.sqlalchemy.org/core/engines.html#database-urls
            load_args: Provided to underlying pyarrow.FlightClinet client constructor/init
                method. For more on all available arguments for FlightClient:
                https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightClient.html
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``), as well as
                to pass to the filesystem's `open` method through nested keys
                `open_args_load` and `open_args_save`.
                Here you can find all available arguments for `open`:
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.open
                All defaults are preserved, except `mode`, which is set to `r` when loading.
            filepath: A path to a file with a sql query statement.

        Raises:
            DataSetError: When either ``sql`` or ``con`` parameters is empty.
        """
        self._client = None
        if sql and filepath:
            raise DataSetError(
                "'sql' and 'filepath' arguments cannot both be provided."
                "Please only provide one."
            )

        if not (sql or filepath):
            raise DataSetError(
                "'sql' and 'filepath' arguments cannot both be empty."
                "Please provide a sql query or path to a sql query file."
            )

        if not (credentials and "con" in credentials and credentials["con"]):
            raise DataSetError(
                "'con' argument cannot be empty. Please "
                "provide a pyarrow.flight connection string."
            )

        default_load_args: Dict[str, Any] = {
            "connect_timeout": None,
            "return_pandas": True,
        }

        self._load_args = (
            {**default_load_args, **load_args}
            if load_args is not None
            else default_load_args
        )

        if sql:
            self._load_args["sql"] = sql
            self._filepath = None
        else:
            _fs_args = copy.deepcopy(fs_args) or {}
            _fs_credentials = _fs_args.pop("credentials", {})
            self._protocol, self._filepath = get_protocol_and_path(str(filepath))

            self._fs = fsspec.filesystem(self._protocol, **_fs_credentials, **_fs_args)

        default_credentials: Dict[str, Any] = {
            "certs": None,
            "tls": False,
            "disable_server_verification": False,
            "username": None,
            "password": None,
        }

        _credentials = (
            {**default_credentials, **copy.deepcopy(credentials)}
            if credentials is not None
            else default_credentials
        )

        self._flight_con = process_credentials(credentials)

        if _credentials.get("certs", None) and _credentials.get("tls", False):
            with open(_credentials.get("certs", None), "rb") as root_certs:
                self._certs = root_certs.read()
        elif _credentials.get("tls", False):
            raise ValueError(
                "Trusted certificates must be provided to establish a TLS connection"
            )
        else:
            self._certs = None

    def _get_hostname(self) -> Tuple[str, bool]:
        hostname = self._flight_con.get("hostname", None)
        if self._flight_con.get("port", None):
            hostname = f"{hostname}:{self._flight_con.get('port', None)}"
        autheticate = self._flight_con.get("username", None) and self._flight_con.get(
            "password", None
        )
        return hostname, autheticate

    def _get_client(self) -> Tuple[flight.FlightClient, bool]:
        hostname, authenticate = self._get_hostname()
        connection_args = {}
        if self._flight_con.get("tls", False):
            connection_args["tls_root_certs"] = self._certs
        if self._flight_con.get("disable_server_verification", False):
            connection_args["disable_server_verification"] = self._flight_con.get(
                "disable_server_verification", False
            )
        if authenticate:
            client_auth_middleware = ClientMiddlewareFactory()
            connection_args["middleware"] = [client_auth_middleware]
        client = flight.FlightClient(
            f"{self._flight_con['protocol']}://{hostname}", **connection_args
        )

        self._client = client

        return client, authenticate

    @staticmethod
    def _get_chunks(reader: flight.FlightStreamReader) -> DataFrame:
        dataframe = DataFrame()
        while True:
            try:
                flight_batch = reader.read_chunk()
                record_batch = flight_batch.data
                data_to_pandas = record_batch.to_pandas()
                dataframe = concat([dataframe, data_to_pandas])
            except StopIteration:
                break

        return dataframe

    def _load_authenticated(
        self, load_args: Dict[str, Any], client: flight.FlightClient
    ) -> DataFrame or flight.FlightStreamReader:
        auth_options = flight.FlightCallOptions(
            timeout=load_args.get("connect_timeout")
        )
        try:
            bearer_token = client.authenticate_basic_token(
                self._flight_con["username"], self._flight_con["password"], auth_options
            )
            headers = [bearer_token]
        except (
            FlightUnauthenticatedError,
            FlightUnavailableError,
            FlightServerError,
            ConnectionError,
            TimeoutError,
        ):
            handler = HttpClientAuthHandler(
                self._flight_con["username"], self._flight_con["password"]
            )
            client.authenticate(handler, options=auth_options)
            headers = []

        headers.append((b"routing_tag", b"test-routing-tag"))
        headers.append((b"routing_queue", b"Low Cost User Queries"))

        flight_desc = flight.FlightDescriptor.for_command(load_args["sql"])
        options = flight.FlightCallOptions(
            headers=headers, timeout=load_args.get("request_timeout", 9000)
        )
        flight_info = client.get_flight_info(flight_desc, options)
        reader = client.do_get(flight_info.endpoints[0].ticket, options)
        return (
            self._get_chunks(reader)
            if self._load_args.get("return_pandas", True)
            else reader
        )

    def _load(self) -> DataFrame or flight.FlightStreamReader:
        load_args = copy.deepcopy(self._load_args)
        if self._filepath:
            load_path = get_filepath_str(PurePosixPath(self._filepath), self._protocol)
            with self._fs.open(load_path, mode="r") as fs_file:
                load_args["sql"] = fs_file.read()
        client, authenticate = self._get_client()

        if authenticate:
            return self._load_authenticated(load_args, client)

        flight_desc = flight.FlightDescriptor.for_command(load_args["sql"])
        options = flight.FlightCallOptions()
        flight_info = client.get_flight_info(flight_desc, options)
        reader = client.do_get(flight_info.endpoints[0].ticket, options)
        return (
            self._get_chunks(reader)
            if self._load_args.get("return_pandas", True)
            else reader
        )

    def _save(self, _: None) -> NoReturn:
        raise DataSetError("'save' is not supported on DremioFlightDataSet")

    def _exists(self) -> NoReturn:
        raise DataSetError("'exists' is not supported on DremioFlightDataSet")

    def _describe(self):
        client, _ = self._get_client()

        return client.get_flight_info(
            flight.FlightDescriptor.for_command(self._load_args["sql"])
        )

    def close(self) -> None:
        """
        Closes the client and disconnect.
        """
        if self._client:
            self._client.close()
