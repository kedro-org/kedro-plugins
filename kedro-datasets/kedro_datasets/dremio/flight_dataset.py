import copy
from pathlib import Path, PurePosixPath
import fsspec
from pandas import DataFrame, concat 
from kedro.io.core import (
    AbstractDataSet,
    DataSetError,
    get_filepath_str,
    get_protocol_and_path,
)
from typing import Any, Dict, NoReturn, Optional
from pyarrow import flight

def process_con(uri, tls=False, user=None, password=None):
    """
    Extracts hostname, protocol, user and passworrd from URI

    Parameters
    ----------
    uri: str or None
        Connection string in the form username:password@hostname:port
    tls: boolean
        Whether TLS is enabled
    username: str or None
        Username if not supplied as part of the URI
    password: str or None
        Password if not supplied as part of the URI
    """
    if '://' in uri:
        protocol, uri = uri.split('://')
    else:
        protocol = 'grpc+tls' if tls else 'grpc+tcp'
    if '@' in uri:
        if user or password:
            raise ValueError(
                "Dremio URI must not include username and password "
                "if they were supplied explicitly."
            )
        userinfo, hostname = uri.split('@')
        user, password = userinfo.split(':')
    elif not (user and password):
        raise ValueError(
            "Flight URI must include username and password "
            "or they must be provided explicitly."
        )
    else:
        hostname = uri
    return protocol, hostname, user, password

class HttpClientAuthHandler(flight.ClientAuthHandler):

    def __init__(self, username, password):
        super(flight.ClientAuthHandler, self).__init__()
        self.basic_auth = flight.BasicAuth(username, password)
        self.token = None

    def authenticate(self, outgoing, incoming):
        auth = self.basic_auth.serialize()
        outgoing.write(auth)
        self.token = incoming.read()

    def get_token(self):
        return self.token


class ClientMiddleware(flight.ClientMiddleware):
    def __init__(self, factory):
        self.factory = factory

    def received_headers(self, headers):
        auth_header_key = 'authorization'
        authorization_header = []
        for key in headers:
          if key.lower() == auth_header_key:
            authorization_header = headers.get(auth_header_key)
        self.factory.set_call_credential([
            b'authorization', authorization_header[0].encode("utf-8")])


class ClientMiddlewareFactory(flight.ClientMiddlewareFactory):
    def __init__(self):
        self.call_credential = []

    def start_call(self, info):
        return ClientMiddleware(self)

    def set_call_credential(self, call_credential):
        self.call_credential = call_credential

 
class DremioFlightDataset(AbstractDataSet[pd.DataFrame, pd.DataFrame]):
    def __init__(  # pylint: disable=too-many-arguments
        self,
        sql: str = None,
        credentials: Dict[str, Any] = None,
        load_args: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
        filepath: str = None,
        execution_options: Optional[Dict[str, Any]] = None,
        metadata: Dict[str, Any] = None,
    ) -> None:
        """Creates a new ``DremioFlightDataset``.

        Args:
            sql: The sql query statement.
            credentials: A dictionary with a ``Flight`` connection string.
                Users are supposed to provide the connection string 'con'
                through credentials. It overwrites `con` parameter in
                ``load_args`` and ``save_args`` in case it is provided. To find
                all supported connection string formats, see here:
                https://docs.sqlalchemy.org/core/engines.html#database-urls
            load_args: Provided to underlying pandas ``read_sql_query``
                function along with the connection string.
                To find all supported arguments, see here:
                https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_sql_query.html
                To find all supported connection string formats, see here:
                https://docs.sqlalchemy.org/core/engines.html#database-urls
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``), as well as
                to pass to the filesystem's `open` method through nested keys
                `open_args_load` and `open_args_save`.
                Here you can find all available arguments for `open`:
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.open
                All defaults are preserved, except `mode`, which is set to `r` when loading.
            filepath: A path to a file with a sql query statement.
            execution_options: A dictionary with non-SQL advanced options for the connection to
                be applied to the underlying engine. To find all supported execution
                options, see here:
                https://docs.sqlalchemy.org/core/connections.html#sqlalchemy.engine.Connection.execution_options
                Note that this is not a standard argument supported by pandas API, but could be
                useful for handling large datasets.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DataSetError: When either ``sql`` or ``con`` parameters is empty.
        """
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
            "certs": None,
            "tls": False,
            "connect_timeout": None,
        }

        self._load_args = (
            {**default_load_args, **load_args}
            if load_args is not None
            else default_load_args
        )

        self.metadata = metadata

        if sql:
            self._load_args["sql"] = sql
            self._filepath = None
        else:
            _fs_args = copy.deepcopy(fs_args) or {}
            _fs_credentials = _fs_args.pop("credentials", {})
            protocol, path = get_protocol_and_path(str(filepath))

            self._protocol = protocol
            self._fs = fsspec.filesystem(self._protocol, **_fs_credentials, **_fs_args)
            self._filepath = path
        self._execution_options = execution_options or {}

        self._flight_con = credentials["con"]
        self._protocol, self._hostname, self._user, self._password = process_con(
            self._flight_con, tls=self._load_args["tls"]
        )

        if self._load_args["cert"] is not None and self._load_args["tls"]:
            with open(self._load_args["cert"], "rb") as root_certs:
                self._certs = root_certs.read()
        elif self._load_args["tls"]:
            raise ValueError('Trusted certificates must be provided to establish a TLS connection')
        else:
            self._certs = None

    def _get_reader(self):
        client_auth_middleware = ClientMiddlewareFactory()
        connection_args = {'middleware': [client_auth_middleware]}
        if self._load_args["tls"]:
            connection_args["tls_root_certs"] = self._certs
        client = flight.FlightClient(
            f'{self._protocol}://{self._hostname}',
            **connection_args
        )
        auth_options = flight.FlightCallOptions(timeout=self._connect_timeout)
        try:
            bearer_token = client.authenticate_basic_token(self._user, self._password)
            headers = [bearer_token]
        except Exception as e:
            if self._tls:
                raise e
            handler = HttpClientAuthHandler(self._user, self._password)
            client.authenticate(handler, options=auth_options)
            headers = []
        flight_desc = flight.FlightDescriptor.for_command(self._sql_expr)
        options = flight.FlightCallOptions(headers=headers, timeout=self._request_timeout)
        flight_info = client.get_flight_info(flight_desc, options)
        reader = client.do_get(flight_info.endpoints[0].ticket, options)
        return reader
    
    def _get_chunks(self, reader: flight.FlightStreamReader) -> DataFrame:
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

    def _load(self) -> DataFrame:
        return self._get_reader().read_pandas()

    def _save(self, _: None) -> NoReturn:
        raise DataSetError("'save' is not supported on DremioFlightDataset")

    def _exists(self) -> NoReturn:
        raise DataSetError("'exists' is not supported on DremioFlightDataset")

    def _describe(self):
        return self._get_reader().schema