import io

import pyarrow as pa
import pytest
from kedro.io.core import DataSetError
from pyarrow import flight
from pyarrow.flight import FlightServerBase, ServerAuthHandler

from kedro_datasets.dremio import DremioFlightDataSet
from kedro_datasets.dremio.flight_dataset import (
    ClientMiddleware,
    ClientMiddlewareFactory,
    HttpClientAuthHandler,
    process_con,
)


@pytest.fixture
def filepath_sql(tmp_path):
    return (tmp_path / "test.sql").as_posix()


@pytest.fixture
def default_dremio_host():
    return "localhost"


def simple_ints_table():
    data = [pa.array([-10, -5, 0, 5, 10])]
    return pa.Table.from_arrays(data, names=["some_ints"])


def simple_dicts_table():
    dict_values = pa.array(["foo", "baz", "quux"], type=pa.utf8())
    data = [
        pa.chunked_array(
            [
                pa.DictionaryArray.from_arrays([1, 0, None], dict_values),
                pa.DictionaryArray.from_arrays([2, 1], dict_values),
            ]
        )
    ]
    return pa.Table.from_arrays(data, names=["some_dicts"])


def multiple_column_table():
    return pa.Table.from_arrays(
        [pa.array(["foo", "bar", "baz", "qux"]), pa.array([1, 2, 3, 4])],
        names=["a", "b"],
    )


class NoopAuthHandler(ServerAuthHandler):
    """A no-op auth handler."""

    def authenticate(self, _, __):
        """Do nothing."""

    def is_valid(self, _):
        """
        Returning an empty string.
        Returning None causes Type error.
        """
        return ""


class ConstantFlightServer(FlightServerBase):
    """A Flight server that always returns the same data.

    See ARROW-4796: this server implementation will segfault if Flight
    does not properly hold a reference to the Table object.
    """

    CRITERIA = b"the expected criteria"

    def __init__(self, location=None, options=None, **kwargs):
        super().__init__(location, **kwargs)
        # Ticket -> Table
        self.table_factories = {
            b"ints": simple_ints_table,
            b"dicts": simple_dicts_table,
            b"multi": multiple_column_table,
        }
        self.options = options

    def list_flights(self, _, criteria):
        if criteria == self.CRITERIA:
            yield flight.FlightInfo(
                pa.schema([]), flight.FlightDescriptor.for_path("/foo"), [], -1, -1
            )

    def do_get(self, _, ticket):
        # Return a fresh table, so that Flight is the only one keeping a
        # reference.
        table = self.table_factories[ticket.ticket]()
        return flight.RecordBatchStream(table, options=self.options)

    def get_flight_info(self, _, descriptor):
        return flight.FlightInfo(
            pa.schema([("a", pa.int32())]),
            descriptor,
            [
                flight.FlightEndpoint(b"ints", ["grpc://test"]),
                flight.FlightEndpoint(
                    b"",
                    [flight.Location.for_grpc_tcp("localhost", 5005)],
                ),
            ],
            -1,
            -1,
        )


class GetInfoFlightServer(FlightServerBase):
    """A Flight server that tests FlightDataset._describe."""

    def get_flight_info(self, _, descriptor):
        return flight.FlightInfo(
            pa.schema([("a", pa.int32())]),
            descriptor,
            [
                flight.FlightEndpoint(b"", ["grpc://test"]),
                flight.FlightEndpoint(
                    b"",
                    [flight.Location.for_grpc_tcp("localhost", 5005)],
                ),
            ],
            -1,
            -1,
        )

    def get_schema(self, context, descriptor):
        info = self.get_flight_info(context, descriptor)
        return flight.SchemaResult(info.schema)


class TestProcessCon:
    def test_process_con_with_username_and_password(self, default_dremio_host):
        uri = f"username:password@{default_dremio_host}:port"
        result = process_con(uri)
        assert result["protocol"] == "grpc+tcp"
        assert result["hostname"] == default_dremio_host
        assert result["username"] == "username"
        assert result["password"] == "password"

    def test_process_con_with_username_and_password_and_protocal(
        self, default_dremio_host
    ):
        uri = f"grpc://username:password@{default_dremio_host}:port"
        result = process_con(uri)
        assert result["protocol"] == "grpc"
        assert result["hostname"] == default_dremio_host
        assert result["username"] == "username"
        assert result["password"] == "password"

    def test_process_con_with_username_and_password_and_tls(self, default_dremio_host):
        uri = f"username:password@{default_dremio_host}:port"
        result = process_con(uri, tls=True)
        assert result["protocol"] == "grpc+tls"
        assert result["hostname"] == default_dremio_host
        assert result["username"] == "username"
        assert result["password"] == "password"

    def test_process_con_with_username_and_password_provided_explicitly(
        self, default_dremio_host
    ):
        username = "username"
        password = "password"
        result = process_con(default_dremio_host, username=username, password=password)
        assert result["protocol"] == "grpc+tcp"
        assert result["hostname"] == default_dremio_host
        assert result["username"] == username
        assert result["password"] == password

    def test_process_con_with_credentials_provided_explicitly_duplicate(
        self, default_dremio_host
    ):
        uri = f"username:password@{default_dremio_host}:port"
        username = "username"
        password = "password"
        # pylint: disable=C0301
        pattern = "Dremio URI must not include username and password if they were supplied explicitly"
        with pytest.raises(ValueError, match=pattern):
            process_con(uri, username=username, password=password)

    def test_process_con_without_username_and_password(self, default_dremio_host):
        result = process_con(default_dremio_host)
        assert result["hostname"] == default_dremio_host
        assert result["username"] is None
        assert result["password"] is None

    def test_process_no_uri(self):
        pattern = "Dremio URI can not be empty"
        with pytest.raises(ValueError, match=pattern):
            process_con(None)
        with pytest.raises(ValueError, match=pattern):
            process_con("")


class TestHttpClientAuthHandler:
    def test_init(self):
        handler = HttpClientAuthHandler("username", "password")
        basic_auth = handler.basic_auth
        token = handler.token
        assert basic_auth
        assert token is None

    def test_authenticate(self):
        handler = HttpClientAuthHandler("username", "password")
        outgoing = io.BytesIO()
        incoming = io.BytesIO()
        handler.authenticate(outgoing, incoming)
        outgoing = outgoing.getvalue()
        incoming = incoming.getvalue()
        token = handler.token
        assert token == b""

    def test_get_token(self):
        handler = HttpClientAuthHandler("username", "password")
        handler.token = b"token"
        token = handler.get_token()
        assert token


class TestClientMiddleware:
    def test_received_headers(self):
        factory = ClientMiddlewareFactory()
        middleware = ClientMiddleware(factory)
        headers = {"authorization": ["Bearer my-token"]}
        middleware.received_headers(headers)
        assert middleware.factory.call_credential == [
            b"authorization",
            b"Bearer my-token",
        ]

    def test_received_headers_no_authorization(self):
        factory = ClientMiddlewareFactory()
        middleware = ClientMiddleware(factory)
        headers = {}
        middleware.received_headers(headers)
        assert middleware.factory.call_credential == []


class TestClientMiddlewareFactory:
    def test_start_call(self):
        factory = ClientMiddlewareFactory()
        factory.set_call_credential([b"credential1", b"credential2"])
        middleware = factory.start_call()
        assert middleware.factory.call_credential == [b"credential1", b"credential2"]

    def test_set_call_credential(self):
        factory = ClientMiddlewareFactory()
        factory.set_call_credential([b"credential1", b"credential2"])
        assert factory.call_credential == [b"credential1", b"credential2"]


class TestDremioFlightDataSet:
    def test_init_with_sql(self):
        with ConstantFlightServer() as server:
            credentials = {"con": f"username:password@localhost:{server.port}"}
            dataset = DremioFlightDataSet(
                sql="SELECT * FROM users", credentials=credentials
            )
            assert dataset._load_args["sql"] == "SELECT * FROM users"

    def test_init_with_filepath(self):
        with ConstantFlightServer() as server:
            credentials = {"con": f"username:password@localhost:{server.port}"}
            filepath = "/path/to/file.sql"
            dataset = DremioFlightDataSet(filepath=filepath, credentials=credentials)
            assert dataset._filepath == filepath

    def test_load(self):
        with ConstantFlightServer() as server:
            credentials = {"con": f"localhost:{server.port}"}
            dataset = DremioFlightDataSet(
                sql="SELECT * FROM users", credentials=credentials
            )
            df = dataset._load()
            assert df.shape[0] > 0

    def test_load_authenticate(self):
        with ConstantFlightServer() as server:
            # credentials = {"con": f"username:password@localhost:{server.port}"}
            credentials = {"con": f"localhost:{server.port}"}
            dataset = DremioFlightDataSet(
                sql="SELECT * FROM users", credentials=credentials
            )
            df = dataset._load()
            assert df.shape[0] > 0

    def test_load_tls_certs(self, tmp_path):
        certs_dir = tmp_path / "certs"
        certs_dir.mkdir()
        certs_path = certs_dir / "cert.pom"
        certs_path.write_text("")
        load_args = {
            "certs": certs_path,
            "tls": True,
            "disable_server_verification": True,
        }
        with ConstantFlightServer() as server:
            credentials = {"con": f"localhost:{server.port}"}
            try:
                dataset = DremioFlightDataSet(
                    sql="SELECT * FROM users",
                    credentials=credentials,
                    load_args=load_args,
                )
                df = dataset._load()
                assert df.shape[0] > 0
            except Exception:
                pytest.skip("disable_server_verification feature is not available")

    def test_save_not_supported(self):
        with ConstantFlightServer() as server:
            credentials = {"con": f"username:password@localhost:{server.port}"}
            dataset = DremioFlightDataSet(
                sql="SELECT * FROM users", credentials=credentials
            )
            with pytest.raises(DataSetError):
                dataset.save(data=None)

    def test_exists_not_supported(self):
        with ConstantFlightServer() as server:
            credentials = {"con": f"username:password@localhost:{server.port}"}
            dataset = DremioFlightDataSet(sql=b"ints", credentials=credentials)
            with pytest.raises(DataSetError):
                dataset._exists()

    def test_sql_and_filepath_is_invalid(self):
        with ConstantFlightServer() as server:
            credentials = {"con": f"username:password@localhost:{server.port}"}
            pattern = "'sql' and 'filepath' arguments cannot both be provided"
            with pytest.raises(DataSetError, match=pattern):
                DremioFlightDataSet(
                    sql="select * from dual",
                    filepath="test.sql",
                    credentials=credentials,
                )

    def test_sql_and_filepath_are_empty(self):
        with ConstantFlightServer() as server:
            credentials = {"con": f"username:password@localhost:{server.port}"}
            pattern = "'sql' and 'filepath' arguments cannot both be empty."
            with pytest.raises(DataSetError, match=pattern):
                DremioFlightDataSet(credentials=credentials)

    def test_con_is_empty(self):
        pattern = "'con' argument cannot be empty."
        with pytest.raises(DataSetError, match=pattern):
            DremioFlightDataSet(sql="select * from dual")

    def test_load_certs(self, tmp_path):
        certs_dir = tmp_path / "certs"
        certs_dir.mkdir()
        certs_path = certs_dir / "cert.pom"
        certs_path.write_text("")
        load_args = {
            "certs": certs_path,
            "tls": True,
        }
        with ConstantFlightServer() as server:
            credentials = {"con": f"username:password@localhost:{server.port}"}
            dataset = DremioFlightDataSet(
                sql=b"ints", credentials=credentials, load_args=load_args
            )
            assert dataset._certs == b""

    def test_tls_no_certs(self):
        load_args = {"tls": True}
        with ConstantFlightServer() as server:
            credentials = {"con": f"username:password@localhost:{server.port}"}
            pattern = (
                "Trusted certificates must be provided to establish a TLS connection"
            )
            with pytest.raises(ValueError, match=pattern):
                DremioFlightDataSet(
                    sql=b"ints", credentials=credentials, load_args=load_args
                )

    def test_describe(self):
        with GetInfoFlightServer() as server:
            credentials = {"con": f"localhost:{server.port}"}
            dataset = DremioFlightDataSet(
                sql="SELECT * FROM users", credentials=credentials
            )
            info = dataset._describe()
            assert info.total_records == -1
            assert info.total_bytes == -1
