import io

import pytest

from kedro_datasets.dremio import DremioFlightDataSet
from kedro_datasets.dremio.flight_dataset import HttpClientAuthHandler, process_con


@pytest.fixture
def default_dremio_host():
    return "localhost"


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
        # pylint: disable=C0301
        pattern = "Flight URI must include username and password or they must be provided explicitly."
        with pytest.raises(ValueError, match=pattern):
            process_con(default_dremio_host)

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


class TestDremioFlightDataSet:
    def test_1(self):
        ...
