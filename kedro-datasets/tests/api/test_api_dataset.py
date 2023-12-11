import base64
import json
import socket
from typing import Any

import pytest
import requests
from kedro.io.core import DatasetError
from requests.auth import HTTPBasicAuth

from kedro_datasets.api import APIDataset

POSSIBLE_METHODS = ["GET", "OPTIONS", "HEAD", "POST", "PUT", "PATCH", "DELETE"]
SAVE_METHODS = ["POST", "PUT"]

TEST_URL = "http://example.com/api/test"
TEST_TEXT_RESPONSE_DATA = "This is a response."
TEST_JSON_REQUEST_DATA = [{"key": "value"}]
TEST_JSON_RESPONSE_DATA = [{"key": "value"}]

TEST_PARAMS = {"param": "value"}
TEST_URL_WITH_PARAMS = TEST_URL + "?param=value"
TEST_METHOD = "GET"
TEST_HEADERS = {"key": "value"}

TEST_SAVE_DATA = [{"key1": "info1", "key2": "info2"}]


class TestAPIDataset:
    @pytest.mark.parametrize("method", POSSIBLE_METHODS)
    def test_request_method(self, requests_mock, method):
        if method in ["OPTIONS", "HEAD", "PATCH", "DELETE"]:
            with pytest.raises(
                ValueError,
                match="Only GET, POST and PUT methods are supported",
            ):
                APIDataset(url=TEST_URL, method=method)

        else:
            api_dataset = APIDataset(url=TEST_URL, method=method)

            requests_mock.register_uri(method, TEST_URL, text=TEST_TEXT_RESPONSE_DATA)

            if method == "GET":
                response = api_dataset.load()
                assert response.text == TEST_TEXT_RESPONSE_DATA
            else:
                with pytest.raises(
                    DatasetError, match="Only GET method is supported for load"
                ):
                    api_dataset.load()

    @pytest.mark.parametrize(
        "parameters_in, url_postfix",
        [
            ({"param": "value"}, "?param=value"),
            (bytes("a=1", "latin-1"), "?a=1"),
        ],
    )
    def test_params_in_request(self, requests_mock, parameters_in, url_postfix):
        api_dataset = APIDataset(
            url=TEST_URL, method=TEST_METHOD, load_args={"params": parameters_in}
        )
        requests_mock.register_uri(
            TEST_METHOD, TEST_URL + url_postfix, text=TEST_TEXT_RESPONSE_DATA
        )

        response = api_dataset.load()
        assert isinstance(response, requests.Response)
        assert response.text == TEST_TEXT_RESPONSE_DATA

    def test_json_in_request(self, requests_mock):
        api_dataset = APIDataset(
            url=TEST_URL,
            method=TEST_METHOD,
            load_args={"json": TEST_JSON_REQUEST_DATA},
        )
        requests_mock.register_uri(TEST_METHOD, TEST_URL)

        response = api_dataset.load()
        assert response.request.json() == TEST_JSON_REQUEST_DATA

    def test_headers_in_request(self, requests_mock):
        api_dataset = APIDataset(
            url=TEST_URL, method=TEST_METHOD, load_args={"headers": TEST_HEADERS}
        )
        requests_mock.register_uri(TEST_METHOD, TEST_URL, headers={"pan": "cake"})

        response = api_dataset.load()

        assert response.request.headers["key"] == "value"
        assert response.headers["pan"] == "cake"

    def test_api_cookies(self, requests_mock):
        api_dataset = APIDataset(
            url=TEST_URL, method=TEST_METHOD, load_args={"cookies": {"pan": "cake"}}
        )
        requests_mock.register_uri(TEST_METHOD, TEST_URL, text="text")

        response = api_dataset.load()
        assert response.request.headers["Cookie"] == "pan=cake"

    def test_credentials_auth_error(self):
        """
        If ``auth`` in ``load_args`` and ``credentials`` are both provided,
        the constructor should raise a ValueError.
        """
        with pytest.raises(ValueError, match="both auth and credentials"):
            APIDataset(
                url=TEST_URL, method=TEST_METHOD, load_args={"auth": []}, credentials={}
            )

    @staticmethod
    def _basic_auth(username, password):
        encoded = base64.b64encode(f"{username}:{password}".encode("latin-1"))
        return f"Basic {encoded.decode('latin-1')}"

    @pytest.mark.parametrize(
        "auth_kwarg",
        [
            {"load_args": {"auth": ("john", "doe")}},
            {"load_args": {"auth": ["john", "doe"]}},
            {"load_args": {"auth": HTTPBasicAuth("john", "doe")}},
            {"credentials": ("john", "doe")},
            {"credentials": ["john", "doe"]},
            {"credentials": HTTPBasicAuth("john", "doe")},
        ],
    )
    def test_auth_sequence(self, requests_mock, auth_kwarg):
        api_dataset = APIDataset(url=TEST_URL, method=TEST_METHOD, **auth_kwarg)
        requests_mock.register_uri(
            TEST_METHOD,
            TEST_URL,
            text=TEST_TEXT_RESPONSE_DATA,
        )

        response = api_dataset.load()
        assert isinstance(response, requests.Response)
        assert response.request.headers["Authorization"] == TestAPIDataset._basic_auth(
            "john", "doe"
        )
        assert response.text == TEST_TEXT_RESPONSE_DATA

    @pytest.mark.parametrize(
        "timeout_in, timeout_out",
        [
            (1, 1),
            ((1, 2), (1, 2)),
            ([1, 2], (1, 2)),
        ],
    )
    def test_api_timeout(self, requests_mock, timeout_in, timeout_out):
        api_dataset = APIDataset(
            url=TEST_URL, method=TEST_METHOD, load_args={"timeout": timeout_in}
        )
        requests_mock.register_uri(TEST_METHOD, TEST_URL)
        response = api_dataset.load()
        assert response.request.timeout == timeout_out

    def test_stream(self, requests_mock):
        text = "I am being streamed."

        api_dataset = APIDataset(
            url=TEST_URL, method=TEST_METHOD, load_args={"stream": True}
        )

        requests_mock.register_uri(TEST_METHOD, TEST_URL, text=text)

        response = api_dataset.load()
        assert isinstance(response, requests.Response)
        assert response.request.stream

        chunks = list(response.iter_content(chunk_size=2, decode_unicode=True))
        assert chunks == ["I ", "am", " b", "ei", "ng", " s", "tr", "ea", "me", "d."]

    def test_proxy(self, requests_mock):
        api_dataset = APIDataset(
            url="ftp://example.com/api/test",
            method=TEST_METHOD,
            load_args={"proxies": {"ftp": "ftp://127.0.0.1:3000"}},
        )
        requests_mock.register_uri(
            TEST_METHOD,
            "ftp://example.com/api/test",
        )

        response = api_dataset.load()
        assert response.request.proxies.get("ftp") == "ftp://127.0.0.1:3000"

    @pytest.mark.parametrize(
        "cert_in, cert_out",
        [
            (("cert.pem", "privkey.pem"), ("cert.pem", "privkey.pem")),
            (["cert.pem", "privkey.pem"], ("cert.pem", "privkey.pem")),
            ("some/path/to/file.pem", "some/path/to/file.pem"),
            (None, None),
        ],
    )
    def test_certs(self, requests_mock, cert_in, cert_out):
        api_dataset = APIDataset(
            url=TEST_URL, method=TEST_METHOD, load_args={"cert": cert_in}
        )
        requests_mock.register_uri(TEST_METHOD, TEST_URL)
        response = api_dataset.load()
        assert response.request.cert == cert_out

    def test_exists_http_error(self, requests_mock):
        """
        In case of an unexpected HTTP error,
        ``exists()`` should not silently catch it.
        """
        api_dataset = APIDataset(
            url=TEST_URL,
            method=TEST_METHOD,
            load_args={"params": TEST_PARAMS, "headers": TEST_HEADERS},
        )
        requests_mock.register_uri(
            TEST_METHOD,
            TEST_URL_WITH_PARAMS,
            headers=TEST_HEADERS,
            text="Nope, not found",
            status_code=requests.codes.FORBIDDEN,
        )
        with pytest.raises(DatasetError, match="Failed to fetch data"):
            api_dataset.exists()

    def test_exists_ok(self, requests_mock):
        """
        If the file actually exists and server responds 200,
        ``exists()`` should return True
        """
        api_dataset = APIDataset(
            url=TEST_URL,
            method=TEST_METHOD,
            load_args={"params": TEST_PARAMS, "headers": TEST_HEADERS},
        )
        requests_mock.register_uri(
            TEST_METHOD,
            TEST_URL_WITH_PARAMS,
            headers=TEST_HEADERS,
            text=TEST_TEXT_RESPONSE_DATA,
        )

        assert api_dataset.exists()

    def test_http_error(self, requests_mock):
        api_dataset = APIDataset(
            url=TEST_URL,
            method=TEST_METHOD,
            load_args={"params": TEST_PARAMS, "headers": TEST_HEADERS},
        )
        requests_mock.register_uri(
            TEST_METHOD,
            TEST_URL_WITH_PARAMS,
            headers=TEST_HEADERS,
            text="Nope, not found",
            status_code=requests.codes.FORBIDDEN,
        )

        with pytest.raises(DatasetError, match="Failed to fetch data"):
            api_dataset.load()

    def test_socket_error(self, requests_mock):
        api_dataset = APIDataset(
            url=TEST_URL,
            method=TEST_METHOD,
            load_args={"params": TEST_PARAMS, "headers": TEST_HEADERS},
        )
        requests_mock.register_uri(TEST_METHOD, TEST_URL_WITH_PARAMS, exc=socket.error)

        with pytest.raises(DatasetError, match="Failed to connect"):
            api_dataset.load()

    @pytest.mark.parametrize("method", POSSIBLE_METHODS)
    @pytest.mark.parametrize(
        "data",
        [TEST_SAVE_DATA, json.dumps(TEST_SAVE_DATA)],
        ids=["data_as_dict", "data_as_json_string"],
    )
    def test_successful_save(self, requests_mock, method, data):
        """
        When we want to save some data on a server
        Given an APIDataset class
        Then check that the response is OK and the sent data is in the correct form.
        """

        def json_callback(request: requests.Request, context: Any) -> dict:
            """Callback that sends back the json."""
            return request.json()

        if method in ["PUT", "POST"]:
            api_dataset = APIDataset(
                url=TEST_URL,
                method=method,
                save_args={"params": TEST_PARAMS, "headers": TEST_HEADERS},
            )
            requests_mock.register_uri(
                method,
                TEST_URL_WITH_PARAMS,
                headers=TEST_HEADERS,
                status_code=requests.codes.ok,
                json=json_callback,
            )
            response = api_dataset._save(data)
            assert isinstance(response, requests.Response)
            assert response.json() == TEST_SAVE_DATA

        elif method == "GET":
            api_dataset = APIDataset(
                url=TEST_URL,
                method=method,
                save_args={"params": TEST_PARAMS, "headers": TEST_HEADERS},
            )
            with pytest.raises(DatasetError, match="Use PUT or POST methods for save"):
                api_dataset._save(TEST_SAVE_DATA)
        else:
            with pytest.raises(
                ValueError,
                match="Only GET, POST and PUT methods are supported",
            ):
                APIDataset(url=TEST_URL, method=method)

    @pytest.mark.parametrize("save_methods", SAVE_METHODS)
    def test_successful_save_with_json(self, requests_mock, save_methods):
        """
        When we want to save with json parameters
        Given an APIDataset class
        Then check we get a response
        """

        def json_callback(request: requests.Request, context: Any) -> dict:
            """Callback that sends back the json."""
            return request.json()

        api_dataset = APIDataset(
            url=TEST_URL,
            method=save_methods,
            save_args={"json": TEST_JSON_RESPONSE_DATA, "headers": TEST_HEADERS},
        )
        requests_mock.register_uri(
            save_methods,
            TEST_URL,
            headers=TEST_HEADERS,
            json=json_callback,
        )
        response_list = api_dataset._save(TEST_SAVE_DATA)
        assert isinstance(response_list, requests.Response)
        # check that the data was sent in the correct format
        assert response_list.json() == TEST_SAVE_DATA

        response_dict = api_dataset._save({"item1": "key1"})
        assert isinstance(response_dict, requests.Response)
        assert response_dict.json() == {"item1": "key1"}

        response_json = api_dataset._save(TEST_SAVE_DATA[0])
        assert isinstance(response_json, requests.Response)
        assert response_json.json() == TEST_SAVE_DATA[0]

    @pytest.mark.parametrize("save_methods", SAVE_METHODS)
    def test_save_http_error(self, requests_mock, save_methods):
        api_dataset = APIDataset(
            url=TEST_URL,
            method=save_methods,
            save_args={"params": TEST_PARAMS, "headers": TEST_HEADERS, "chunk_size": 2},
        )
        requests_mock.register_uri(
            save_methods,
            TEST_URL_WITH_PARAMS,
            headers=TEST_HEADERS,
            text="Nope, not found",
            status_code=requests.codes.FORBIDDEN,
        )

        with pytest.raises(DatasetError, match="Failed to send data"):
            api_dataset.save(TEST_SAVE_DATA)

        with pytest.raises(DatasetError, match="Failed to send data"):
            api_dataset.save(TEST_SAVE_DATA[0])

    @pytest.mark.parametrize("save_methods", SAVE_METHODS)
    def test_save_socket_error(self, requests_mock, save_methods):
        api_dataset = APIDataset(
            url=TEST_URL,
            method=save_methods,
            save_args={"params": TEST_PARAMS, "headers": TEST_HEADERS},
        )
        requests_mock.register_uri(save_methods, TEST_URL_WITH_PARAMS, exc=socket.error)

        with pytest.raises(
            DatasetError, match="Failed to connect to the remote server"
        ):
            api_dataset.save(TEST_SAVE_DATA)

        with pytest.raises(
            DatasetError, match="Failed to connect to the remote server"
        ):
            api_dataset.save(TEST_SAVE_DATA[0])
