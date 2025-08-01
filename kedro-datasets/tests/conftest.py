"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""

from collections.abc import Callable
from unittest.mock import MagicMock

import aiobotocore.awsrequest
import aiohttp
import aiohttp.client_reqrep
import aiohttp.typedefs
import botocore.awsrequest
import botocore.model
from kedro.io.core import generate_timestamp
from pytest import fixture

BUCKET_NAME = "test_bucket"
IP_ADDRESS = "127.0.0.1"
PORT = 5555
ENDPOINT_URI = f"http://{IP_ADDRESS}:{PORT}/"


"""
Patch aiobotocore to work with moto
See https://github.com/aio-libs/aiobotocore/issues/755
"""


class MockAWSResponse(aiobotocore.awsrequest.AioAWSResponse):
    def __init__(self, response: botocore.awsrequest.AWSResponse):
        self._moto_response = response
        self.status_code = response.status_code
        self.raw = MockHttpClientResponse(response)

    @property
    def headers(self) -> dict:
        # This is accessed directly in some cases
        return self._moto_response.headers

    # adapt async methods to use moto's response
    async def _content_prop(self) -> bytes:
        return self._moto_response.content

    async def _text_prop(self) -> str:
        return self._moto_response.text


class MockHttpClientResponse(aiohttp.client_reqrep.ClientResponse):
    def __init__(self, response: botocore.awsrequest.AWSResponse):
        async def read(self, n: int = -1) -> bytes:
            # streaming/range requests. used by s3fs
            return response.content

        self.content = MagicMock(aiohttp.StreamReader)
        self.content.read = read
        self.response = response
        self._headers = {
            k.encode("utf-8"): str(v).encode("utf-8")
            for k, v in self.response.headers.items()
        }

    @property
    def headers(self) -> dict:
        # This is required by aiobotocore during streaming
        return self.response.headers

    @property
    def raw_headers(self) -> aiohttp.typedefs.RawHeaders:
        return self._headers.items()


@fixture(scope="session", autouse=True)
def patch_aiobotocore():
    import aiobotocore.endpoint  # noqa: PLC0415

    def factory(original: Callable) -> Callable:
        def patched_convert_to_response_dict(
            http_response: botocore.awsrequest.AWSResponse,
            operation_model: botocore.model.OperationModel,
        ):
            return original(MockAWSResponse(http_response), operation_model)

        return patched_convert_to_response_dict

    aiobotocore.endpoint.convert_to_response_dict = factory(
        aiobotocore.endpoint.convert_to_response_dict
    )


@fixture(params=[None])
def load_version(request):
    return request.param


@fixture(params=[None])
def save_version(request):
    return request.param or generate_timestamp()


@fixture(params=[None])
def load_args(request):
    return request.param


@fixture(params=[None])
def save_args(request):
    return request.param


@fixture(params=[None])
def fs_args(request):
    return request.param
