"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""
import json
import os

import requests
from kedro.io.core import generate_timestamp
from moto.moto_server.threaded_moto_server import ThreadedMotoServer
from pytest import fixture
from typing import Callable
from unittest.mock import MagicMock

import aiobotocore.awsrequest
import aiobotocore.endpoint
import aiohttp
import aiohttp.client_reqrep
import aiohttp.typedefs
import botocore.awsrequest
import botocore.model
import pytest

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

    @property
    def raw_headers(self) -> aiohttp.typedefs.RawHeaders:
        # Return the headers encoded the way that aiobotocore expects them
        return {
            k.encode("utf-8"): str(v).encode("utf-8")
            for k, v in self.response.headers.items()
        }.items()


@fixture()
def patch_aiobotocore():
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


@fixture(params=[None])
def mock_fs_args(request):
    fs_args = {
        # NB: use moto server to mock S3
        "client_kwargs": {"endpoint_url": ENDPOINT_URI}
    }

    if isinstance(request.param, dict):
        fs_args.update(request.param)

    return fs_args


@fixture
def credentials():
    return {
        "key": "fake_access_key",
        "secret": "fake_secret_key",
    }


@fixture(scope="session")
def moto_server(patch_aiobotocore):
    # This fixture is module-scoped, meaning that we can re-use the MotoServer across all tests
    server = ThreadedMotoServer(ip_address=IP_ADDRESS, port=PORT)
    server.start()

    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "fake_access_key"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "fake_secret_key"

    yield

    server.stop()


def _reset_moto_server(patch_aiobotocore):
    # We reuse the MotoServer for all S3 related tests
    # But we do want a clean state for every test
    requests.post(f"{ENDPOINT_URI}/moto-api/reset", timeout=2.0)


def _get_boto3_client(patch_aiobotocore):
    from botocore.session import Session  # pylint: disable=import-outside-toplevel

    # NB: we use the sync botocore client for setup
    session = Session()
    return session.create_client(service_name="s3", endpoint_url=ENDPOINT_URI)


@fixture
def mocked_s3_bucket(patch_aiobotocore, moto_server):  # pylint: disable=unused-argument
    """Create a bucket for testing using moto."""
    _reset_moto_server(patch_aiobotocore)
    client = _get_boto3_client(patch_aiobotocore)
    client.create_bucket(Bucket=BUCKET_NAME)
    yield client


@fixture
def mocked_encrypted_s3_bucket(patch_aiobotocore, moto_server):  # pylint: disable=unused-argument
    bucket_policy = {
        "Version": "2012-10-17",
        "Id": "PutObjPolicy",
        "Statement": [
            {
                "Sid": "DenyUnEncryptedObjectUploads",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}/*",
                "Condition": {"Null": {"s3:x-amz-server-side-encryption": "aws:kms"}},
            }
        ],
    }
    bucket_policy = json.dumps(bucket_policy)
    _reset_moto_server(patch_aiobotocore)
    client = _get_boto3_client(patch_aiobotocore)
    client.create_bucket(Bucket=BUCKET_NAME)
    client.put_bucket_policy(Bucket=BUCKET_NAME, Policy=bucket_policy)
    yield client
