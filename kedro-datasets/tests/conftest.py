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

BUCKET_NAME = "test_bucket"
IP_ADDRESS = "127.0.0.1"
PORT = 5555
ENDPOINT_URI = f"http://{IP_ADDRESS}:{PORT}/"


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
def moto_server():
    # This fixture is module-scoped, meaning that we can re-use the MotoServer across all tests
    server = ThreadedMotoServer(ip_address=IP_ADDRESS, port=PORT)
    server.start()

    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "fake_access_key"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "fake_secret_key"

    yield

    server.stop()


def _reset_moto_server():
    # We reuse the MotoServer for all S3 related tests
    # But we do want a clean state for every test
    requests.post(f"{ENDPOINT_URI}/moto-api/reset")


def _get_boto3_client():
    from botocore.session import Session

    # NB: we use the sync botocore client for setup
    session = Session()
    return session.create_client(service_name="s3", endpoint_url=ENDPOINT_URI)


@fixture
def mocked_s3_bucket(moto_server):
    """Create a bucket for testing using moto."""
    _reset_moto_server()
    client = _get_boto3_client()
    client.create_bucket(Bucket=BUCKET_NAME)
    yield client


@fixture
def mocked_encrypted_s3_bucket(moto_server):
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
    _reset_moto_server()
    client = _get_boto3_client()
    client.create_bucket(Bucket=BUCKET_NAME)
    client.put_bucket_policy(Bucket=BUCKET_NAME, Policy=bucket_policy)
    yield client
