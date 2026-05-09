"""Tests for the send_individually parameter in APIDataset."""
import json as json_module
from typing import Any

import pytest
import requests

from kedro_datasets.api import APIDataset

TEST_URL = "http://example.com/api/test"
SAVE_METHODS = ["POST", "PUT"]


class TestAPIDatasetSendIndividually:
    """Test suite for the send_individually feature."""

    @pytest.mark.parametrize("method", SAVE_METHODS)
    def test_send_individually_true_sends_each_item(self, requests_mock, method):
        """
        When send_individually=True with a list of items,
        Each item should be sent as a separate request.
        """
        items = [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}]
        request_history = []

        def json_callback(request: requests.Request, context: Any) -> dict:
            """Store request and return it."""
            request_history.append(request.json())
            return request.json()

        api_dataset = APIDataset(
            url=TEST_URL,
            method=method,
            save_args={"send_individually": True},
        )
        requests_mock.register_uri(
            method,
            TEST_URL,
            json=json_callback,
        )

        # Save list of items
        response = api_dataset.save.__wrapped__(api_dataset, items)

        # Verify response is from the last item
        assert isinstance(response, requests.Response)
        assert response.json() == items[-1]

        # Verify each item was sent individually (not as a list)
        assert len(request_history) == len(items)
        assert request_history[0] == items[0]
        assert request_history[1] == items[1]

    @pytest.mark.parametrize("method", SAVE_METHODS)
    def test_send_individually_false_uses_chunking(self, requests_mock, method):
        """
        When send_individually=False (default) with a list of items,
        Items should be sent as chunks according to chunk_size.
        """
        items = [
            {"id": 1, "name": "item1"},
            {"id": 2, "name": "item2"},
            {"id": 3, "name": "item3"},
        ]
        request_history = []

        def json_callback(request: requests.Request, context: Any) -> dict:
            """Store request and return it."""
            request_history.append(request.json())
            return request.json()

        api_dataset = APIDataset(
            url=TEST_URL,
            method=method,
            save_args={"send_individually": False, "chunk_size": 2},
        )
        requests_mock.register_uri(
            method,
            TEST_URL,
            json=json_callback,
        )

        # Save list of items
        api_dataset.save.__wrapped__(api_dataset, items)

        # Verify response is from the last chunk
        assert request_history[-1] == [items[2]]

        # Verify items were sent as chunks
        assert len(request_history) == len(items) - 1
        assert request_history[0] == items[:2]  # First chunk: items 1-2
        assert request_history[1] == items[2:]  # Second chunk: item 3

    @pytest.mark.parametrize("method", SAVE_METHODS)
    def test_send_individually_default_false(self, requests_mock, method):
        """
        When send_individually is not specified,
        Default behavior should be chunking (send_individually=False).
        """
        items = [{"id": 1}, {"id": 2}]
        request_history = []

        def json_callback(request: requests.Request, context: Any) -> dict:
            request_history.append(request.json())
            return request.json()

        api_dataset = APIDataset(
            url=TEST_URL,
            method=method,
            save_args={"chunk_size": 1},
        )
        requests_mock.register_uri(
            method,
            TEST_URL,
            json=json_callback,
        )

        api_dataset.save.__wrapped__(api_dataset, items)

        # Verify items were sent as chunks (not individually)
        assert len(request_history) == len(items)
        assert request_history[0] == [items[0]]  # First chunk as list
        assert request_history[1] == [items[1]]  # Second chunk as list

    @pytest.mark.parametrize("method", SAVE_METHODS)
    def test_send_individually_with_single_item(self, requests_mock, method):
        """
        When send_individually=True with a single item list,
        The item should be sent individually.
        """
        items = [{"id": 1, "data": "single"}]
        request_history = []

        def json_callback(request: requests.Request, context: Any) -> dict:
            request_history.append(request.json())
            return request.json()

        api_dataset = APIDataset(
            url=TEST_URL,
            method=method,
            save_args={"send_individually": True},
        )
        requests_mock.register_uri(
            method,
            TEST_URL,
            json=json_callback,
        )

        response = api_dataset.save.__wrapped__(api_dataset, items)

        assert isinstance(response, requests.Response)
        assert len(request_history) == 1
        assert request_history[0] == items[0]

    @pytest.mark.parametrize("method", SAVE_METHODS)
    def test_send_individually_with_empty_list(self, requests_mock, method):
        """
        When send_individually=True with an empty list,
        No requests should be made.
        """
        items = []
        request_history = []

        def json_callback(request: requests.Request, context: Any) -> dict:
            request_history.append(request.json())
            return {"error": "no data"}

        api_dataset = APIDataset(
            url=TEST_URL,
            method=method,
            save_args={"send_individually": True},
        )
        requests_mock.register_uri(
            method,
            TEST_URL,
            json=json_callback,
        )

        # Empty list with send_individually=True should return None for response
        response = api_dataset.save.__wrapped__(api_dataset, items)

        # No requests should be made for empty list
        assert len(request_history) == 0
        # Response should be None since no requests were made
        assert response is None

    @pytest.mark.parametrize("method", SAVE_METHODS)
    def test_send_individually_with_dict_data(self, requests_mock, method):
        """
        When send_individually=True but data is a dict (not a list),
        The dict should be sent as-is.
        """
        data = {"id": 1, "name": "single_item"}
        request_history = []

        def json_callback(request: requests.Request, context: Any) -> dict:
            request_history.append(request.json())
            return request.json()

        api_dataset = APIDataset(
            url=TEST_URL,
            method=method,
            save_args={"send_individually": True},
        )
        requests_mock.register_uri(
            method,
            TEST_URL,
            json=json_callback,
        )

        api_dataset.save.__wrapped__(api_dataset, data)

        # Dict data should still be sent as a single request
        assert len(request_history) == 1
        assert request_history[0] == data

    @pytest.mark.parametrize("method", SAVE_METHODS)
    def test_send_individually_takes_precedence_over_chunk_size(
        self, requests_mock, method
    ):
        """
        When both send_individually=True and chunk_size are specified,
        send_individually should take precedence.
        """
        items = [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 4},
        ]
        request_history = []

        def json_callback(request: requests.Request, context: Any) -> dict:
            request_history.append(request.json())
            return request.json()

        api_dataset = APIDataset(
            url=TEST_URL,
            method=method,
            save_args={"send_individually": True, "chunk_size": 2},
        )
        requests_mock.register_uri(
            method,
            TEST_URL,
            json=json_callback,
        )

        api_dataset.save.__wrapped__(api_dataset, items)

        # All items should be sent individually (not chunked by 2)
        assert len(request_history) == len(items)
        for i, req_data in enumerate(request_history):
            assert req_data == items[i]

    @pytest.mark.parametrize("method", SAVE_METHODS)
    def test_send_individually_with_response_dataset(
        self, requests_mock, tmp_path, method
    ):
        """
        When send_individually=True with response_dataset configured,
        Only the final response should be stored.
        """
        items = [
            {"id": 1, "action": "create"},
            {"id": 2, "action": "create"},
            {"id": 3, "action": "create"},
        ]
        json_file = tmp_path / "response.json"
        response_data = {"status": "all_created", "count": 3}

        def json_callback(request: requests.Request, context: Any) -> dict:
            return response_data

        api_dataset = APIDataset(
            url=TEST_URL,
            method=method,
            save_args={"send_individually": True},
            response_dataset={"type": "json.JSONDataset", "filepath": str(json_file)},
        )
        requests_mock.register_uri(
            method,
            TEST_URL,
            json=json_callback,
        )

        response = api_dataset.save.__wrapped__(api_dataset, items)

        # Verify response is the last one
        assert response.json() == response_data

        # Verify the final response was stored
        assert json_file.exists()
        with open(json_file) as f:
            stored_data = json_module.load(f)
        assert stored_data == response_data
