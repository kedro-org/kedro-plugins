from copy import deepcopy

import pytest
from kedro.io.core import DatasetError

from kedro_datasets.api import PaginatedAPIDataset

TEST_URL = "http://example.com/api/items"
NEXT_URL = "http://example.com/api/items?page=2"
LAST_URL = "http://example.com/api/items?page=3"


def make_dataset(**pagination):
    return PaginatedAPIDataset(
        url=TEST_URL,
        pagination={
            "next_url_path": "links.next",
            "results_path": "data.items",
            **pagination,
        },
    )


def test_load_returns_results_from_a_single_page(requests_mock):
    requests_mock.get(TEST_URL, json={"data": {"items": [{"id": 1}]}})

    assert make_dataset().load() == [{"id": 1}]


def test_load_follows_next_urls_and_preserves_order(requests_mock):
    requests_mock.get(
        TEST_URL,
        json={
            "data": {"items": [{"id": 1}, {"id": 2}]},
            "links": {"next": NEXT_URL},
        },
    )
    requests_mock.get(
        NEXT_URL,
        json={
            "data": {"items": [{"id": 3}]},
            "links": {"next": LAST_URL},
        },
    )
    requests_mock.get(
        LAST_URL,
        json={"data": {"items": [{"id": 4}]}, "links": {"next": None}},
    )

    assert make_dataset().load() == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]
    assert [request.url for request in requests_mock.request_history] == [
        TEST_URL,
        NEXT_URL,
        LAST_URL,
    ]


def test_load_propagates_request_options_and_does_not_mutate_load_args(requests_mock):
    load_args = {
        "params": {"page_size": 2},
        "headers": {"X-Request-ID": "test"},
        "auth": ("user", "password"),
        "timeout": 7,
    }
    original_load_args = deepcopy(load_args)
    requests_mock.get(
        TEST_URL,
        json={"data": {"items": [1]}, "links": {"next": NEXT_URL}},
    )
    requests_mock.get(NEXT_URL, json={"data": {"items": [2]}, "links": {}})

    dataset = PaginatedAPIDataset(
        url=TEST_URL,
        load_args=load_args,
        pagination={"next_url_path": "links.next", "results_path": "data.items"},
    )
    assert dataset.load() == [1, 2]
    assert load_args == original_load_args

    first_request, second_request = requests_mock.request_history
    assert first_request.qs == {"page_size": ["2"]}
    assert first_request.headers["X-Request-ID"] == "test"
    assert first_request.headers["Authorization"].startswith("Basic ")
    assert second_request.headers["X-Request-ID"] == "test"
    assert second_request.headers["Authorization"].startswith("Basic ")
    assert second_request.qs == {"page": ["2"]}


def test_load_handles_empty_page_and_missing_or_null_next(requests_mock):
    requests_mock.get(
        TEST_URL,
        json={"data": {"items": []}, "links": {"next": NEXT_URL}},
    )
    requests_mock.get(NEXT_URL, json={"data": {"items": [1]}, "links": {}})

    assert make_dataset().load() == [1]

    requests_mock.reset_mock()
    requests_mock.get(TEST_URL, json={"data": {"items": [2]}, "links": {"next": None}})
    assert make_dataset().load() == [2]


@pytest.mark.parametrize("status_code", [400, 500])
def test_load_http_error_on_first_page(requests_mock, status_code):
    requests_mock.get(TEST_URL, status_code=status_code)

    with pytest.raises(DatasetError, match="Failed to fetch data"):
        make_dataset().load()


def test_load_http_error_on_later_page(requests_mock):
    requests_mock.get(
        TEST_URL,
        json={"data": {"items": [1]}, "links": {"next": NEXT_URL}},
    )
    requests_mock.get(NEXT_URL, status_code=503)

    with pytest.raises(DatasetError, match="Failed to fetch data"):
        make_dataset().load()


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"links": {"next": None}}, "missing results"),
        ({"data": {"items": {"id": 1}}, "links": {}}, "to be a list"),
        ([1, 2], "JSON object"),
    ],
)
def test_load_rejects_unsupported_response_shapes(requests_mock, payload, message):
    requests_mock.get(TEST_URL, json=payload)

    with pytest.raises(DatasetError, match=message):
        make_dataset().load()


def test_load_rejects_invalid_json(requests_mock):
    requests_mock.get(TEST_URL, text="not json")

    with pytest.raises(
        DatasetError, match="expected each response to contain a JSON object"
    ):
        make_dataset().load()


@pytest.mark.parametrize("next_value", [123, "items", "/api/items?page=2"])
def test_load_rejects_malformed_pagination_metadata(requests_mock, next_value):
    requests_mock.get(
        TEST_URL,
        json={"data": {"items": [1]}, "links": {"next": next_value}},
    )

    with pytest.raises(DatasetError, match="next-page"):
        make_dataset().load()


def test_load_rejects_repeated_next_urls(requests_mock):
    requests_mock.get(
        TEST_URL,
        json={"data": {"items": [1]}, "links": {"next": NEXT_URL}},
    )
    requests_mock.get(
        NEXT_URL,
        json={"data": {"items": [2]}, "links": {"next": NEXT_URL}},
    )

    with pytest.raises(DatasetError, match="repeated next-page URL"):
        make_dataset().load()
    assert len(requests_mock.request_history) == 2


def test_load_enforces_max_pages(requests_mock):
    requests_mock.get(
        TEST_URL,
        json={"data": {"items": [1]}, "links": {"next": NEXT_URL}},
    )
    requests_mock.get(
        NEXT_URL,
        json={"data": {"items": [2]}, "links": {"next": LAST_URL}},
    )

    with pytest.raises(DatasetError, match="maximum of 2 pages"):
        make_dataset(max_pages=2).load()
    assert len(requests_mock.request_history) == 2


@pytest.mark.parametrize(
    ("pagination", "message"),
    [
        (None, "requires a pagination mapping"),
        ({}, "requires a non-empty 'next_url_path'"),
        ({"next_url_path": "next"}, "requires a non-empty 'results_path'"),
        (
            {"next_url_path": "next", "results_path": "results", "max_pages": 0},
            "positive integer",
        ),
        (
            {
                "next_url_path": "next",
                "results_path": "results",
                "unsupported": True,
            },
            "unsupported keys",
        ),
    ],
)
def test_invalid_pagination_configuration(pagination, message):
    with pytest.raises(ValueError, match=message):
        PaginatedAPIDataset(url=TEST_URL, pagination=pagination)


def test_invalid_request_method():
    with pytest.raises(ValueError, match="only supports GET"):
        PaginatedAPIDataset(
            url=TEST_URL,
            load_args={"method": "POST"},
            pagination={"next_url_path": "next", "results_path": "results"},
        )


def test_describe_includes_pagination_without_authentication():
    dataset = make_dataset(max_pages=5)

    description = dataset._describe()

    assert description["pagination"] == {
        "next_url_path": "links.next",
        "results_path": "data.items",
        "max_pages": 5,
    }
    assert "auth" not in description
