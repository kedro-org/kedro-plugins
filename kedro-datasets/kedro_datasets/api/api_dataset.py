"""``APIDataset`` and ``PaginatedAPIDataset`` load data from HTTP(S) APIs.
It uses the python requests library: https://requests.readthedocs.io/en/latest/
"""

from __future__ import annotations

import json as json_  # make pylint happy
import math
from copy import deepcopy
from typing import Any
from urllib.parse import ParseResult, urlparse

import requests
from kedro.io.core import AbstractDataset, DatasetError, parse_dataset_definition
from requests import Session, sessions
from requests.auth import AuthBase

from kedro_datasets.json import JSONDataset
from kedro_datasets.text import TextDataset


class APIDataset(AbstractDataset[None, requests.Response]):
    """``APIDataset`` loads/saves data from/to HTTP(S) APIs.
    It uses the python requests library: https://requests.readthedocs.io/en/latest/

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        usda:
          type: api.APIDataset
          url: https://quickstats.nass.usda.gov
          load_args:
              params:
                key: SOME_TOKEN
                format: JSON
                commodity_desc: CORN
                statisticcat_des: YIELD
                agg_level_desc: STATE
                year: 2000
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from kedro_datasets.api import APIDataset
        >>>
        >>>
        >>> dataset = APIDataset(
        ...     url="https://api.spaceflightnewsapi.net/v4/articles",
        ...     load_args={
        ...         "params": {
        ...             "news_site": "NASA",
        ...             "launch": "65896761-b6ca-4df3-9699-e077a360c52a",  # Artemis I
        ...         }
        ...     },
        ... )
        >>> data = dataset.load()  # doctest: +SKIP

        ``APIDataset`` can also be used to save output on a remote server using HTTP(S)
        methods:

        >>> example_table = '{"col1":["val1", "val2"], "col2":["val3", "val4"]}'
        >>>
        >>> dataset = APIDataset(
        ...     method="POST",
        ...     url="https://dummyjson.com/products/add",
        ...     save_args={"chunk_size": 1},
        ... )
        >>> dataset.save(example_table)  # doctest: +SKIP

        ``APIDataset`` can automatically persist the output of ``POST`` and ``PUT``
        requests via the ``response_dataset`` parameter. This is useful for auditing,
        debugging, or reusing API responses downstream in a pipeline.

        When ``response_dataset`` is configured, the behavior is:

        - For ``JSONDataset``: stores ``response.json()`` (parsed JSON payload)
        - For ``TextDataset``: stores ``response.text`` (raw response body)
        - For other datasets (e.g. ``PickleDataset``, ``MemoryDataset``): stores the
          full ``requests.Response`` object

        You can later retrieve the persisted response by calling
        ``dataset.get_last_response()`` on the dataset instance.

        ```yaml
        api_with_response_storage:
          type: api.APIDataset
          url: https://dummyjson.com/products/add
          method: POST
          response_dataset:
            type: json.JSONDataset
            filepath: data/api_response.json
        ```

        Or using the Python API:

        >>> dataset = APIDataset(
        ...     url="https://dummyjson.com/products/add",
        ...     method="POST",
        ...     response_dataset={"type": "json.JSONDataset", "filepath": "response.json"},
        ... )
        >>> response = dataset.save({"key": "value"})  # doctest: +SKIP
        >>> # The response data is automatically saved to response.json

    On initialisation, we can specify all the necessary parameters in the save args
    dictionary. The default HTTP(S) method is POST but PUT is also supported. Two
    important parameters to keep in mind are timeout and chunk_size. `timeout` defines
    how long our program waits for a response after a request. `chunk_size`, is only
    used if the input of save method is a list. It will divide the request into chunks
    of size `chunk_size`. For example, here we will send two requests each containing
    one row of our example DataFrame.

    If the data passed to the save method is not a list, ``APIDataset`` will check if it
    can be loaded as JSON. If true, it will send the data unchanged in a single request.
    Otherwise, the ``_save`` method will try to dump the data in JSON format and execute
    the request.

    The optional ``send_individually`` parameter in save_args (default: False) allows
    sending each list item as an individual JSON object instead of as an array. This is
    useful for APIs that expect one record per request instead of batched arrays.
    When True and the input is a list, each element is sent separately, which takes
    precedence over ``chunk_size``.
    """

    DEFAULT_SAVE_ARGS = {
        "params": None,
        "headers": None,
        "auth": None,
        "json": None,
        "timeout": 60,
        "chunk_size": 100,
        "send_individually": False,
    }

    def __init__(  # noqa: PLR0913
        self,
        *,
        url: str,
        method: str = "GET",
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        credentials: tuple[str, str] | list[str] | AuthBase | None = None,
        metadata: dict[str, Any] | None = None,
        response_dataset: str | type[AbstractDataset] | dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``APIDataset`` to fetch data from an API endpoint.

        Args:
            url: The API URL endpoint.
            method: The method of the request. GET, POST, PUT are the only supported
                methods
            load_args: Additional parameters to be fed to requests.request.
                https://requests.readthedocs.io/en/latest/api.html#requests.request
            save_args: Options for saving data on server. Includes all parameters used
                during load method. Adds an optional parameter, ``chunk_size`` which
                determines the size of the package sent at each request, and
                ``send_individually`` to send list items as individual requests.
            credentials: Allows specifying secrets in credentials.yml.
                Expected format is ``('login', 'password')`` if given as a tuple or
                list. An ``AuthBase`` instance can be provided for more complex cases.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
            response_dataset: Optional dataset to automatically store API responses.
                The API response is stored based on the dataset type:

                - `JSONDataset`: Stores `response.json()` (parsed JSON data)
                - `TextDataset`: Stores `response.text` (response body as string)
                - Other datasets (e.g., `PickleDataset`, `MemoryDataset`): Stores the
                  full `requests.Response` object

                Can be specified as:

                - A string type identifier: `"json.JSONDataset"`
                - A dict with `"type"` key: `{"type": "json.JSONDataset", "filepath": "..."}`
                - A dataset class (advanced usage)

                If `None` (default), responses are not automatically stored.

        Raises:
            ValueError: if both ``auth`` and ``credentials`` are specified or used
                unsupported RESTful API method.
        """
        super().__init__()

        self._send_individually = False

        if method == "GET":
            self._params = load_args or {}

        elif method in ["PUT", "POST"]:
            self._params = deepcopy(self.DEFAULT_SAVE_ARGS)
            if save_args is not None:
                self._params.update(save_args)
            self._chunk_size = self._params.pop("chunk_size", 1)
            self._send_individually = self._params.pop("send_individually", False)
        else:
            raise ValueError("Only GET, POST and PUT methods are supported")

        self._param_auth = self._params.pop("auth", None)

        if credentials is not None and self._param_auth is not None:
            raise ValueError("Cannot specify both auth and credentials.")

        self._auth = credentials or self._param_auth

        if "cert" in self._params:
            self._params["cert"] = self._convert_type(self._params["cert"])

        if "timeout" in self._params:
            self._params["timeout"] = self._convert_type(self._params["timeout"])

        self._request_args: dict[str, Any] = {
            "url": url,
            "method": method,
            "auth": self._convert_type(self._auth),
            **self._params,
        }

        self.metadata = metadata

        # Initialize response dataset if provided
        self._response_dataset_type: type[AbstractDataset[Any, Any]] | None = None
        self._response_dataset_config: dict[str, Any] | None = None
        self._response_dataset_instance: AbstractDataset[Any, Any] | None = None

        if response_dataset is not None:
            dataset_config = (
                response_dataset
                if isinstance(response_dataset, dict)
                else {"type": response_dataset}
            )
            (
                self._response_dataset_type,
                self._response_dataset_config,
            ) = parse_dataset_definition(dataset_config)

    @staticmethod
    def _convert_type(value: Any):
        """
        From the Data Catalog, iterables are provided as Lists.
        However, for some parameters in the Python requests library,
        only Tuples are allowed.
        """
        if isinstance(value, list):
            return tuple(value)
        return value

    @property
    def _response_dataset(self) -> AbstractDataset | None:
        """Lazily create and cache the response dataset instance."""
        if self._response_dataset_type is None:
            return None

        if self._response_dataset_instance is None:
            # Type guard: _response_dataset_config is not None when _response_dataset_type is not None
            assert self._response_dataset_config is not None
            self._response_dataset_instance = self._response_dataset_type(
                **self._response_dataset_config
            )

        return self._response_dataset_instance

    def _describe(self) -> dict[str, Any]:
        # prevent auth from logging
        request_args_cp = self._request_args.copy()
        request_args_cp.pop("auth", None)

        result = dict(request_args_cp)
        if self._response_dataset is not None:
            result["response_dataset"] = self._response_dataset._describe()

        return result

    def _execute_request(
        self,
        session: Session,
        request_args: dict[str, Any] | None = None,
    ) -> requests.Response:
        try:
            response = session.request(
                **(self._request_args if request_args is None else request_args)
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise DatasetError("Failed to fetch data", exc) from exc
        except OSError as exc:
            raise DatasetError("Failed to connect to the remote server") from exc

        return response

    def get_last_response(self) -> Any:
        if self._response_dataset is None:
            raise DatasetError(
                "No response_dataset configured; cannot retrieve persisted response."
            )

        return self._response_dataset.load()  # type: ignore[return-value]

    def load(self) -> Any:
        if self._request_args["method"] != "GET":
            raise DatasetError(
                "Only GET method is supported for load()."
                "Use save() to send data or get_last_response() to retrieve "
                "a persisted response."
            )

        with sessions.Session() as session:
            return self._execute_request(session)

    def _execute_save_with_chunks(
        self,
        json_data: list[dict[str, Any]],
    ) -> requests.Response:
        # If send_individually is True, send each item as a separate request
        if self._send_individually:
            if not json_data:
                raise DatasetError(
                    "Cannot save an empty list with send_individually=True."
                )

            response = None
            for record in json_data:
                response = self._execute_save_request(json_data=record)
            return response  # type: ignore[return-value]

        # Otherwise, use chunked sending
        if not json_data:
            raise DatasetError("Cannot save an empty list.")

        chunk_size = self._chunk_size
        n_chunks = math.ceil(len(json_data) / chunk_size)

        for i in range(n_chunks):
            send_data = json_data[i * chunk_size : (i + 1) * chunk_size]
            response = self._execute_save_request(json_data=send_data)

        return response  # type: ignore[return-value]

    def _execute_save_request(self, json_data: Any) -> requests.Response:
        try:
            self._request_args["json"] = json_.loads(json_data)
        except TypeError:
            self._request_args["json"] = json_data
        try:
            response = requests.request(**self._request_args)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise DatasetError("Failed to send data", exc) from exc

        except OSError as exc:
            raise DatasetError("Failed to connect to the remote server") from exc
        return response

    def save(self, data: Any) -> requests.Response:  # type: ignore[override]
        if self._request_args["method"] in ["PUT", "POST"]:
            if isinstance(data, list):
                response: requests.Response = self._execute_save_with_chunks(
                    json_data=data
                )
            else:
                response: requests.Response = self._execute_save_request(json_data=data)

            if self._response_dataset is not None:
                if isinstance(self._response_dataset, JSONDataset):
                    extracted_data = response.json()
                elif isinstance(self._response_dataset, TextDataset):
                    extracted_data = response.text
                else:
                    extracted_data = response

                self._response_dataset.save(extracted_data)

            return response

        raise DatasetError("Use PUT or POST methods for save")

    def _exists(self) -> bool:
        if self._request_args["method"] != "GET":
            return False

        with sessions.Session() as session:
            response = self._execute_request(session)

        return response.ok


class PaginatedAPIDataset(APIDataset):
    """Load and combine list results from a JSON API with next-page links.

    The API response must be a JSON object. ``pagination.next_url_path`` locates
    an absolute HTTP(S) URL for the next page, and ``pagination.results_path``
    locates the list of items in each response. Paths use dot-separated object
    keys, for example ``meta.next``. Missing or null next-page values terminate
    pagination. The collected lists are concatenated in request order and
    returned from :meth:`load` as one list; response metadata is not retained.

    Only next-link pagination is supported. Page-number, offset, cursor-token,
    HTTP ``Link`` header, and top-level JSON-list conventions are not inferred.
    Pagination is restricted to the initial URL's host and port by default.
    Additional exact host authorities can be supplied through
    ``pagination.allowed_hosts``. Hostnames are case-insensitive, ports are
    significant, and wildcard or suffix matching is not supported.

    ``max_pages`` defaults to 1000 and can be set in ``pagination`` to protect
    against an API that keeps returning new links.

    Example YAML configuration:

    ```yaml
    items:
      type: api.PaginatedAPIDataset
      url: https://example.com/api/items
      credentials: api_credentials
      load_args:
        params:
          page_size: 100
        timeout: 30
      pagination:
        next_url_path: links.next
        results_path: data.items
        max_pages: 100
    ```

    The Python API accepts the same configuration:

    >>> from kedro_datasets.api import PaginatedAPIDataset
    >>> dataset = PaginatedAPIDataset(
    ...     url="https://example.com/api/items",
    ...     load_args={"params": {"page_size": 100}},
    ...     pagination={"next_url_path": "links.next", "results_path": "data.items"},
    ... )
    >>> items = dataset.load()  # doctest: +SKIP

    ``api_credentials`` should be defined in ``credentials.yml`` as the
    username/password pair accepted by ``requests``. Authentication, headers,
    timeout, and other ``load_args`` are passed to every trusted request. Query
    parameters are sent on the initial request; a next URL is treated as
    complete, so its own query string is used on later requests instead of
    appending the initial parameters again.

    Args:
        url: The first API URL endpoint.
        load_args: Additional arguments passed to ``requests`` for each page.
        pagination: Mapping with ``next_url_path`` and ``results_path`` string
            keys, plus optional positive integer ``max_pages`` and a list of
            exact additional host authorities in ``allowed_hosts``.
        credentials: Authentication credentials passed to every request.
        metadata: Arbitrary metadata ignored by Kedro.

    Raises:
        ValueError: If the URL, pagination configuration, or HTTP method is invalid.
        DatasetError: If a request fails, a response has an invalid JSON shape,
            a next link is malformed, untrusted, or repeated, or the page limit
            is reached.
    """

    DEFAULT_MAX_PAGES = 1000
    _PAGINATION_KEYS = {
        "next_url_path",
        "results_path",
        "max_pages",
        "allowed_hosts",
    }

    def __init__(  # noqa: PLR0913
        self,
        *,
        url: str,
        load_args: dict[str, Any] | None = None,
        pagination: dict[str, Any],
        credentials: tuple[str, str] | list[str] | AuthBase | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if not isinstance(pagination, dict):
            raise ValueError("PaginatedAPIDataset requires a pagination mapping.")
        if load_args is not None and load_args.get("method", "GET") != "GET":
            raise ValueError("PaginatedAPIDataset only supports GET requests.")

        unknown_keys = set(pagination) - self._PAGINATION_KEYS
        if unknown_keys:
            raise ValueError(
                "PaginatedAPIDataset pagination contains unsupported keys: "
                f"{sorted(unknown_keys)}."
            )

        next_url_path = pagination.get("next_url_path")
        results_path = pagination.get("results_path")
        if not isinstance(next_url_path, str) or not next_url_path:
            raise ValueError(
                "PaginatedAPIDataset pagination requires a non-empty "
                "'next_url_path'."
            )
        if not isinstance(results_path, str) or not results_path:
            raise ValueError(
                "PaginatedAPIDataset pagination requires a non-empty " "'results_path'."
            )

        max_pages = pagination.get("max_pages", self.DEFAULT_MAX_PAGES)
        if (
            isinstance(max_pages, bool)
            or not isinstance(max_pages, int)
            or max_pages < 1
        ):
            raise ValueError(
                "PaginatedAPIDataset pagination 'max_pages' must be a positive integer."
            )

        allowed_hosts = pagination.get("allowed_hosts", [])
        if not isinstance(allowed_hosts, list) or any(
            not isinstance(host, str) or not host for host in allowed_hosts
        ):
            raise ValueError(
                "PaginatedAPIDataset pagination 'allowed_hosts' must be a list "
                "of non-empty strings."
            )

        initial_url = self._parse_initial_url(url)
        self._initial_scheme = initial_url.scheme.lower()
        self._initial_authority = self._authority_from_parsed_url(initial_url)
        self._allowed_hosts = list(allowed_hosts)
        self._trusted_authorities = {
            self._initial_authority,
            *(self._normalise_host_entry(host) for host in self._allowed_hosts),
        }

        super().__init__(
            url=url,
            method="GET",
            load_args=deepcopy(load_args) if load_args is not None else None,
            credentials=credentials,
            metadata=metadata,
        )
        self._next_url_path: str = next_url_path
        self._results_path: str = results_path
        self._max_pages: int = max_pages

    @staticmethod
    def _parse_initial_url(url: str) -> ParseResult:
        if not isinstance(url, str):
            raise ValueError("PaginatedAPIDataset requires a URL string.")

        parsed = urlparse(url)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            raise ValueError(
                "PaginatedAPIDataset requires an absolute HTTP(S) initial URL."
            )
        try:
            hostname = parsed.hostname
            parsed.port
        except ValueError as exc:
            raise ValueError(
                "PaginatedAPIDataset requires a valid initial URL host and port."
            ) from exc
        if not hostname:
            raise ValueError(
                "PaginatedAPIDataset requires a valid initial URL host and port."
            )
        return parsed

    @staticmethod
    def _authority_from_parsed_url(parsed_url: ParseResult) -> tuple[str, int]:
        try:
            hostname = parsed_url.hostname
            port = parsed_url.port
        except ValueError as exc:
            raise ValueError(
                "PaginatedAPIDataset received an invalid URL port."
            ) from exc

        if not hostname:
            raise ValueError("PaginatedAPIDataset received a URL without a hostname.")

        if port is None:
            port = 80 if parsed_url.scheme.lower() == "http" else 443
        return hostname.lower(), port

    def _normalise_host_entry(self, host: str) -> tuple[str, int]:
        parsed = urlparse(f"//{host}")
        if (
            host != host.strip()
            or not parsed.netloc
            or parsed.netloc.endswith(":")
            or parsed.path
            or parsed.params
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError(
                "PaginatedAPIDataset pagination 'allowed_hosts' entries must be "
                "hostnames with optional ports."
            )

        parsed = parsed._replace(scheme=self._initial_scheme)
        try:
            return self._authority_from_parsed_url(parsed)
        except ValueError as exc:
            raise ValueError(
                "PaginatedAPIDataset pagination 'allowed_hosts' entries must "
                "contain valid hosts and ports."
            ) from exc

    def _describe(self) -> dict[str, Any]:
        description = super()._describe()
        description["pagination"] = {
            "next_url_path": self._next_url_path,
            "results_path": self._results_path,
            "max_pages": self._max_pages,
            "allowed_hosts": list(self._allowed_hosts),
        }
        return description

    @staticmethod
    def _get_path(data: Any, path: str) -> tuple[bool, Any]:
        current = data
        for key in path.split("."):
            if not isinstance(current, dict) or key not in current:
                return False, None
            current = current[key]
        return True, current

    def _validate_next_url(self, next_url: Any) -> str:
        if not isinstance(next_url, str):
            raise DatasetError(
                "PaginatedAPIDataset received a next-page value that is not a URL."
            )

        parsed = urlparse(next_url)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            raise DatasetError(
                "PaginatedAPIDataset received a malformed next-page URL. "
                "Expected an absolute HTTP(S) URL."
            )
        if parsed.scheme.lower() != self._initial_scheme:
            raise DatasetError(
                "PaginatedAPIDataset rejected a next-page URL with an unexpected scheme."
            )

        try:
            authority = self._authority_from_parsed_url(parsed)
        except ValueError as exc:
            raise DatasetError(
                "PaginatedAPIDataset received a next-page URL with an invalid host or port."
            ) from exc

        if authority not in self._trusted_authorities:
            raise DatasetError(
                "PaginatedAPIDataset rejected a next-page URL on an untrusted host. "
                "Use pagination.allowed_hosts to explicitly allow additional hosts."
            )
        return next_url

    def _load(self) -> list[Any]:
        items: list[Any] = []
        request_args = deepcopy(self._request_args)
        requested_urls: set[str] = set()

        with sessions.Session() as session:
            while True:
                page_url = request_args["url"]
                requested_urls.add(page_url)

                response = self._execute_request(session, request_args)
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise DatasetError(
                        "PaginatedAPIDataset expected each response to contain a JSON object."
                    ) from exc

                if not isinstance(payload, dict):
                    raise DatasetError(
                        "PaginatedAPIDataset expected each response to be a JSON object."
                    )

                results_found, page_items = self._get_path(payload, self._results_path)
                if not results_found:
                    raise DatasetError(
                        "PaginatedAPIDataset response is missing results at "
                        f"'{self._results_path}'."
                    )
                if not isinstance(page_items, list):
                    raise DatasetError(
                        "PaginatedAPIDataset expected results at "
                        f"'{self._results_path}' to be a list."
                    )
                items.extend(page_items)

                next_found, next_value = self._get_path(payload, self._next_url_path)
                if not next_found or next_value is None or next_value == "":
                    return items

                next_url = self._validate_next_url(next_value)
                if next_url in requested_urls:
                    raise DatasetError(
                        "PaginatedAPIDataset encountered a repeated next-page URL. "
                        "Pagination cannot terminate safely."
                    )
                if len(requested_urls) >= self._max_pages:
                    raise DatasetError(
                        "PaginatedAPIDataset exceeded the configured maximum of "
                        f"{self._max_pages} pages."
                    )

                request_args = deepcopy(self._request_args)
                request_args["url"] = next_url
                request_args.pop("params", None)
