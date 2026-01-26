"""``APIDataset`` loads the data from HTTP(S) APIs.
It uses the python requests library: https://requests.readthedocs.io/en/latest/
"""
from __future__ import annotations

import json as json_  # make pylint happy
from copy import deepcopy
from typing import Any

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
        >>> data = dataset.load()

        ``APIDataset`` can also be used to save output on a remote server using HTTP(S)
        methods:

        >>> example_table = '{"col1":["val1", "val2"], "col2":["val3", "val4"]}'
        >>>
        >>> dataset = APIDataset(
        ...     method="POST",
        ...     url="https://dummyjson.com/products/add",
        ...     save_args={"chunk_size": 1},
        ... )
        >>> dataset.save(example_table)

        To store API responses, use the `response_dataset` parameter to automatically
        persist the response to a file or data store:

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
        >>> response = dataset.save({"key": "value"})
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
    """

    DEFAULT_SAVE_ARGS = {
        "params": None,
        "headers": None,
        "auth": None,
        "json": None,
        "timeout": 60,
        "chunk_size": 100,
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
                determines the size of the package sent at each request.
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

        if method == "GET":
            self._params = load_args or {}

        elif method in ["PUT", "POST"]:
            self._params = deepcopy(self.DEFAULT_SAVE_ARGS)
            if save_args is not None:
                self._params.update(save_args)
            self._chunk_size = self._params.pop("chunk_size", 1)
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

    def _execute_request(self, session: Session) -> requests.Response:
        try:
            response = session.request(**self._request_args)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise DatasetError("Failed to fetch data", exc) from exc
        except OSError as exc:
            raise DatasetError("Failed to connect to the remote server") from exc

        return response

    def get_last_response(self) -> requests.Response:
        if self._response_dataset is None:
            raise DatasetError(
                "No response_dataset configured; cannot retrieve persisted response."
            )

        return self._response_dataset.load()  # type: ignore[return-value]

    def load(self) -> requests.Response:
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
        chunk_size = self._chunk_size
        n_chunks = len(json_data) // chunk_size + 1

        for i in range(n_chunks):
            send_data = json_data[i * chunk_size : (i + 1) * chunk_size]
            response = self._execute_save_request(json_data=send_data)

        return response

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
