"""``APIDataset`` loads the data from HTTP(S) APIs.
It uses the python requests library: https://requests.readthedocs.io/en/latest/
"""
import json as json_  # make pylint happy
from copy import deepcopy
from typing import Any, Union

import requests
from kedro.io.core import AbstractDataset, DatasetError
from requests import Session, sessions
from requests.auth import AuthBase


class APIDataset(AbstractDataset[None, requests.Response]):
    """``APIDataset`` loads/saves data from/to HTTP(S) APIs.
    It uses the python requests library: https://requests.readthedocs.io/en/latest/

    Example usage for the `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        usda:
          type: api.APIDataset
          url: https://quickstats.nass.usda.gov
          params:
            key: SOME_TOKEN,
            format: JSON,
            commodity_desc: CORN,
            statisticcat_des: YIELD,
            agg_level_desc: STATE,
            year: 2000

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

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
    methods.

    .. code-block:: pycon

        >>> example_table = '{"col1":["val1", "val2"], "col2":["val3", "val4"]}'
        >>>
        >>> dataset = APIDataset(
        ...     method="POST",
        ...     url="https://httpbin.org/post",
        ...     save_args={"chunk_size": 1},
        ... )
        >>> dataset.save(example_table)

    On initialisation, we can specify all the necessary parameters in the save args
    dictionary. The default HTTP(S) method is POST but PUT is also supported. Two
    important parameters to keep in mind are timeout and chunk_size. `timeout` defines
    how long  our program waits for a response after a request. `chunk_size`, is only
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
        load_args: dict[str, Any] = None,
        save_args: dict[str, Any] = None,
        credentials: Union[tuple[str, str], list[str], AuthBase] = None,
        metadata: dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``APIDataset`` to fetch data from an API endpoint.

        Args:
            url: The API URL endpoint.
            method: The method of the request. GET, POST, PUT are the only supported
                methods
            load_args: Additional parameters to be fed to requests.request.
                https://requests.readthedocs.io/en/latest/api/#requests.request
            save_args: Options for saving data on server. Includes all parameters used
                during load method. Adds an optional parameter, ``chunk_size`` which
                determines the size of the package sent at each request.
            credentials: Allows specifying secrets in credentials.yml.
                Expected format is ``('login', 'password')`` if given as a tuple or
                list. An ``AuthBase`` instance can be provided for more complex cases.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            ValueError: if both ``auth`` and ``credentials`` are specified or used
                unsupported RESTful API method.
        """
        super().__init__()

        # GET method means load
        if method == "GET":
            self._params = load_args or {}

        # PUT, POST means save
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

    def _describe(self) -> dict[str, Any]:
        # prevent auth from logging
        request_args_cp = self._request_args.copy()
        request_args_cp.pop("auth", None)
        return request_args_cp

    def _execute_request(self, session: Session) -> requests.Response:
        try:
            response = session.request(**self._request_args)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise DatasetError("Failed to fetch data", exc) from exc
        except OSError as exc:
            raise DatasetError("Failed to connect to the remote server") from exc

        return response

    def _load(self) -> requests.Response:
        if self._request_args["method"] == "GET":
            with sessions.Session() as session:
                return self._execute_request(session)

        raise DatasetError("Only GET method is supported for load")

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

    def _save(self, data: Any) -> requests.Response:
        if self._request_args["method"] in ["PUT", "POST"]:
            if isinstance(data, list):
                return self._execute_save_with_chunks(json_data=data)

            return self._execute_save_request(json_data=data)

        raise DatasetError("Use PUT or POST methods for save")

    def _exists(self) -> bool:
        with sessions.Session() as session:
            response = self._execute_request(session)
        return response.ok
