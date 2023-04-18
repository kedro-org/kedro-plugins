"""``APIDataSet`` loads the data from HTTP(S) APIs.
It uses the python requests library: https://requests.readthedocs.io/en/latest/
"""
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Union

import requests
from kedro.io.core import AbstractDataSet, DataSetError
from requests.auth import AuthBase


class APIDataSet(AbstractDataSet[None, requests.Response]):
    """``APIDataSet`` loads the data from HTTP(S) APIs.
    It uses the python requests library: https://requests.readthedocs.io/en/latest/

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

    .. code-block:: yaml

        usda:
          type: api.APIDataSet
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
    data_catalog.html#use-the-data-catalog-with-the-code-api>`_:
    ::

        >>> from kedro_datasets.api import APIDataSet
        >>>
        >>>
        >>> data_set = APIDataSet(
        >>>     url="https://quickstats.nass.usda.gov",
        >>>     params={
        >>>         "key": "SOME_TOKEN",
        >>>         "format": "JSON",
        >>>         "commodity_desc": "CORN",
        >>>         "statisticcat_des": "YIELD",
        >>>         "agg_level_desc": "STATE",
        >>>         "year": 2000
        >>>     }
        >>> )
        >>> data = data_set.load()

    Example of saving data with a REST API.
    """

    DEFAULT_SAVE_ARGS = {
        "method": "POST",
        "params": None,
        "headers": None,
        "auth": None,
        "json": None,
        "timeout": 60,
        "chunk_size": 100,
    }
    # pylint: disable=too-many-arguments

    def __init__(
        self,
        url: str,
        method: str = "GET",
        data: Any = None,
        params: Dict[str, Any] = None,
        headers: Dict[str, Any] = None,
        auth: Union[Iterable[str], AuthBase] = None,
        json: Union[List, Dict[str, Any]] = None,
        timeout: int = 60,
        credentials: Union[Iterable[str], AuthBase] = None,
        save_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``APIDataSet`` to fetch data from an API endpoint.

        Args:
            url: The API URL endpoint. method: The Method of the request, GET, POST, PUT,
            DELETE, HEAD, etc... data: The request payload, used for POST, PUT, etc
            requests
                https://requests.readthedocs.io/en/latest/user/quickstart/#more-complicated-post-requests
            params: The url parameters of the API.
                https://requests.readthedocs.io/en/latest/user/quickstart/#passing-parameters-in-urls
            headers: The HTTP headers.
                https://requests.readthedocs.io/en/latest/user/quickstart/#custom-headers
            auth: Anything ``requests`` accepts. Normally it's either ``('login',
            'password')``,
                or ``AuthBase``, ``HTTPBasicAuth`` instance for more complex cases. Any
                iterable will be cast to a tuple.
            json: The request payload, used for POST, PUT, etc requests, passed in
                to the json kwarg in the requests object.
                https://requests.readthedocs.io/en/latest/user/quickstart/#more-complicated-post-requests
            timeout: The wait time in seconds for a response, defaults to 1 minute.
                https://requests.readthedocs.io/en/latest/user/quickstart/#timeouts
            credentials: same as ``auth``. Allows specifying ``auth`` secrets in
                credentials.yml.
            save_args: Options for saving data on server. Includes all parameters used
            during load method.
                        Adds an optional parameter, ``chunk_size`` which determines the
                        size of the package sent at each request.
        Raises:
            ValueError: if both ``credentials`` and ``auth`` are specified.
        """
        super().__init__()

        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)

        if save_args is not None:
            self._save_args.update(save_args)

        if credentials is not None and auth is not None:
            raise ValueError("Cannot specify both auth and credentials.")

        auth = credentials or auth

        if isinstance(auth, Iterable):
            auth = tuple(auth)

        self._request_args: Dict[str, Any] = {
            "url": url,
            "method": method,
            "data": data,
            "params": params,
            "headers": headers,
            "auth": auth,
            "json": json,
            "timeout": timeout,
        }

    def _describe(self) -> Dict[str, Any]:
        return {**self._request_args}

    def _execute_request(self) -> requests.Response:
        try:
            response = requests.request(**self._request_args)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise DataSetError("Failed to fetch data", exc) from exc
        except OSError as exc:
            raise DataSetError("Failed to connect to the remote server") from exc

        return response

    def _load(self) -> requests.Response:
        return self._execute_request()

    def _execute_save_request(
        self,
        json_data: List[Dict[str, Any]],
    ) -> requests.Response:
        # retrieve parameters to execute request
        chunk_size = self._save_args["chunk_size"]

        print(self._save_args)
        # compute nb of chunks to send data to endpoint
        n_chunks = len(json_data) // chunk_size + 1
        for i in range(n_chunks):
            # are we sure we need to do this at each iteration ?
            send_data = json_data[i * chunk_size : (i + 1) * chunk_size]

            self._save_args["json"] = send_data
            # same error catching as load method
            try:
                response = requests.request(**self._request_args)
                print(response, response.raise_for_status())
                response.raise_for_status()

            except requests.exceptions.HTTPError as exc:
                raise DataSetError("Failed to fetch data", exc) from exc

            except OSError as exc:
                raise DataSetError("Failed to connect to the remote server") from exc
        return response

    def _save(self, data: Any) -> requests.Response:
        # check here that we are correctly sending JSON format as an argument ?
        # also in this case the expected format would be a list of JSON files
        # should we keep this ? maybe too specific
        return self._execute_save_request(json_data=data)

    def _exists(self) -> bool:
        response = self._execute_request()
        return response.ok
