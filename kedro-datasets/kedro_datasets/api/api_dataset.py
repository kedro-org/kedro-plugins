"""``APIDataSet`` loads the data from HTTP(S) APIs.
It uses the python requests library: https://requests.readthedocs.io/en/latest/
"""
import json as json_  # make pylint happy
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Union

import requests
from kedro.io.core import AbstractDataSet, DataSetError
from requests.auth import AuthBase


class APIDataSet(AbstractDataSet[None, requests.Response]):
    """``APIDataSet`` loads the data from HTTP(S) APIs.
    It uses the python requests library: https://requests.readthedocs.io/en/latest/

    Example usage for the `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

    .. code-block:: yaml

        usda:
          type: api.APIDataSet url: https://quickstats.nass.usda.gov params:
            key: SOME_TOKEN, format: JSON, commodity_desc: CORN, statisticcat_des: YIELD,
            agg_level_desc: STATE, year: 2000

    Example usage for the `Python API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-code-api>`_: ::

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

    ``APIDataSet`` can also be used to save some output on some remote server using
    HTTP(S) methods.

        >>> import pandas as pd
        >>> example_table = pd.DataFrame({"col1":["val1", "val2"], "col2":["val3", "val4"]}

    Here we initialise our APIDataSet with the correct parameters to make requests
    towards the configured remote server.

        >>> data_set = APIDataSet(
                url = "url_of_remote_server",
                save_args = {"method":"POST",
                            "chunk_size":1}
        )
    On initialization, we can specify all the necessary parameters in the save args
    dictionary. The default HTTP(S) method is POST but all other methods are supported.
    Two important parameters to keep in mind are timeout and chunk_size. ``Timeout``
    defines how long  our program waits for a response after a request. ``Chunk_size``, is
    only used if the input of save method is a list. It will, divide the request into
    chunks of size ``chunk_size``. For example, here we will send two requests each
    containing one row of our example DataFrame.

        >>> data_to_save = example_table.to_dict(orient="records")
        >>> data_set.save(data_to_save)

    If the data passed to the save method is not a list, ``APIDataSet`` will check if
    it can be loaded as JSON. If true, it will send the data unchanged in a single
    request. Otherwise, the ``_save`` method will try to dump the data in JSON format and
    execute the request.
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
            url: The API URL endpoint.
            method: The Method of the request, GET, POST, PUT,
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
                during load method. Adds an optional parameter, ``chunk_size`` which determines the
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

    def _execute_save_with_chunks(
        self,
        json_data: List[Dict[str, Any]],
    ) -> requests.Response:
        chunk_size = self._save_args["chunk_size"]
        n_chunks = len(json_data) // chunk_size + 1

        for i in range(n_chunks):
            send_data = json_data[i * chunk_size : (i + 1) * chunk_size]

            self._save_args["json"] = send_data
            try:
                response = requests.request(**self._request_args)
                response.raise_for_status()

            except requests.exceptions.HTTPError as exc:
                raise DataSetError("Failed to send data", exc) from exc

            except OSError as exc:
                raise DataSetError("Failed to connect to the remote server") from exc
        return response

    def _execute_save_request(self, json_data: Any) -> requests.Response:
        self._save_args["json"] = json_data
        try:
            response = requests.request(**self._request_args)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise DataSetError("Failed to send data", exc) from exc

        except OSError as exc:
            raise DataSetError("Failed to connect to the remote server") from exc
        return response

    def _save(self, data: Any) -> requests.Response:
        # case where we have a list of json data
        if isinstance(data, list):
            return self._execute_save_with_chunks(json_data=data)
        try:
            json_.loads(data)
        except TypeError:
            data = json_.dumps(data)

        return self._execute_save_request(json_data=data)

    def _exists(self) -> bool:
        response = self._execute_request()
        return response.ok
