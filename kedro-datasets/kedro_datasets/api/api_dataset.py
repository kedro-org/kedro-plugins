"""``APIDataSet`` loads the data from HTTP(S) APIs.
It uses the python requests library: https://requests.readthedocs.io/en/latest/
"""
import json as json_  # make pylint happy
from copy import deepcopy
from typing import Any, Dict, List, Tuple, Union

import requests
from kedro.io.core import AbstractDataSet, DataSetError
from requests import Session, sessions
from requests.auth import AuthBase

# NOTE: kedro.extras.datasets will be removed in Kedro 0.19.0.
# Any contribution to datasets should be made in kedro-datasets
# in kedro-plugins (https://github.com/kedro-org/kedro-plugins)


class APIDataSet(AbstractDataSet[None, requests.Response]):
    """``APIDataSet`` loads the data from HTTP(S) APIs.
    It uses the python requests library: https://requests.readthedocs.io/en/latest/

    Example usage for the `YAML API <https://kedro.readthedocs.io/en/stable/data/\
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

    Example usage for the `Python API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-code-api>`_: ::

        >>> from kedro.extras.datasets.api import APIDataSet
        >>>
        >>>
        >>> data_set = APIDataSet(
        >>>     url="https://quickstats.nass.usda.gov",
        >>>     load_args={
        >>>         "params": {
        >>>             "key": "SOME_TOKEN",
        >>>             "format": "JSON",
        >>>             "commodity_desc": "CORN",
        >>>             "statisticcat_des": "YIELD",
        >>>             "agg_level_desc": "STATE",
        >>>             "year": 2000
        >>>         }
        >>>     },
        >>>     credentials=("username", "password")
        >>> )
        >>> data = data_set.load()

    ``APIDataSet`` can also be used to save output on a remote server using
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
    On initialisation, we can specify all the necessary parameters in the save args
    dictionary. The default HTTP(S) method is POST but PUT is also supported.
    Two important parameters to keep in mind are timeout and chunk_size. ``Timeout``
    defines how long  our program waits for a response after a request. ``Chunk_size``, is
    only used if the input of save method is a list. It will divide the request into
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
        load_args: Dict[str, Any] = None,
        credentials: Union[Tuple[str, str], List[str], AuthBase] = None,
    ) -> None:
        """Creates a new instance of ``APIDataSet`` to fetch data from an API endpoint.

        Args:
            url: The API URL endpoint.
            method: The Method of the request, GET, POST, PUT
            load_args: Additional parameters to be fed to requests.request.
                https://requests.readthedocs.io/en/latest/api/#requests.request
            credentials: Allows specifying secrets in credentials.yml.
                Expected format is ``('login', 'password')`` if given as a tuple or list.
                An ``AuthBase`` instance can be provided for more complex cases.
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
            ValueError: if both ``auth`` in ``load_args`` and ``credentials`` are specified.
        """
        super().__init__()

        self._load_args = load_args or {}
        self._load_args_auth = self._load_args.pop("auth", None)

        # PUT, POST, DELETE means save
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

        self._auth = credentials or self._load_args_auth

        if "cert" in self._load_args:
            self._load_args["cert"] = self._convert_type(self._load_args["cert"])

        if "timeout" in self._load_args:
            self._load_args["timeout"] = self._convert_type(self._load_args["timeout"])

        self._request_args: Dict[str, Any] = {
            "url": url,
            "method": method,
            "auth": self._convert_type(self._auth),
            **self._load_args,
        }
        self.chunk_size = chunk_size

    @staticmethod
    def _convert_type(value: Any):
        """
        From the Data Catalog, iterables are provided as Lists.
        However, for some parameters in the Python requests library,
        only Tuples are allowed.
        """
        if isinstance(value, List):
            return tuple(value)
        return value

    @staticmethod
    def _convert_type(value: Any):
        """
        From the Data Catalog, iterables are provided as Lists.
        However, for some parameters in the Python requests library,
        only Tuples are allowed.
        """
        if isinstance(value, List):
            return tuple(value)
        return value

    def _describe(self) -> Dict[str, Any]:
        # prevent auth from logging
        request_args_cp = self._request_args.copy()
        request_args_cp.pop("auth", None)
        return request_args_cp

    def _execute_request(self, session: Session) -> requests.Response:
        try:
            response = session.request(**self._request_args)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            raise DataSetError("Failed to fetch data", exc) from exc
        except OSError as exc:
            raise DataSetError("Failed to connect to the remote server") from exc

        return response

    def _load(self) -> requests.Response:
        return self._execute_request()

    def _save(self, data: None) -> NoReturn:
        raise DataSetError(f"{self.__class__.__name__} is a read only data set type")

    def _exists(self) -> bool:
        response = self._execute_request()
        return response.ok
