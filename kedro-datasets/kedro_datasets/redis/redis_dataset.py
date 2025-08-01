"""``PickleDataset`` loads/saves data from/to a Redis database. The underlying
functionality is supported by the redis library, so it supports all allowed
options for instantiating the redis app ``from_url`` and setting a value."""
from __future__ import annotations

import importlib
import os
from copy import deepcopy
from typing import Any

import redis
from kedro.io.core import AbstractDataset, DatasetError


class PickleDataset(AbstractDataset[Any, Any]):
    """``PickleDataset`` loads/saves data from/to a Redis database. The
    underlying functionality is supported by the redis library, so it supports
    all allowed options for instantiating the redis app ``from_url`` and setting
    a value.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        my_python_object: # simple example
          type: redis.PickleDataset
          key: my_object
          from_url_args:
            url: redis://127.0.0.1:6379

        final_python_object: # example with save args
          type: redis.PickleDataset
          key: my_final_object
          from_url_args:
            url: redis://127.0.0.1:6379
            db: 1
          save_args:
            ex: 10
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> import pandas as pd
        >>> from kedro_datasets.redis import PickleDataset
        >>>
        >>> data = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>>
        >>> my_data = PickleDataset(key="my_data")
        >>> my_data.save(data)
        >>> reloaded = my_data.load()
        >>> assert data.equals(reloaded)

    """

    DEFAULT_REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        key: str,
        backend: str = "pickle",
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        redis_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``PickleDataset``. This loads/saves data from/to
        a Redis database while deserialising/serialising. Supports custom backends to
        serialise/deserialise objects.

        Example backends that are compatible - non-exhaustive:
            * `pickle`
            * `dill`
            * `compress_pickle`
            * `cloudpickle`

        Example backends that are incompatible:
            * `torch`

        Args:
            key: The key to use for saving/loading object to Redis.
            backend: Backend to use, must be an import path to a module which satisfies the
                ``pickle`` interface. That is, contains a `loads` and `dumps` function.
                Defaults to 'pickle'.
            load_args: Pickle options for loading pickle files.
                You can pass in arguments that the backend load function specified accepts, e.g:
                pickle.loads: https://docs.python.org/3/library/pickle.html#pickle.loads
                dill.loads: https://dill.readthedocs.io/en/latest/index.html#dill.loads
                compress_pickle.loads:
                https://lucianopaz.github.io/compress_pickle/html/api/compress_pickle.html#compress_pickle.compress_pickle.loads
                cloudpickle.loads:
                https://github.com/cloudpipe/cloudpickle/blob/master/tests/cloudpickle_test.py
                All defaults are preserved.
            save_args: Pickle options for saving pickle files.
                You can pass in arguments that the backend dump function specified accepts, e.g:
                pickle.dumps: https://docs.python.org/3/library/pickle.html#pickle.dump
                dill.dumps: https://dill.readthedocs.io/en/latest/index.html#dill.dumps
                compress_pickle.dumps:
                https://lucianopaz.github.io/compress_pickle/html/api/compress_pickle.html#compress_pickle.compress_pickle.dumps
                cloudpickle.dumps:
                https://github.com/cloudpipe/cloudpickle/blob/master/tests/cloudpickle_test.py
                All defaults are preserved.
            credentials: Credentials required to get access to the redis server.
                E.g. `{"password": None}`.
            redis_args: Extra arguments to pass into the redis client constructor
                ``redis.StrictRedis.from_url``. (e.g. `{"socket_timeout": 10}`), as well as to pass
                to the ``redis.StrictRedis.set`` through nested keys `from_url_args` and `set_args`.
                Here you can find all available arguments for `from_url`:
                https://redis-py.readthedocs.io/en/stable/connections.html?highlight=from_url#redis.Redis.from_url
                All defaults are preserved, except `url`, which is set to `redis://127.0.0.1:6379`.
                You could also specify the url through the env variable ``REDIS_URL``.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            ValueError: If ``backend`` does not satisfy the `pickle` interface.
            ImportError: If the ``backend`` module could not be imported.
        """
        try:
            imported_backend = importlib.import_module(backend)
        except ImportError as exc:
            raise ImportError(
                f"Selected backend '{backend}' could not be imported. "
                "Make sure it is installed and importable."
            ) from exc

        if not (
            hasattr(imported_backend, "loads") and hasattr(imported_backend, "dumps")
        ):
            raise ValueError(
                f"Selected backend '{backend}' should satisfy the pickle interface. "
                "Missing one of 'loads' and 'dumps' on the backend."
            )

        self._backend = backend

        self._key = key

        self.metadata = metadata

        _redis_args = deepcopy(redis_args) or {}
        self._redis_from_url_args = _redis_args.pop("from_url_args", {})
        self._redis_from_url_args.setdefault("url", self.DEFAULT_REDIS_URL)
        self._redis_set_args = _redis_args.pop("set_args", {})
        _credentials = deepcopy(credentials) or {}

        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._redis_db = redis.Redis.from_url(
            **self._redis_from_url_args, **_credentials
        )

    def _describe(self) -> dict[str, Any]:
        return {"key": self._key, **self._redis_from_url_args}

    # `redis_db` mypy does not work since it is optional and optional is not
    # accepted by pickle.loads.
    def load(self) -> Any:
        if not self.exists():
            raise DatasetError(f"The provided key {self._key} does not exists.")
        imported_backend = importlib.import_module(self._backend)
        return imported_backend.loads(  # type: ignore
            self._redis_db.get(self._key), **self._load_args
        )  # type: ignore

    def save(self, data: Any) -> None:
        try:
            imported_backend = importlib.import_module(self._backend)
            self._redis_db.set(
                self._key,
                imported_backend.dumps(data, **self._save_args),  # type: ignore
                **self._redis_set_args,
            )
        except Exception as exc:
            raise DatasetError(
                f"{data.__class__} was not serialised due to: {exc}"
            ) from exc

    def _exists(self) -> bool:
        try:
            return bool(self._redis_db.exists(self._key))
        except Exception as exc:
            raise DatasetError(
                f"The existence of key {self._key} could not be established due to: {exc}"
            ) from exc
