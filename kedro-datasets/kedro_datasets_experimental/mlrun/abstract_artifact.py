"""Base class for MLRun datasets."""
from __future__ import annotations

from typing import Any

import mlrun.artifacts
from kedro.io import AbstractDataset

from .context_manager import MLRunContextManager


class MLRunAbstractDataset(AbstractDataset):
    """Base class for MLRun datasets; use for generic artifacts (any serializable data).

    Uses MLRun's
    `log_artifact <https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_artifact>`_
    and
    `get_artifact <https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.get_artifact>`_.
    ``load_args`` and ``save_args`` accept any arguments supported by the corresponding
    MLRun API for your MLRun version; see the MLRun docs.

    Examples:
        Using the
        `YAML API <https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/>`_:

        .. code-block:: yaml

            generic_artifact:
              type: kedro_datasets_experimental.mlrun.MLRunAbstractDataset
              key: my_artifact

        Using the
        `Python API <https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/>`_:

        .. code-block:: python

            from kedro_datasets_experimental.mlrun import MLRunAbstractDataset

            dataset = MLRunAbstractDataset(key="config_data")
            dataset.save({"param1": "value1", "param2": 42})
            loaded = dataset.load()

    Args:
        key: Artifact key for MLRun (defaults to catalog dataset name).
        load_args: Passed to MLRun when loading; see MLRun docs for your version.
        save_args: Passed to log_artifact; see MLRun docs for your version.
    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(self,
                 key: str | None = None,
                 load_args: dict[str, Any] | None = None,
                 save_args: dict[str, Any] | None = None, ) -> None:
        self._ctx_manager = MLRunContextManager()
        self._key = key or getattr(self, '_name', 'unnamed')
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}
        self._save_args["db_key"] = self._key
        self._load_args["key"] = self._key


    def load(self) -> mlrun.artifacts.Artifact | None:
        return self._ctx_manager.context.get_artifact(**self._load_args)

    def save(self, data: Any) -> None:
        artifact = mlrun.artifacts.Artifact(key=self._key, body=data)
        self._ctx_manager.context.log_artifact(item=artifact, **self._save_args)

    def _describe(self) -> dict[str, Any]:
        return {
            "key": self._key,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "mlrun_project_name": self._ctx_manager.project.name,
        }
