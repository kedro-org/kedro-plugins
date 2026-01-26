"""Base class for MLRun datasets."""
from __future__ import annotations

from typing import Any

import mlrun.artifacts
from kedro.io import AbstractDataset

from .context_manager import MLRunContextManager


class MLRunAbstractDataset(AbstractDataset):
    """Base class for MLRun datasets that provides access to shared context."""

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
