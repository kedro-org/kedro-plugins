"""MLRun context manager singleton."""
from __future__ import annotations

import os
from typing import ClassVar

import mlrun

DEFAULT_CONTEXT_NAME = "kedro-mlrun-ctx"


class MLRunContextManager:
    """Singleton for MLRun context."""

    _instance: ClassVar[MLRunContextManager | None] = None
    _context: mlrun.MLClientCtx = None
    _project: mlrun.projects.MlrunProject = None

    def __new__(cls) -> MLRunContextManager:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def project(self) -> mlrun.projects.MlrunProject:
        if not self._project:
            self._project = mlrun.get_current_project()
        return self._project

    @property
    def context(self) -> mlrun.MLClientCtx:
        if not self._context:
            # Project must be loaded before context
            _ = self.project
            context_name = os.getenv("MLRUN_CONTEXT_NAME", DEFAULT_CONTEXT_NAME)
            self._context = mlrun.get_or_create_ctx(context_name)
        return self._context

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._context.__exit__(exc_type, exc_value, exc_traceback)
