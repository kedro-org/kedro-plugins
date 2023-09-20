from __future__ import annotations

import typing as t
from typing import Any
from warnings import warn

from kedro.io import AbstractDataset
from transformers import Pipeline, pipeline


class HFTransformerPipelineDataset(AbstractDataset):
    def __init__(
        self,
        task: str | None = None,
        model_name: str | None = None,
        pipeline_kwargs: dict[t.Any] | None = None,
    ):
        if task is None and model_name is None:
            raise ValueError("At least 'task' or 'model_name' are needed")
        self._task = task if task else None
        self._model_name = model_name
        self._pipeline_kwargs = pipeline_kwargs or {}

        if self._pipeline_kwargs and (
            "task" in self._pipeline_kwargs or "model" in self._pipeline_kwargs
        ):
            warn(
                "Specifying 'task' or 'model' in 'pipeline_kwargs' is not allowed",
                UserWarning,
            )
            self._pipeline_kwargs.pop("task", None)
            self._pipeline_kwargs.pop("model", None)

    def _load(self) -> Pipeline:
        return pipeline(self._task, model=self._model_name, **self._pipeline_kwargs)

    def _save(self, pipeline: Pipeline) -> None:
        raise NotImplementedError("Not yet implemented")

    def _describe(self) -> dict[str, Any]:
        return {
            "task": self._task,
            "model_name": self._model_name,
            "pipeline_kwargs": self._pipeline_kwargs,
        }
