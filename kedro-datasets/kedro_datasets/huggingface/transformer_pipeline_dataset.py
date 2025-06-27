from __future__ import annotations

import typing as t
from warnings import warn

from kedro.io import AbstractDataset
from transformers import Pipeline, pipeline


class HFTransformerPipelineDataset(AbstractDataset):
    """``HFTransformerPipelineDataset`` loads pretrained Hugging Face transformers
    using the `transformers <https://pypi.org/project/transformers>`_ library.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        summarizer_model:
          type: huggingface.HFTransformerPipelineDataset
          task: summarization

        fill_mask_model:
          type: huggingface.HFTransformerPipelineDataset
          task: fill-mask
          model_name: Twitter/twhin-bert-base
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> from kedro_datasets.huggingface import HFTransformerPipelineDataset
        >>>
        >>> dataset = HFTransformerPipelineDataset(
        ...     task="text-classification", model_name="prajjwal1/bert-tiny"
        ... )
        >>> model = dataset.load()
        >>> assert model("Hello world")[0]["label"].startswith("LABEL_")

    """

    def __init__(
        self,
        *,
        task: str | None = None,
        model_name: str | None = None,
        pipeline_kwargs: dict[str, t.Any] | None = None,
        metadata: dict[str, t.Any] | None = None,
    ):
        if task is None and model_name is None:
            raise ValueError("At least 'task' or 'model_name' are needed")
        self._task = task if task else None
        self._model_name = model_name
        self._pipeline_kwargs = pipeline_kwargs or {}
        self.metadata = metadata

        if self._pipeline_kwargs and (
            "task" in self._pipeline_kwargs or "model" in self._pipeline_kwargs
        ):
            warn(
                "Specifying 'task' or 'model' in 'pipeline_kwargs' is not allowed",
                UserWarning,
            )
            self._pipeline_kwargs.pop("task", None)
            self._pipeline_kwargs.pop("model", None)

    def load(self) -> Pipeline:
        return pipeline(self._task, model=self._model_name, **self._pipeline_kwargs)

    def save(self, pipeline: Pipeline) -> None:
        raise NotImplementedError("Not yet implemented")

    def _describe(self) -> dict[str, t.Any]:
        return {
            "task": self._task,
            "model_name": self._model_name,
            "pipeline_kwargs": self._pipeline_kwargs,
        }
