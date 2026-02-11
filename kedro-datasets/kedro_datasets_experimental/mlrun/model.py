"""MLRun model dataset for Kedro."""
import os
import tempfile
from typing import Any

import joblib
from mlrun import get_dataitem

from .abstract_artifact import MLRunAbstractDataset


class MLRunModel(MLRunAbstractDataset):
    """Dataset for saving/loading models via MLRun.

    Uses MLRun's
    `log_model <https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_model>`_
    and
    `get_artifact <https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.get_artifact>`_.
    ``load_args`` and ``save_args`` accept any arguments supported by the corresponding
    MLRun API for your MLRun version; see the MLRun docs.

    Examples:
        Using the
        `YAML API <https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/>`_:

        .. code-block:: yaml

            trained_model:
              type: kedro_datasets_experimental.mlrun.MLRunModel
              key: my_sklearn_model
              framework: sklearn
              model_format: pkl

        Using the
        `Python API <https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/>`_:

        .. code-block:: python

            from kedro_datasets_experimental.mlrun import MLRunModel

            dataset = MLRunModel(
                key="my_sklearn_model",
                framework="sklearn",
                model_format="pkl",
            )
            dataset.save(trained_model)
            loaded_model = dataset.load()

    Args:
        key: Artifact key for MLRun (defaults to catalog dataset name).
        framework: ML framework name (e.g. "sklearn", "xgboost", "lightgbm").
        model_format: File format/extension for saving the model (e.g. "pkl").
        load_args: Passed to MLRun when loading; see MLRun docs for your version.
        save_args: Passed to log_model; see MLRun docs for your version.
    """

    def __init__( # noqa: PLR0913
        self,
        key: str | None = None,
        framework: str = "sklearn",
        model_format: str = "pkl",
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(key=key, save_args=save_args, load_args=load_args)
        self._framework = framework
        self._model_format = model_format.lower().lstrip(".")

    def save(self, data: Any) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = os.path.join(tmpdir, f"model.{self._model_format}")
            joblib.dump(data, model_path)

            self._ctx_manager.context.log_model(
                key=self.key,
                model_file=model_path,
                framework=self._framework,
                **self._save_args
            )

    def load(self) -> Any:
        artifact = self._ctx_manager.project.get_artifact(self.key)
        target_path = artifact.get_target_path()
        model_file = artifact.model_file
        local_path = get_dataitem(target_path + model_file).local()
        return joblib.load(local_path)

    def _describe(self) -> dict[str, Any]:
        return {
            **super()._describe(),
            "framework": self._framework,
            "model_format": self._model_format,
        }
