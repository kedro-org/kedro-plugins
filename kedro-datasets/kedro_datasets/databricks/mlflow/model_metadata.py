import logging
from typing import Any, Dict, Union

import mlflow
from kedro.io.core import AbstractDataSet

from .common import ModelOpsException, parse_model_uri

logger = logging.getLogger(__name__)


class MLFlowModelMetadata(AbstractDataSet):
    def __init__(
        self, registered_model_name: str, registered_model_version: str = None
    ):
        self._model_name = registered_model_name
        self._model_version = registered_model_version

    def _save(self, tags: Dict[str, Union[str, float, int]]) -> None:
        raise NotImplementedError()

    def _load(self) -> Any:
        if self._model_version is None:
            model_uri = f"models:/{self._model_name}"
        else:
            model_uri = f"models:/{self._model_name}/{self._model_version}"
        _, _, load_version = parse_model_uri(model_uri)

        if load_version is None:
            raise ModelOpsException(
                f"No model with version " f"'{self._model_version}'"
            )

        pyfunc_model = mlflow.pyfunc.load_model(
            f"models:/{self._model_name}/{load_version}"
        )
        all_metadata = pyfunc_model._model_meta
        model_metadata = {
            "model_name": self._model_name,
            "model_version": int(load_version),
            "run_id": all_metadata.run_id,
        }
        return model_metadata

    def _describe(self) -> Dict[str, Any]:
        return dict(
            registered_model_name=self._model_name,
            registered_model_version=self._model_version,
        )
