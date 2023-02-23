import logging
from typing import Any, Dict, Union

import mlflow
from kedro.io.core import AbstractDataSet
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient

from .common import ModelOpsException

logger = logging.getLogger(__name__)


class MLFlowMetrics(AbstractDataSet):
    def __init__(
        self,
        prefix: str = None,
        run_id: str = None,
        registered_model_name: str = None,
        registered_model_version: str = None,
    ):
        if None in (registered_model_name, registered_model_version):
            if registered_model_name or registered_model_version:
                raise ModelOpsException(
                    "'registered_model_name' and "
                    "'registered_model_version' should be "
                    "set together"
                )

        if run_id and registered_model_name:
            raise ModelOpsException(
                "'run_id' cannot be passed when " "'registered_model_name' is set"
            )

        self._prefix = prefix
        self._run_id = run_id
        self._registered_model_name = registered_model_name
        self._registered_model_version = registered_model_version

        if registered_model_name:
            self._version = f"{registered_model_name}/{registered_model_version}"
        else:
            self._version = run_id

    def _save(self, metrics: Dict[str, Union[str, float, int]]) -> None:
        if self._prefix is not None:
            metrics = {f"{self._prefix}_{key}": value for key, value in metrics.items()}
        mlflow.log_metrics(metrics)

        run_id = mlflow.active_run().info.run_id
        if self._version is not None:
            logger.warning(
                f"Ignoring version {self._version.save} set "
                f"earlier, will use version='{run_id}' for loading"
            )
        self._version = run_id

    def _load(self) -> Any:
        if self._version is None:
            msg = (
                "Could not determine the version to load. "
                "Please specify either 'run_id' or 'registered_model_name' "
                "along with 'registered_model_version' explicitly in "
                "MLFlowMetrics constructor"
            )
            raise MlflowException(msg)

        client = MlflowClient()

        if "/" in self._version:
            model_uri = f"models:/{self._version}"
            model = mlflow.pyfunc.load_model(model_uri)
            run_id = model._model_meta.run_id
        else:
            run_id = self._version

        run = client.get_run(run_id)
        metrics = run.data.metrics
        if self._prefix is not None:
            metrics = {
                key[len(self._prefix) + 1 :]: value
                for key, value in metrics.items()
                if key[: len(self._prefix)] == self._prefix
            }
        return metrics

    def _describe(self) -> Dict[str, Any]:
        return dict(
            prefix=self._prefix,
            run_id=self._run_id,
            registered_model_name=self._registered_model_name,
            registered_model_version=self._registered_model_version,
        )
