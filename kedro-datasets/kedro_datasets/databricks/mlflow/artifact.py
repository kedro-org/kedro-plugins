import logging
import os
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Dict

import mlflow
from kedro.io.core import AbstractDataSet
from kedro.utils import load_obj as load_dataset
from mlflow.exceptions import MlflowException
from mlflow.tracking.artifact_utils import _download_artifact_from_uri

from .common import MLFLOW_RUN_ID_ENV_VAR, ModelOpsException

logger = logging.getLogger(__name__)


class MLFlowArtifact(AbstractDataSet):
    def __init__(
        self,
        dataset_name: str,
        dataset_type: str,
        dataset_args: Dict[str, Any] = None,
        *,
        file_suffix: str,
        run_id: str = None,
        registered_model_name: str = None,
        registered_model_version: str = None,
    ):
        """
        Log arbitrary Kedro datasets as mlflow artifacts

        Args:
            dataset_name: dataset name as it should appear on mlflow run
            dataset_type: full kedro dataset class name (incl. module)
            dataset_args: kedro dataset args
            file_suffix: file extension as it should appear on mlflow run
            run_id: mlflow run-id, this should only be used when loading a
                dataset saved from run which is different from active run
            registered_model_name: mlflow registered model name, this should
                only be used when loading an artifact linked to a model of
                interest (i.e. back tracing atifacts from the run corresponding
                to the model)
            registered_model_version: mlflow registered model name, should be
                used in combination with `registered_model_name`

            `run_id` and `registered_model_name` can't be specified together.
        """
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

        self._dataset_name = dataset_name
        self._dataset_type = dataset_type
        self._dataset_args = dataset_args or {}
        self._file_suffix = file_suffix
        self._run_id = run_id or os.environ.get(MLFLOW_RUN_ID_ENV_VAR)
        self._registered_model_name = registered_model_name
        self._registered_model_version = registered_model_version

        self._artifact_path = f"{dataset_name}{self._file_suffix}"

        self._filepath = Path(mkdtemp()) / self._artifact_path

        if registered_model_name:
            self._version = f"{registered_model_name}/{registered_model_version}"
        else:
            self._version = run_id

    def _save(self, data: Any) -> None:
        cls = load_dataset(self._dataset_type)
        ds = cls(filepath=self._filepath.as_posix(), **self._dataset_args)
        ds.save(data)

        filepath = self._filepath.as_posix()
        if os.path.isdir(filepath):
            mlflow.log_artifacts(self._filepath.as_posix(), self._artifact_path)
        elif os.path.isfile(filepath):
            mlflow.log_artifact(self._filepath.as_posix())
        else:
            raise RuntimeError("cls.save() didn't work. Unexpected error.")

        run_id = mlflow.active_run().info.run_id
        if self._version is not None:
            logger.warning(
                f"Ignoring version {self._version} set "
                f"earlier, will use version='{run_id}' for loading"
            )
        self._version = run_id

    def _load(self) -> Any:
        if self._version is None:
            msg = (
                "Could not determine the version to load. "
                "Please specify either 'run_id' or 'registered_model_name' "
                "along with 'registered_model_version' explicitly in "
                "MLFlowArtifact constructor"
            )
            raise MlflowException(msg)

        if "/" in self._version:
            model_uri = f"models:/{self._version}"
            model = mlflow.pyfunc.load_model(model_uri)
            run_id = model._model_meta.run_id
        else:
            run_id = self._version

        local_path = _download_artifact_from_uri(
            f"runs:/{run_id}/{self._artifact_path}"
        )

        cls = load_dataset(self._dataset_type)
        ds = cls(filepath=local_path, **self._dataset_args)
        return ds.load()

    def _describe(self) -> Dict[str, Any]:
        return dict(
            dataset_name=self._dataset_name,
            dataset_type=self._dataset_type,
            dataset_args=self._dataset_args,
            file_suffix=self._file_suffix,
            registered_model_name=self._registered_model_name,
            registered_model_version=self._registered_model_version,
        )
