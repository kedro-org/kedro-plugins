import importlib
import logging
from typing import Any, Dict

from kedro.io.core import AbstractDataSet

from .common import ModelOpsException, parse_model_uri

logger = logging.getLogger(__name__)


class MLFlowDataSet(AbstractDataSet):
    def __init__(
        self,
        flavor: str,
        dataset_name: str = None,
        dataset_type: str = None,
        dataset_args: Dict[str, Any] = None,
        *,
        file_suffix: str = None,
        load_version: str = None,
    ):
        self._flavor = flavor
        self._dataset_name = dataset_name
        self._dataset_type = dataset_type
        self._dataset_args = dataset_args or {}
        self._file_suffix = file_suffix
        self._load_version = load_version

    def _save(self, model: Any) -> None:
        if self._load_version is not None:
            msg = (
                f"Trying to save an MLFlowDataSet::{self._describe} which "
                f"was initialized with load_version={self._load_version}. "
                f"This can lead to inconsistency between saved and loaded "
                f"versions, therefore disallowed. Please create separate "
                f"catalog entries for saved and loaded datasets."
            )
            raise ModelOpsException(msg)

        importlib.import_module(self._flavor).log_model(
            model,
            self._dataset_name,
            registered_model_name=self._dataset_name,
            dataset_type=self._dataset_type,
            dataset_args=self._dataset_args,
            file_suffix=self._file_suffix,
        )

    def _load(self) -> Any:
        *_, latest_version = parse_model_uri(f"models:/{self._dataset_name}")

        dataset_version = self._load_version or latest_version
        *_, dataset_version = parse_model_uri(
            f"models:/{self._dataset_name}/{dataset_version}"
        )

        logger.info(f"Loading model '{self._dataset_name}' version '{dataset_version}'")

        if dataset_version != latest_version:
            logger.warning(f"Newer version {latest_version} exists in repo")

        model = importlib.import_module(self._flavor).load_model(
            f"models:/{self._dataset_name}/{dataset_version}",
            dataset_type=self._dataset_type,
            dataset_args=self._dataset_args,
            file_suffix=self._file_suffix,
        )

        return model

    def _describe(self) -> Dict[str, Any]:
        return dict(
            flavor=self._flavor,
            dataset_name=self._dataset_name,
            dataset_type=self._dataset_type,
            dataset_args=self._dataset_args,
            file_suffix=self._file_suffix,
            load_version=self._load_version,
        )
