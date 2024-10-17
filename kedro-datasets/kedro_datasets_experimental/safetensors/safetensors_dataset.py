from __future__ import annotations

import importlib
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any

import fsspec
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)


class SafetensorsDataset(AbstractVersionedDataset[Any, Any]):

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        backend: str = "torch",
    ) -> None:
        pass

    def load(self) -> Any:
        pass

    def save(self, data: Any) -> None:
        pass

    def _exists(self) -> bool:
        pass