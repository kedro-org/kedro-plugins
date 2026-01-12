"""``AbstractDataset`` implementation to load/save torch models using DartsTorchForecastingModel's built-in methods """

from typing import Any

import lazy_loader as lazy

DartsTorchModelDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"darts_torch_model_dataset": ["DartsTorchModelDataset"]}
)
