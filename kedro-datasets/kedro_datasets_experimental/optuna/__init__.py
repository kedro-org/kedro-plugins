"""Provide data loading and saving functionality for Optuna's study."""

from typing import Any

import lazy_loader as lazy

StudyDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "study_dataset": ["StudyDataset"],
    },
)
