"""Provide data loading and saving functionality for Optuna's study."""

from typing import Any

import lazy_loader as lazy

try:
    from .study_dataset import StudyDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    StudyDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "study_dataset": ["StudyDataset"],
    },
)
