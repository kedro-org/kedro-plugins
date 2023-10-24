"""Provides interface to Hugging Face transformers and datasets."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
HFDataset: Any
HFTransformerPipelineDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "hugging_face_dataset": ["HFDataset"],
        "transformer_pipeline_dataset": ["HFTransformerPipelineDataset"],
    },
)
