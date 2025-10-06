"""``AbstractDataset`` implementation to load/save data from/to OpenXML files."""

from typing import Any

import lazy_loader as lazy

try:
    from .docx_dataset import DocxDataset
except (ImportError, RuntimeError):
    DocxDataset: Any

try:
    from .pptx_dataset import PptxDataset
except (ImportError, RuntimeError):
    PptxDataset: Any

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"docx_dataset": ["DocxDataset"], "pptx_dataset": ["PptxDataset"]},
)
