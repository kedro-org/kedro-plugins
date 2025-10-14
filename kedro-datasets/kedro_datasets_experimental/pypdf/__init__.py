"""``AbstractDataset`` implementation to load data from PDF files using pypdf."""

from typing import Any

import lazy_loader as lazy

try:
    from .pdf_dataset import PDFDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    PDFDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"pdf_dataset": ["PDFDataset"]}
)
