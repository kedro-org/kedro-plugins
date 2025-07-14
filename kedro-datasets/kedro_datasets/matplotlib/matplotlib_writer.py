"""``MatplotlibWriter`` is deprecated and will be removed in a future release.
Please use ``MatplotlibDataset`` instead.
"""
from __future__ import annotations

from warnings import warn

from kedro_datasets import KedroDeprecationWarning
from kedro_datasets.matplotlib.matplotlib_dataset import MatplotlibDataset


class MatplotlibWriter(MatplotlibDataset):
    """``MatplotlibWriter`` saves one or more Matplotlib objects as image
    files to an underlying filesystem (e.g. local, S3, GCS).

    .. warning::
        This class is deprecated and will be removed in a future release.
        Please use ``MatplotlibDataset`` instead.
    """

    def __init__(self, **kwargs) -> None:
        """Creates a new instance of ``MatplotlibWriter``.

        .. warning::
            This class is deprecated and will be removed in a future release.
            Please use ``MatplotlibDataset`` instead.
        """
        warn(
            "The MatplotlibWriter class has been renamed to MatplotlibDataset. "
            "The MatplotlibWriter name is deprecated and will be removed in a future release.",
            KedroDeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)
