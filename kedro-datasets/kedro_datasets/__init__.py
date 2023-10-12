"""``kedro_datasets`` is where you can find all of Kedro's data connectors."""

__all__ = ["KedroDeprecationWarning"]
__version__ = "1.7.1"

import sys
import warnings

try:
    # Custom `KedroDeprecationWarning` class was added in Kedro 0.18.14.
    from kedro import KedroDeprecationWarning
except ImportError:

    class KedroDeprecationWarning(DeprecationWarning):
        """Custom class for warnings about deprecated Kedro features."""


if not sys.warnoptions:
    warnings.simplefilter("default", KedroDeprecationWarning)
