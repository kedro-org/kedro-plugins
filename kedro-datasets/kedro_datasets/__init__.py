"""``kedro_datasets`` is where you can find all of Kedro's data connectors. test"""

__all__ = ["KedroDeprecationWarning"]
__version__ = "3.0.1"

import sys
import warnings

try:
    # Custom `KedroDeprecationWarning` class was added in Kedro 0.18.14.
    from kedro import KedroDeprecationWarning
except ImportError:  # pragma: no cover

    class KedroDeprecationWarning(DeprecationWarning):  # type: ignore[no-redef]
        """Custom class for warnings about deprecated Kedro features."""


if not sys.warnoptions:
    warnings.simplefilter("default", KedroDeprecationWarning)
