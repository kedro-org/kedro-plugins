"""``kedro_datasets`` is where you can find all of Kedro's data connectors."""

__all__ = ["KedroDeprecationWarning"]
__version__ = "9.3.0"

import sys
import warnings

with warnings.catch_warnings():
    # TODO: remove once Kedro releases Python 3.14 support
    warnings.filterwarnings("ignore", "Kedro is not yet fully compatible")
    try:
        # Custom `KedroDeprecationWarning` class was added in Kedro 0.18.14.
        from kedro import KedroDeprecationWarning
    except ImportError:  # pragma: no cover

        class KedroDeprecationWarning(DeprecationWarning):  # type: ignore[no-redef]
            """Custom class for warnings about deprecated Kedro features."""


if not sys.warnoptions:
    warnings.simplefilter("default", KedroDeprecationWarning)
