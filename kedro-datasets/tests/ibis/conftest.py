import importlib
from unittest import mock

# Stub expensive/optional ibis backends that may raise ImportError when
# accessed via attribute lookup during tests (for example, ibis.mssql
# tries to import pyodbc which may not be installed in the test env).
# Creating module attributes here prevents ibis.__getattr__ from being
# invoked by tests that call mocker.patch("ibis.<backend>")...

_IBIS_BACKENDS_TO_STUB = ("mssql", "postgres",)

try:
    ibis = importlib.import_module("ibis")
except Exception:
    # If ibis itself is unavailable for some reason, nothing to stub.
    ibis = None

if ibis is not None:
    for _b in _IBIS_BACKENDS_TO_STUB:
        # Directly set attribute on the module to avoid triggering
        # ibis.__getattr__ lazy-loading logic when tests access it.
        try:
            setattr(ibis, _b, mock.MagicMock(name=f"ibis.{_b}"))
        except Exception:
            # Guard against any unexpected issues in CI/test envs.
            pass
