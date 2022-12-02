"""
We disable execution of tests that require real Snowflake instance
to run by default. Providing -m snowflake option explicitly to
pytest will make these and only these tests run
"""
import pytest


def pytest_collection_modifyitems(config, items):
    markers_arg = config.getoption("-m")

    # Naive implementation to handle basic marker expressions
    # Will not work if someone will (ever) run pytest with complex marker
    # expressions like "-m spark and not (snowflake or pandas)"
    if (
        "snowflake" in markers_arg.lower()
        and "not snowflake" not in markers_arg.lower()
    ):
        return

    skip_snowflake = pytest.mark.skip(reason="need -m snowflake option to run")
    for item in items:
        if "snowflake" in item.keywords:
            item.add_marker(skip_snowflake)
