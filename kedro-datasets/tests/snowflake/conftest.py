"""
We disable execution of tests that require real Snowflake instance
to run by default. Providing -m snowflake option explicitly to
pytest will make these and only these tests run
"""
import pytest


def pytest_collection_modifyitems(config, items):
    markers_arg = config.getoption("-m")

    # Naive implementation that will not work with complex marker expressions
    # provided like "-m spark and not (snowflake or pandas)" but we are far
    # from having these use-cases
    if (
        "snowflake" in markers_arg.lower()
        and "not snowflake" not in markers_arg.lower()
    ):
        return

    skip_snowflake = pytest.mark.skip(reason="need -m snowflake option to run")
    for item in items:
        if "snowflake" in item.keywords:
            item.add_marker(skip_snowflake)
