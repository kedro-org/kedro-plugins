# Snowpark Testing Omitted
As of October 2024, the Snowpark connector is compatible with Python versions 3.9, 3.10, and 3.11. Python 3.12 is not supported yet.

Currently, the build process of kedro-datasets does not support testing different Python versions for each dataset. Additionally, each dataset test is required to have 100% coverage. Due to these constraints, the kedro-datasets/snowflake folder is excluded from pytest's coverage report.
