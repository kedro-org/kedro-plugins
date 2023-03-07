# Snowpark connector testing

Execution of automated tests for Snowpark connector requires real Snowflake instance access. Therefore tests located in this folder are **disabled** by default from pytest execution scope using [conftest.py](conftest.py).

[Makefile](/Makefile) provides separate argument ``test-snowflake-only`` to run only tests related to Snowpark connector. To run tests one need to provide Snowflake connection parameters via environment variables:
* SNOWSQL_ACCOUNT - Snowflake account name with region. Ex `ab12345.eu-central-2`
* SNOWSQL_WAREHOUSE - Snowflake virtual warehouse to use
* SNOWSQL_DATABASE - Database to use
* SNOWSQL_SCHEMA - Schema to use when creating tables for tests
* SNOWSQL_ROLE - Role to use for connection
* SNOWSQL_USER - Username to use for connection
* SNOWSQL_PWD - Plain password to use for connection

All environment variables need to be provided for tests to run.

Here is example shell command to run snowpark tests via make utility:
```bash
SNOWSQL_ACCOUNT='ab12345.eu-central-2' SNOWSQL_WAREHOUSE='DEV_WH' SNOWSQL_DATABASE='DEV_DB' SNOWSQL_ROLE='DEV_ROLE' SNOWSQL_USER='DEV_USER' SNOWSQL_SCHEMA='DATA' SNOWSQL_PWD='supersecret' make test-snowflake-only
```

Currently running tests supports only simple username & password authentication and not SSO/MFA.

As of Mar-2023, the snowpark connector only works with Python 3.8.

## Snowflake permissions required
Credentials provided via environment variables should have following permissions granted to run tests successfully:
* Create tables in a given schema
* Drop tables in a given schema
* Insert rows into tables in a given schema
* Query tables in a given schema
* Query `INFORMATION_SCHEMA.TABLES` of respective database

## Extending tests
Contributors adding new tests should add `@pytest.mark.snowflake` decorator to each test. Exclusion of Snowpark-related pytests from overall execution scope in [conftest.py](conftest.py) works based on markers.
