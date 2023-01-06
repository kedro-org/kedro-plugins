# Snowpark connector testing

Execution of automated tests for Snowpark connector requires real Snowflake instance access. Therefore tests located in this folder are **disabled** by default from pytest execution scope using [conftest.py](conftest.py). 

[Makefile](/Makefile) provides separate argument ``test-snowflake-only`` to run only tests related to Snowpark connector. To run tests one need to provide Snowflake connection parameters via environment variables:
* SF_ACCOUNT - Snowflake account name with region. Ex `ab12345.eu-central-2`
* SF_WAREHOUSE - Snowflake virtual warehouse to use
* SF_DATABASE - Database to use
* SF_SCHEMA - Schema to use when creating tables for tests
* SF_ROLE - Role to use for connection
* SF_USER - Username to use for connection
* SF_PASSWORD - Plain password to use for connection

All environment variables need to be provided for tests to run.

Here is example shell command to run snowpark tests via make utility: 
```bash
SF_ACCOUNT='ab12345.eu-central-2' SF_WAREHOUSE='DEV_WH' SF_DATABASE='DEV_DB' SF_ROLE='DEV_ROLE' SF_USER='DEV_USER' SF_SCHEMA='DATA' SF_PASSWORD='supersecret' make test-snowflake-only
```

Currently running tests supports only simple username & password authentication and not SSO/MFA.

## Snowflake permissions required
Credentials provided via environment variables should have following permissions granted to run tests successfully:
* Create tables in a given schema
* Drop tables in a given schema
* Insert rows into tables in a given schema
* Query tables in a given schema
* Query `INFORMATION_SCHEMA.TABLES` of respective database

## Extending tests
Contributors adding new tests should add `@pytest.mark.snowflake` decorator to each test. Exclusion of Snowpark-related pytests from overall execution scope in [conftest.py](conftest.py) works based on markers. 