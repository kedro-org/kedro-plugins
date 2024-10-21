# Snowpark connector testing

Execution of connection tests for Snowpark connector requires real Snowflake instance access.

As of Oct-2024, the snowpark connector works with Python 3.9, 3.10 and 3.11. 3.12 is not supported yet.

## Snowflake permissions required
Credentials provided via environment variables should have following permissions granted to run tests successfully:
* Create tables in a given schema
* Drop tables in a given schema
* Insert rows into tables in a given schema
* Query tables in a given schema
* Query `INFORMATION_SCHEMA.TABLES` of respective database
