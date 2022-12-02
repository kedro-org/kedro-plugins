import pytest
import snowflake.snowpark as sp
from kedro_datasets.snowflake import SnowParkDataSet as spds
import pandas as pd
import datetime
import os


def get_connection():
    account = os.getenv('SF_ACCOUNT')
    warehouse = os.getenv('SF_WAREHOUSE')
    database = os.getenv('SF_DATABASE')
    role = os.getenv('SF_ROLE')
    user = os.getenv('SF_USER')
    schema = os.getenv('SF_SCHEMA')
    password = os.getenv('SF_PASSWORD')

    if not (account and warehouse and database and role and user and schema and password):
        raise Exception('Snowflake connection environment variables provided not in full')

    conn = {
    "account": account,
    "warehouse": warehouse,
    "database": database,
    "role": role,
    "user": user,
    "schema": schema,
    "password": password
    }
    return conn


def sf_setup_db(sf_session):
    # For table exists test
    run_query(sf_session, 'CREATE TABLE KEDRO_PYTEST_TESTEXISTS ("name" VARCHAR)')

    # For load test
    query = 'CREATE TABLE KEDRO_PYTEST_TESTLOAD ("name" VARCHAR, "age" INTEGER, "bday" date, "height" float, "insert_dttm" timestamp)'
    run_query(sf_session,query)

    query = "INSERT INTO HAE_DEV_DB.DATA.KEDRO_PYTEST_TESTLOAD VALUES ('John', 23, to_date('1999-12-02','YYYY-MM-DD'), 6.5, to_timestamp_ntz('2022-12-02 13:20:01', 'YYYY-MM-DD hh24:mi:ss'))"
    run_query(sf_session, query)

    query = "INSERT INTO HAE_DEV_DB.DATA.KEDRO_PYTEST_TESTLOAD VALUES ('Jane', 41, to_date('1981-01-03','YYYY-MM-DD'), 5.7, to_timestamp_ntz('2022-12-02 13:21:11', 'YYYY-MM-DD hh24:mi:ss'))"
    run_query(sf_session, query)


def sf_db_cleanup(sf_session):
    run_query(sf_session, "DROP TABLE IF EXISTS KEDRO_PYTEST_TESTEXISTS")
    run_query(sf_session, "DROP TABLE IF EXISTS KEDRO_PYTEST_TESTLOAD")

def run_query(session, query):
    df = session.sql(query)
    df.collect()
    return df

def pandas_equals_ignore_dtype(df1, df2):
    # Pytest will show respective stdout only if test fails
    # this will help to debug what was exactly not matching right away
    print(df1)
    print('--- comparing to ---')
    print(df2)
    c1 = df1.values.tolist()
    c2 = df2.values.tolist()

    for row in range(0, len(c1)):
        for column in range(0, len(c1[row])):
            if not c1[row][column] == c2[row][column]:
                print("{} not equal to {}".format(c1[row][column], c2[row][column]))
                return False
    return True


@pytest.fixture
def sf_session():
    sf_session = sp.Session.builder.configs(get_connection()).create()

    # Running cleanup in case previous run was interrupted w/o proper cleanup
    sf_db_cleanup(sf_session)
    sf_setup_db(sf_session)

    yield sf_session
    sf_db_cleanup(sf_session)
    sf_session.close()


class TestSnowParkDataSet:
    @pytest.mark.snowflake
    def test_save(self, sf_session):
        assert int(1) == int(1)

    @pytest.mark.snowflake
    def test_load(self, sf_session):
        to_match = pd.DataFrame(
            {
                "name": ["John", "Jane"],
                "age": [23, 41],
                "bday": [datetime.date(1999, 12, 2), datetime.date(1981, 1, 3)],
                "height": [6.5, 5.7],
                "insert_dttm": [datetime.datetime(2022, 12, 2, 13, 20, 1), datetime.datetime(2022, 12, 2, 13, 21, 11)]
            },
            columns=["name", "age", "bday", "height", "insert_dttm"]
        )

        df_sf = spds(table_name = 'KEDRO_PYTEST_TESTLOAD', credentials = get_connection())._load()
        sf = df_sf.to_pandas()

        # Ignoring dtypes as ex. age can be int8 vs int64 and pandas.compare
        # fails on that
        assert pandas_equals_ignore_dtype(to_match, sf) == True

    @pytest.mark.snowflake
    def test_exists(self, sf_session):
        df_e = spds(table_name = 'KEDRO_PYTEST_TESTEXISTS',
                    credentials = get_connection())
        df_ne = spds(table_name='KEDRO_PYTEST_TESTNEXISTS',
                  credentials=get_connection())
        assert df_e._exists() == True
        assert df_ne._exists() == False
        return