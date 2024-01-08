import datetime
import os

import pytest
from kedro.io.core import DatasetError

try:
    import snowflake.snowpark as sp

    from kedro_datasets.snowflake import SnowparkTableDataset as spds
except ImportError:
    pass  # this is only for test discovery to succeed on Python <> 3.8


def get_connection():
    account = os.getenv("SNOWSQL_ACCOUNT")
    warehouse = os.getenv("SNOWSQL_WAREHOUSE")
    database = os.getenv("SNOWSQL_DATABASE")
    role = os.getenv("SNOWSQL_ROLE")
    user = os.getenv("SNOWSQL_USER")
    schema = os.getenv("SNOWSQL_SCHEMA")
    password = os.getenv("SNOWSQL_PWD")

    if not (
        account and warehouse and database and role and user and schema and password
    ):
        raise DatasetError(
            "Snowflake connection environment variables provided not in full"
        )

    conn = {
        "account": account,
        "warehouse": warehouse,
        "database": database,
        "role": role,
        "user": user,
        "schema": schema,
        "password": password,
    }
    return conn


def sf_setup_db(sf_session):
    # For table exists test
    run_query(sf_session, 'CREATE TABLE KEDRO_PYTEST_TESTEXISTS ("name" VARCHAR)')

    # For load test
    query = 'CREATE TABLE KEDRO_PYTEST_TESTLOAD ("name" VARCHAR\
                                                , "age" INTEGER\
                                                , "bday" date\
                                                , "height" float\
                                                , "insert_dttm" timestamp)'
    run_query(sf_session, query)

    query = "INSERT INTO KEDRO_PYTEST_TESTLOAD VALUES ('John'\
                                                        , 23\
                                                        , to_date('1999-12-02','YYYY-MM-DD')\
                                                        , 6.5\
                                                        , to_timestamp_ntz('2022-12-02 13:20:01',\
                                                                        'YYYY-MM-DD hh24:mi:ss'))"
    run_query(sf_session, query)

    query = "INSERT INTO KEDRO_PYTEST_TESTLOAD VALUES ('Jane'\
                                                        , 41\
                                                        , to_date('1981-01-03','YYYY-MM-DD')\
                                                        , 5.7\
                                                        , to_timestamp_ntz('2022-12-02 13:21:11',\
                                                                        'YYYY-MM-DD hh24:mi:ss'))"
    run_query(sf_session, query)


def sf_db_cleanup(sf_session):
    run_query(sf_session, "DROP TABLE IF EXISTS KEDRO_PYTEST_TESTEXISTS")
    run_query(sf_session, "DROP TABLE IF EXISTS KEDRO_PYTEST_TESTLOAD")
    run_query(sf_session, "DROP TABLE IF EXISTS KEDRO_PYTEST_TESTSAVE")


def run_query(session, query):
    df = session.sql(query)
    df.collect()
    return df


def df_equals_ignore_dtype(df1, df2):
    # Pytest will show respective stdout only if test fails
    # this will help to debug what was exactly not matching right away

    c1 = df1.to_pandas().values.tolist()
    c2 = df2.to_pandas().values.tolist()

    print(c1)
    print("--- comparing to ---")
    print(c2)

    for i, row in enumerate(c1):
        for j, column in enumerate(row):
            if not column == c2[i][j]:
                print(f"{column} not equal to {c2[i][j]}")
                return False
    return True


@pytest.fixture
def sample_sp_df(sf_session):
    return sf_session.create_dataframe(
        [
            [
                "John",
                23,
                datetime.date(1999, 12, 2),
                6.5,
                datetime.datetime(2022, 12, 2, 13, 20, 1),
            ],
            [
                "Jane",
                41,
                datetime.date(1981, 1, 3),
                5.7,
                datetime.datetime(2022, 12, 2, 13, 21, 11),
            ],
        ],
        schema=["name", "age", "bday", "height", "insert_dttm"],
    )


@pytest.fixture
def sf_session():
    sf_session = sp.Session.builder.configs(get_connection()).create()

    # Running cleanup in case previous run was interrupted w/o proper cleanup
    sf_db_cleanup(sf_session)
    sf_setup_db(sf_session)

    yield sf_session
    sf_db_cleanup(sf_session)
    sf_session.close()


class TestSnowparkTableDataset:
    @pytest.mark.snowflake
    def test_save(self, sample_sp_df, sf_session):
        sp_df = spds(table_name="KEDRO_PYTEST_TESTSAVE", credentials=get_connection())
        sp_df._save(sample_sp_df)
        sp_df_saved = sf_session.table("KEDRO_PYTEST_TESTSAVE")
        assert sp_df_saved.count() == 2

    @pytest.mark.snowflake
    def test_load(self, sample_sp_df, sf_session):
        print(sf_session)
        sp_df = spds(
            table_name="KEDRO_PYTEST_TESTLOAD", credentials=get_connection()
        )._load()

        # Ignoring dtypes as ex. age can be int8 vs int64 and pandas.compare
        # fails on that
        assert df_equals_ignore_dtype(sample_sp_df, sp_df)

    @pytest.mark.snowflake
    def test_exists(self, sf_session):
        print(sf_session)
        df_e = spds(table_name="KEDRO_PYTEST_TESTEXISTS", credentials=get_connection())
        df_ne = spds(
            table_name="KEDRO_PYTEST_TESTNEXISTS", credentials=get_connection()
        )
        assert df_e._exists()
        assert not df_ne._exists()
