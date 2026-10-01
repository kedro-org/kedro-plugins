"""A failed existence check must not read as "the table does not exist".

``kedro run --only-missing-outputs`` skips nodes whose outputs exist, using
``catalog.exists()``. If a dropped connection or a suspended warehouse makes
``_exists()`` return ``False``, the node runs again and overwrites the table.
"""

import pytest

snowflake = pytest.importorskip("snowflake")

from snowflake.snowpark import Session  # noqa: E402
from snowflake.snowpark import exceptions as sp_exceptions  # noqa: E402

from kedro_datasets.snowflake.snowpark_dataset import SnowparkTableDataset  # noqa: E402


@pytest.fixture(scope="module")
def local_session() -> Session:
    return Session.builder.config("local_testing", True).create()


@pytest.fixture
def dataset(local_session: Session) -> SnowparkTableDataset:
    return SnowparkTableDataset(
        table_name="NON_EXISTENT_TABLE",
        database="DUMMY_DATABASE",
        schema="DUMMY_SCHEMA",
        credentials={"account": "DUMMY_ACCOUNT", "warehouse": "DUMMY_WAREHOUSE"},
        session=local_session,
    )


def _table_raising(mocker, dataset, error):
    table = mocker.MagicMock()
    table.show.side_effect = error
    mocker.patch.object(dataset.session, "table", return_value=table)


def test_missing_table_is_still_false(dataset):
    # Local testing raises Snowflake's own "does not exist or not authorized" error.
    assert dataset._exists() is False


def test_snowflake_object_does_not_exist_is_still_false(mocker, dataset):
    _table_raising(
        mocker,
        dataset,
        sp_exceptions.SnowparkSQLException(
            "SQL compilation error:\nObject 'DUMMY_DATABASE.DUMMY_SCHEMA.T' "
            "does not exist or not authorized.",
            sql_error_code=2003,
        ),
    )

    assert dataset._exists() is False


def test_lost_session_raises(mocker, dataset):
    _table_raising(
        mocker, dataset, sp_exceptions.SnowparkSessionException("Session expired")
    )

    with pytest.raises(sp_exceptions.SnowparkSessionException):
        dataset._exists()


def test_other_sql_error_raises(mocker, dataset):
    _table_raising(
        mocker,
        dataset,
        sp_exceptions.SnowparkSQLException(
            "Warehouse 'DUMMY_WAREHOUSE' cannot be resumed because resource monitor "
            "has exceeded its quota.",
            sql_error_code=606,
        ),
    )

    with pytest.raises(sp_exceptions.SnowparkSQLException):
        dataset._exists()
