"""Unit tests for ``PolarsDatabaseDataset``."""

import fsspec
import polars as pl
import pytest
from kedro.io.core import DatasetError
from polars.testing import assert_frame_equal
from sqlalchemy import create_engine, text

from kedro_datasets_experimental.polars import PolarsDatabaseDataset

CONNECTION = "sqlite:///:memory:"


@pytest.fixture(autouse=True)
def cleanup_engines():
    yield
    PolarsDatabaseDataset.engines = {}


@pytest.fixture
def sqlite_credentials(tmp_path):
    return {"con": f"sqlite:///{tmp_path / 'test.db'}"}


@pytest.fixture
def numeric_frame():
    """Int-only frame with order-line-item columns."""
    return pl.DataFrame(
        {"quantity": [1, 2], "total": [4, 5], "units_sold": [5, 6]}
    )


@pytest.fixture
def string_frame():
    """String-only frame, different schema from ``numeric_frame``."""
    return pl.DataFrame(
        {"name": ["alice", "bob", "carol"], "city": ["NYC", "LA", "SF"]}
    )


@pytest.fixture
def mixed_frame():
    """Mixed-dtype frame: int / string / float."""
    return pl.DataFrame(
        {"id": [10, 20, 30, 40], "label": ["a", "b", "c", "d"], "score": [1.5, 2.5, 3.5, 4.5]}
    )


@pytest.fixture
def seed_table(sqlite_credentials):
    """Factory fixture: seeds ``table_name`` with ``frame`` and returns credentials."""

    def _seed(table_name, frame):
        writer = PolarsDatabaseDataset(
            table_name=table_name,
            credentials=sqlite_credentials,
        )
        writer.save(frame)
        return sqlite_credentials

    return _seed


class TestPolarsDatabaseDataset:
    def test_no_query_or_table_name(self):
        """Check the error when none of ``sql``, ``filepath``, ``table_name`` is given."""
        with pytest.raises(DatasetError):
            PolarsDatabaseDataset(credentials={"con": CONNECTION})

    def test_sql_and_filepath_args(self, seed_table, numeric_frame, tmp_path):
        """Check the error when both ``sql`` and ``filepath`` are provided."""
        table_name = "validation_check"
        sql = f"SELECT * FROM {table_name}"
        credentials = seed_table(table_name, numeric_frame)
        query_file = tmp_path / "query.sql"
        query_file.write_text(sql)

        with pytest.raises(DatasetError):
            PolarsDatabaseDataset(
                sql=sql,
                filepath=query_file.as_posix(),
                table_name=table_name,
                credentials=credentials,
            )

    def test_empty_connection(self):
        """Check the error when instantiating with an empty connection string."""
        with pytest.raises(DatasetError):
            PolarsDatabaseDataset(
                sql="SELECT * FROM any_table",
                table_name="any_table",
                credentials={"con": ""},
            )

    @pytest.mark.parametrize("value", range(5))
    def test_load_trivial_sql(self, value):
        """``SELECT <value>`` executes against the configured connection and returns ``value``."""
        dataset = PolarsDatabaseDataset(
            sql=f"SELECT {value}",
            credentials={"con": CONNECTION},
        )
        assert dataset.load().item() == value

    def test_save_no_table_name(self, numeric_frame):
        """``save`` raises when the dataset was configured without ``table_name``."""
        dataset = PolarsDatabaseDataset(
            sql="SELECT 1",
            credentials={"con": CONNECTION},
        )
        with pytest.raises(DatasetError):
            dataset.save(numeric_frame)

    def test_load_with_sql(self, seed_table, string_frame):
        """``load`` returns the seeded data when ``sql`` is configured."""
        table_name = "shuttles"
        credentials = seed_table(table_name, string_frame)
        dataset = PolarsDatabaseDataset(
            sql=f"SELECT * FROM {table_name}",
            credentials=credentials,
        )
        assert_frame_equal(dataset.load(), string_frame)

    def test_load_query_file(self, seed_table, numeric_frame, tmp_path):
        """``load`` reads the SQL query from a file and returns the seeded data."""
        table_name = "orders"
        credentials = seed_table(table_name, numeric_frame)
        query_file = tmp_path / "orders_query.sql"
        query_file.write_text(f"SELECT quantity, total, units_sold FROM {table_name}")

        dataset = PolarsDatabaseDataset(
            filepath=query_file.as_posix(),
            credentials=credentials,
        )
        assert_frame_equal(dataset.load(), numeric_frame)

    def test_load_table_name_only(self, seed_table, mixed_frame):
        """``load`` builds ``SELECT * FROM <table_name>`` and returns the seeded data."""
        table_name = "users"
        credentials = seed_table(table_name, mixed_frame)
        dataset = PolarsDatabaseDataset(
            table_name=table_name,
            credentials=credentials,
        )
        assert_frame_equal(dataset.load(), mixed_frame)

    def test_sql_overrides_table_name_on_load(self, seed_table, string_frame):
        """``sql`` takes precedence over ``table_name`` for load when both are given."""
        table_name = "source_table"
        credentials = seed_table(table_name, string_frame)
        dataset = PolarsDatabaseDataset(
            sql=f"SELECT * FROM {table_name}",
            table_name="nonexistent_target",
            credentials=credentials,
        )
        assert_frame_equal(dataset.load(), string_frame)

    def test_filepath_overrides_table_name_on_load(
        self, seed_table, mixed_frame, tmp_path
    ):
        """``filepath`` takes precedence over ``table_name`` for load when both are given."""
        table_name = "source_table"
        credentials = seed_table(table_name, mixed_frame)
        query_file = tmp_path / "q.sql"
        query_file.write_text(f"SELECT * FROM {table_name}")

        dataset = PolarsDatabaseDataset(
            filepath=query_file.as_posix(),
            table_name="nonexistent_target",
            credentials=credentials,
        )
        assert_frame_equal(dataset.load(), mixed_frame)

    def test_load_schema_overrides(self, seed_table, numeric_frame):
        """``load_args`` are forwarded to ``polars.read_database`` (schema_overrides)."""
        table_name = "metrics"
        credentials = seed_table(table_name, numeric_frame)
        dataset = PolarsDatabaseDataset(
            table_name=table_name,
            credentials=credentials,
            load_args={"schema_overrides": {"quantity": pl.Float64}},
        )
        assert dataset.load().schema["quantity"] == pl.Float64

    def test_save_writes_rows_to_table(self, sqlite_credentials, string_frame):
        """``save`` writes all rows from the DataFrame to the target table."""
        table_name = "events"
        dataset = PolarsDatabaseDataset(
            table_name=table_name,
            credentials=sqlite_credentials,
        )
        dataset.save(string_frame)

        engine = create_engine(sqlite_credentials["con"])
        with engine.connect() as conn:
            row_count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).scalar()
        assert row_count == len(string_frame)

    def test_save_default_replaces_existing_table(
        self, seed_table, mixed_frame, numeric_frame
    ):
        """The default ``if_table_exists='replace'`` overwrites the existing table."""
        table_name = "snapshots"
        credentials = seed_table(table_name, mixed_frame)
        dataset = PolarsDatabaseDataset(
            table_name=table_name,
            credentials=credentials,
        )
        # precondition: seeded ``mixed_frame`` is actually in the table
        assert_frame_equal(dataset.load(), mixed_frame)

        dataset.save(numeric_frame)

        assert_frame_equal(dataset.load(), numeric_frame)

    def test_save_append_adds_to_existing_table(self, seed_table, numeric_frame):
        """``save_args={'if_table_exists': 'append'}`` adds rows instead of overwriting."""
        table_name = "audit_log"
        credentials = seed_table(table_name, numeric_frame)
        dataset = PolarsDatabaseDataset(
            table_name=table_name,
            credentials=credentials,
            save_args={"if_table_exists": "append"},
        )
        dataset.save(numeric_frame)

        engine = create_engine(credentials["con"])
        with engine.connect() as conn:
            row_count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).scalar()
        assert row_count == 2 * len(numeric_frame)

    def test_fs_args_forwarded_to_fsspec(self, mocker):
        """``fs_args`` and the popped ``credentials`` are passed to ``fsspec.filesystem``."""
        mock_fs = mocker.patch("fsspec.filesystem")
        PolarsDatabaseDataset(
            filepath="s3://bucket/remote_query.sql",
            table_name="remote_query",
            credentials={"con": CONNECTION},
            fs_args={"credentials": {"key": "x", "secret": "y"}, "anon": False},
        )
        mock_fs.assert_called_once_with("s3", key="x", secret="y", anon=False)

    def test_load_reads_query_through_fsspec_filesystem(
        self, seed_table, numeric_frame
    ):
        """``load`` reads the SQL file through the configured fsspec filesystem."""
        table_name = "memorable"
        query_path = f"/queries/{table_name}.sql"
        credentials = seed_table(table_name, numeric_frame)
        mem_fs = fsspec.filesystem("memory")
        with mem_fs.open(query_path, "wb") as f:
            f.write(f"SELECT quantity, total, units_sold FROM {table_name}".encode())

        dataset = PolarsDatabaseDataset(
            filepath=f"memory://{query_path}",
            credentials=credentials,
        )
        assert_frame_equal(dataset.load(), numeric_frame)

    def test_str_representation_sql_includes_sql(self):
        """The string repr exposes the configured SQL query."""
        sql_query = "SELECT * FROM products"
        dataset = PolarsDatabaseDataset(
            sql=sql_query,
            table_name="products",
            credentials={"con": CONNECTION},
        )
        assert f"sql='{sql_query}'" in str(dataset)

    def test_str_representation_sql_includes_table_name(self):
        """The string repr exposes the configured table name."""
        dataset = PolarsDatabaseDataset(
            sql="SELECT * FROM customers",
            table_name="customers",
            credentials={"con": CONNECTION},
        )
        assert "table_name='customers'" in str(dataset)

    def test_str_representation_sql_excludes_connection_string(self):
        """The string repr never leaks the connection string."""
        dataset = PolarsDatabaseDataset(
            sql="SELECT * FROM secrets_check",
            table_name="secrets_check",
            credentials={"con": CONNECTION},
        )
        assert CONNECTION not in str(dataset)

    def test_str_representation_filepath_includes_filepath(self, tmp_path):
        """The string repr exposes the configured filepath."""
        query_file = tmp_path / "reports_query.sql"
        query_file.write_text("SELECT * FROM reports")

        dataset = PolarsDatabaseDataset(
            filepath=query_file.as_posix(),
            table_name="reports",
            credentials={"con": CONNECTION},
        )
        assert f"filepath='{query_file.as_posix()}'" in str(dataset)

    def test_str_representation_filepath_includes_table_name(self, tmp_path):
        """The string repr exposes the configured table name in filepath mode."""
        query_file = tmp_path / "metrics_repr_query.sql"
        query_file.write_text("SELECT * FROM metrics_repr")

        dataset = PolarsDatabaseDataset(
            filepath=query_file.as_posix(),
            table_name="metrics_repr",
            credentials={"con": CONNECTION},
        )
        assert "table_name='metrics_repr'" in str(dataset)

    def test_str_representation_filepath_excludes_connection_string(self, tmp_path):
        """The string repr never leaks the connection string in filepath mode."""
        query_file = tmp_path / "audit_repr_query.sql"
        query_file.write_text("SELECT * FROM audit_repr")

        dataset = PolarsDatabaseDataset(
            filepath=query_file.as_posix(),
            table_name="audit_repr",
            credentials={"con": CONNECTION},
        )
        assert CONNECTION not in str(dataset)
