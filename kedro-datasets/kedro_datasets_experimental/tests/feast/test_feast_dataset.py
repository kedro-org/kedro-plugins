from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from feast import Entity, FeatureStore, FeatureView, Field, FileSource
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.repo_config import RepoConfig
from feast.types import Bool, Float32, Float64, Int64, String
from google.cloud import bigquery
from kedro.io.core import DatasetError

from kedro_datasets_experimental.feast import feast_dataset as feast_module
from kedro_datasets_experimental.feast.feast_dataset import FeastDataset


@pytest.fixture
def data():
    """Three more drivers to append on top of the seeded one."""
    return pd.DataFrame(
        {
            "driver_id": [1, 2, 3],
            "trips": [10, 20, 30],
            "event_timestamp": pd.to_datetime(
                ["2024-01-03", "2024-01-03", "2024-01-03"], utc=True
            ),
        }
    )


@pytest.fixture
def registry(tmp_path):
    """The on-disk Feast registry path, shared by the setup store (which
    materializes it via ``apply``) and the ``FeastDataset`` under test."""
    return str(tmp_path / "registry.db")


@pytest.fixture
def offline_store():
    return {"type": "file"}


@pytest.fixture
def online_store(tmp_path):
    return {"type": "sqlite", "path": str(tmp_path / "online.db")}


@pytest.fixture
def feature_view_name():
    return "driver_stats"

@pytest.fixture
def repo(registry, offline_store, online_store):
    """Repo config used by the ``FeastDataset`` under test."""
    return {
        # Pass the registry as a dict with an explicit ``registry_store_type`` so
        # Feast skips its URI-scheme guessing. On Windows a bare path like
        # ``C:\\...\\registry.db`` is parsed as scheme ``c`` (unsupported); naming
        # the store type routes straight to FileRegistryStore, which reads the
        # path with ``pathlib.Path``.
        "registry": {"path": registry, "registry_store_type": "FileRegistryStore"},
        "project": "test_project",
        "provider": "local",
        "offline_store": offline_store,
        "online_store": online_store,
    }


@pytest.fixture
def store(tmp_path, repo, registry, feature_view_name, data):
    """Register an (online-enabled) ``driver_stats`` feature view into the shared
    registry, and return the setup store (so tests can read the online store)."""
    data_path = tmp_path / "driver_stats.parquet"
    pd.DataFrame(
        {
            "driver_id": [0],
            "trips": [100],
            "event_timestamp": pd.to_datetime(["2024-01-01"], utc=True),
        }
    ).to_parquet(data_path)

    driver = Entity(name="driver", join_keys=["driver_id"])
    source = FileSource(path=str(data_path), timestamp_field="event_timestamp")
    feature_view = FeatureView(
        name=feature_view_name,
        entities=[driver],
        schema=[Field(name="driver_id", dtype=Int64), Field(name="trips", dtype=Int64)],
        source=source,
        online=True,
    )
    setup_store = FeatureStore(
        config=RepoConfig(
            registry=repo["registry"],
            project=repo["project"],
            provider=repo["provider"],
            offline_store=repo["offline_store"],
            online_store=repo["online_store"],
        )
    )
    setup_store.apply([driver, feature_view])
    setup_store.materialize(start_date=pd.Timestamp("2024-01-01", tz="UTC"), end_date=pd.Timestamp("2024-01-02", tz="UTC"))
    # apply materializes the registry on disk; the FeastDataset under test
    # reads this exact file.
    assert Path(registry).exists()
    return setup_store


@pytest.mark.parametrize(
    "write_mode, expected_online_trips",
    [
        # Offline-only write: the online store is left empty for new drivers.
        ("offline", [100, None, None, None]),
        # Online-and-offline write appends to the online store too.
        ("online_and_offline", [100, 10, 20, 30]),
    ],
)
def test_save_then_load_round_trips_through_offline_store(
    store, repo, feature_view_name, data, write_mode, expected_online_trips
):
    dataset = FeastDataset(
        repo=repo,
        save_args={"feature_view_name": feature_view_name, "write_mode": write_mode},
        load_args={"feature_view_name": feature_view_name},
    )

    # Save appends the three new drivers on top of the seeded one.
    dataset.save(data)

    # Load reads the whole offline store back via a point-in-time join: the
    # seeded driver 0 plus the three appended ones.
    entity_df = pd.DataFrame(
        {
            "driver_id": [0, 1, 2, 3],
            "event_timestamp": pd.to_datetime(["2024-01-03"] * 4, utc=True),
        }
    )
    out = (
        dataset.load()
        .get_historical_features(entity_df)
        .sort_values("driver_id")
        .reset_index(drop=True)
    )
    assert out["driver_id"].tolist() == [0, 1, 2, 3]
    assert out["trips"].tolist() == [100, 10, 20, 30]

    # The online store is populated only when write_mode includes online, and
    # only with the rows written through the dataset (read via the dataset).
    online = (
        dataset.load()
        .get_online_features(pd.DataFrame({"driver_id": [0, 1, 2, 3]}))
        .sort_values("driver_id")
        .reset_index(drop=True)
    )
    trips = [None if pd.isna(v) else int(v) for v in online["trips"]]
    assert trips == expected_online_trips


def test_load_historical_features_timestamp_range(
    store, repo, feature_view_name, data
):
    # Timestamp-range retrieval: no entity_df, just a window. Feast returns the
    # feature rows in the window without an entity join -- the seeded driver 0
    # plus the three drivers appended by save.
    dataset = FeastDataset(
        repo=repo,
        save_args={"feature_view_name": feature_view_name},
        load_args={"feature_view_name": feature_view_name},
    )
    dataset.save(data)

    out = (
        dataset.load()
        .get_historical_features(
            start_date=pd.Timestamp("2024-01-01", tz="UTC").to_pydatetime(),
            end_date=pd.Timestamp("2024-01-04", tz="UTC").to_pydatetime(),
        )
        .sort_values("driver_id")
        .reset_index(drop=True)
    )
    assert out["driver_id"].tolist() == [0, 1, 2, 3]
    assert out["trips"].tolist() == [100, 10, 20, 30]


def test_create_table_on_non_bigquery_source_raises(
    store, repo, feature_view_name, data
):
    # The local feature view is backed by a FileSource, so create_table (which
    # only supports BigQuery) must raise before any write happens.
    dataset = FeastDataset(
        repo=repo,
        save_args={"feature_view_name": feature_view_name, "create_table": True},
    )

    with pytest.raises(DatasetError, match="only supported for BigQuerySource"):
        dataset.save(data)


def test_explicit_credentials_not_supported(repo, feature_view_name):
    # Only Application Default Credentials are supported; passing credentials
    # raises at construction (before any store is built).
    with pytest.raises(NotImplementedError, match="Application Default Credentials"):
        FeastDataset(
            repo=repo,
            credentials={"token": "secret"},
            save_args={"feature_view_name": feature_view_name},
        )


def test_invalid_write_mode_raises(repo, feature_view_name, data):
    # write_mode is validated before any Feast call, so no store is needed.
    dataset = FeastDataset(
        repo=repo,
        save_args={"feature_view_name": feature_view_name, "write_mode": "bogus"},
    )

    with pytest.raises(DatasetError, match="write_mode"):
        dataset.save(data)


def test_create_table_creates_correct_bigquery_table(mocker):
    # Mock a BigQuery-backed feature view (schema/table read by create_table).
    bq_table = "my-project.my_dataset.my_table"
    feature_view = MagicMock()
    feature_view.source = BigQuerySource(table=bq_table, timestamp_field="event_timestamp")
    feature_view.entity_columns = [Field(name="driver_id", dtype=Int64)]
    feature_view.schema = [
        Field(name="driver_id", dtype=Int64),
        Field(name="trips", dtype=Int64),
    ]

    # Mock the feast store (no real registry) and the BigQuery client.
    feast_store = MagicMock()
    feast_store.get_feature_view.return_value = feature_view
    mocker.patch.object(feast_module, "FeatureStore", return_value=feast_store)
    client = mocker.patch.object(feast_module.bigquery, "Client").return_value

    dataset = FeastDataset(
        repo={
            "registry": "gs://bucket/feast",
            "project": "p",
            "provider": "gcp",
            "offline_store": {"type": "bigquery"},
        },
        save_args={"feature_view_name": "driver_stats", "create_table": True},
    )
    dataset.save(
        pd.DataFrame(
            {
                "driver_id": [1],
                "trips": [10],
                "event_timestamp": pd.to_datetime(["2024-01-01"], utc=True),
            }
        )
    )

    # The table created must be the one backing the feature view.
    client.create_table.assert_called_once()
    table = client.create_table.call_args.args[0]
    assert f"{table.project}.{table.dataset_id}.{table.table_id}" == bq_table


@pytest.mark.parametrize(
    "feature_view, timestamp_field, expected",
    [
        # Single entity + single int feature (schema also lists the entity).
        (
            SimpleNamespace(
                entity_columns=[Field(name="driver_id", dtype=Int64)],
                schema=[
                    Field(name="driver_id", dtype=Int64),
                    Field(name="trips", dtype=Int64),
                ],
            ),
            "event_timestamp",
            [
                bigquery.SchemaField("driver_id", "INT64"),
                bigquery.SchemaField("trips", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
            ],
        ),
        # Mixed feast types map to the right BigQuery types.
        (
            SimpleNamespace(
                entity_columns=[Field(name="user_id", dtype=String)],
                schema=[
                    Field(name="score", dtype=Float64),
                    Field(name="active", dtype=Bool),
                ],
            ),
            "ts",
            [
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("score", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("active", "BOOL", mode="NULLABLE"),
                bigquery.SchemaField("ts", "TIMESTAMP"),
            ],
        ),
        # Float32 -> FLOAT64, and the entity is not duplicated from the schema.
        (
            SimpleNamespace(
                entity_columns=[Field(name="driver_id", dtype=Int64)],
                schema=[
                    Field(name="driver_id", dtype=Int64),
                    Field(name="rating", dtype=Float32),
                ],
            ),
            "event_timestamp",
            [
                bigquery.SchemaField("driver_id", "INT64"),
                bigquery.SchemaField("rating", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
            ],
        ),
    ],
)
def test_build_bq_schema(feature_view, timestamp_field, expected):
    assert FeastDataset._build_bq_schema(feature_view, timestamp_field) == expected
