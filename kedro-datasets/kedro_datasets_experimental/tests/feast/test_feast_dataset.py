from unittest.mock import MagicMock

import pandas as pd
import pytest
from feast import Field
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.types import Int64
from google.cloud import bigquery

from kedro_datasets_experimental.feast import feast_dataset as feast_module
from kedro_datasets_experimental.feast.feast_dataset import FeastDataset

BQ_TABLE = "my-project.my_dataset.my_table"


@pytest.fixture
def feast_dataset(mock_feature_store):
    return FeastDataset(
        repo={"path": "gs://bucket/feast", "project": "everycure", "provider": "gcp"},
        load_args={"feature_view_name": "driver_features"},
        save_args={"feature_view_name": "driver_features", "if_exists": "append", "allow_schema_evolution": True},
    )


@pytest.fixture
def data():
    """Entity rows to save (no event_timestamp; it is added on save)."""
    return pd.DataFrame({"driver_id": [1, 2], "trips": [10, 20]})


@pytest.fixture
def feature_view():
    """A minimal table-backed feature view with a single entity + feature."""
    fv = MagicMock()
    fv.name = "driver_features"
    fv.source = BigQuerySource(table=BQ_TABLE, timestamp_field="event_timestamp")
    fv.entity_columns = [Field(name="driver_id", dtype=Int64)]
    fv.schema = [Field(name="driver_id", dtype=Int64), Field(name="trips", dtype=Int64)]
    return fv


@pytest.fixture
def mock_feature_store(mocker, feature_view):
    """Patch ``FeatureStore`` so no real Feast registry is contacted."""
    store = MagicMock()
    store.get_feature_view.return_value = feature_view
    mocker.patch.object(feast_module, "FeatureStore", return_value=store)
    return store


@pytest.fixture
def mock_gbq(mocker):
    """Patch ``GBQTableDataset`` with a mock backed by a mock BigQuery client.

    Returns the mock class so tests can inspect construction kwargs and the
    data handed to ``save``. By default the backing table already exists; a
    test can flip ``mock_gbq.return_value._exists.return_value = False`` to
    exercise the table-creation path.
    """
    gbq_instance = MagicMock()

    # Identifiers used by _update_bq_table_schema to build the bigquery.Table.
    gbq_instance._project_id = "my-project"
    gbq_instance._dataset = "my_dataset"
    gbq_instance._table_name = "my_table"

    # Default: table exists; schema query returns no columns so the update
    # path is a harmless no-op against the mock BigQuery connection.
    gbq_instance._exists.return_value = True
    gbq_instance._connection.get_table.return_value.schema = []

    gbq_class = mocker.patch.object(
        feast_module, "GBQTableDataset", return_value=gbq_instance
    )
    return gbq_class


def test_save_writes_to_existing_bigquery_table(
    feast_dataset, mock_feature_store, mock_gbq, data
):
    feast_dataset.save(data)

    # The GBQTableDataset must be pointed at the table backing the feature view.
    _, kwargs = mock_gbq.call_args
    assert kwargs["project"] == "my-project"
    assert kwargs["dataset"] == "my_dataset"
    assert kwargs["table_name"] == "my_table"

    # The table already exists, so no new table is created.
    gbq_instance = mock_gbq.return_value
    gbq_instance._connection.create_table.assert_not_called()

    # And the data must actually be written to it.
    gbq_instance.save.assert_called_once()
    saved = gbq_instance.save.call_args.args[0]
    assert list(saved["driver_id"]) == [1, 2]
    assert list(saved["trips"]) == [10, 20]

    # An event_timestamp column is added when absent.
    assert "event_timestamp" in saved.columns


def test_save_creates_bigquery_table_when_missing(
    feast_dataset, mock_feature_store, mock_gbq, data
):
    gbq_instance = mock_gbq.return_value
    gbq_instance._exists.return_value = False

    feast_dataset.save(data)

    connection = gbq_instance._connection

    # The table is created with the schema derived from the
    # feature view: entity keys, features, then the event timestamp.
    connection.create_table.assert_called_once()
    connection.get_table.assert_not_called()

    created = connection.create_table.call_args.args[0]
    assert created.project == "my-project"
    assert created.dataset_id == "my_dataset"
    assert created.table_id == "my_table"
    assert [f.name for f in created.schema] == ["driver_id", "trips", "event_timestamp"]
    assert created.time_partitioning.field == "event_timestamp"

    # The data is still written after creating the table.
    gbq_instance.save.assert_called_once()
    saved = gbq_instance.save.call_args.args[0]
    assert list(saved["driver_id"]) == [1, 2]
    assert list(saved["trips"]) == [10, 20]


def test_save_updates_existing_table_schema_with_missing_column(
    feast_dataset, mock_feature_store, mock_gbq, data
):
    gbq_instance = mock_gbq.return_value
    # Existing table has the entity key and timestamp, but is missing the
    # 'trips' feature column present in the feature view schema.
    gbq_instance._connection.get_table.return_value.schema = [
        bigquery.SchemaField("driver_id", "INT64"),
        bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
    ]

    feast_dataset.save(data)

    connection = gbq_instance._connection

    # Table exists, so it is updated in place rather than created.
    connection.create_table.assert_not_called()
    connection.update_table.assert_called_once()

    updated_table, updated_fields = connection.update_table.call_args.args
    assert updated_fields == ["schema"]
    # The missing 'trips' column is appended to the existing schema.
    assert [f.name for f in updated_table.schema] == [
        "driver_id",
        "event_timestamp",
        "trips",
    ]

    # And the data is still written.
    gbq_instance.save.assert_called_once()
    saved = gbq_instance.save.call_args.args[0]
    assert list(saved["driver_id"]) == [1, 2]
    assert list(saved["trips"]) == [10, 20]


def test_load_reads_from_feature_view_table(feast_dataset, mock_feature_store):

    source = feast_dataset.load()

    # The feature view is resolved by name, and the table to load from is the
    # BigQuery source backing that feature view.
    mock_feature_store.get_feature_view.assert_called_once_with("driver_features")
    assert isinstance(source, feast_module.FeastFeatureView)
    assert source._feature_view.source.table == BQ_TABLE

    # A point-in-time query then goes through the store, requesting the feature
    # view's (non-entity) features from that table.
    expected = pd.DataFrame({"driver_id": [1], "trips": [10]})
    mock_feature_store.get_historical_features.return_value.to_df.return_value = expected

    out = source.get_historical_features(pd.DataFrame({"driver_id": [1]}))

    _, kwargs = mock_feature_store.get_historical_features.call_args
    assert kwargs["features"] == ["driver_features:trips"]
    pd.testing.assert_frame_equal(out, expected)
