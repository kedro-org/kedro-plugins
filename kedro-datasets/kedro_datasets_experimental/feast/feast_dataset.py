from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import timezone
from typing import Any

import pandas as pd
from feast import FeatureStore, FeatureView, FeatureService
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.repo_config import RepoConfig, RegistryConfig
from google.cloud import bigquery
from kedro.io import AbstractDataset
from kedro.io.core import DatasetError
from kedro_datasets.pandas import GBQTableDataset

# Maps feast.types enum names (.name attribute, uppercase) → BigQuery type strings.
# PrimitiveFeastType members (Bool, Float64, …) expose .name; Array uses class name fallback.
_FEAST_FIELD_TYPE_TO_BQ: dict[str, str] = {
    "BOOL": "BOOL",
    "FLOAT32": "FLOAT64",
    "FLOAT64": "FLOAT64",
    "INT32": "INT64",
    "INT64": "INT64",
    "STRING": "STRING",
    "BYTES": "BYTES",
    "JSON": "JSON",
    "UNIX_TIMESTAMP": "TIMESTAMP",
    "Array": "JSON",
}

TIMESTAMP_FIELD_NAME = "event_timestamp"

# Keys consumed by FeastDataset itself; must be stripped before forwarding
# load_args / save_args to the underlying GBQTableDataset (pd_gbq.to_gbq
# rejects unknown kwargs).
_FEAST_ONLY_ARG_KEYS = frozenset(
    {"feature_view_name", "feature_service_name", "allow_schema_evolution"}
)


def _strip_feast_keys(args: dict[str, Any] | None) -> dict[str, Any] | None:
    if not args:
        return args
    return {k: v for k, v in args.items() if k not in _FEAST_ONLY_ARG_KEYS}


def _feast_field_dtype_to_bq(dtype: Any) -> str:
    # PrimitiveFeastType enum members have .name (e.g. "BOOL", "FLOAT64").
    # Complex types like Array use the class name as fallback.
    key = dtype.name if hasattr(dtype, "name") else type(dtype).__name__
    return _FEAST_FIELD_TYPE_TO_BQ.get(key, "STRING")

def _parse_bq_table(table_ref: str) -> tuple[str, str, str]:
    parts = table_ref.split(".")
    if len(parts) != 3: # noqa: PLR2004
        raise DatasetError(
            f"Expected BigQuery table reference 'project.dataset.table', got: {table_ref!r}"
        )
    return parts[0], parts[1], parts[2]


class FeastFeatureSource(ABC):
    @abstractmethod
    def get_historical_features(
        self, entity_df: pd.DataFrame, *, timestamp: pd.Timestamp | None = None
    ) -> pd.DataFrame:
        pass


class FeastFeatureView(FeastFeatureSource):
    """Returned by ``FeastDataset.load()``.  Provides point-in-time feature retrieval
    against the Feast offline (BigQuery) store for the configured feature view.
    """

    def __init__(
        self,
        store: FeatureStore,
        feature_view: Any,
    ) -> None:
        self._store = store
        self._feature_view = feature_view

    def get_historical_features(
        self,
        entity_df: pd.DataFrame,
        *,
        timestamp: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """Retrieve features via a point-in-time join against the offline store.

        Args:
            entity_df: DataFrame containing entity join key column(s).  If it
                already has an ``event_timestamp`` column that column is used
                as-is; otherwise ``timestamp`` is broadcast across all rows
                (defaults to the current UTC time).
            timestamp: Optional fixed timestamp to assign when ``event_timestamp``
                is absent from ``entity_df``.
        """
        entity_col_names = {f.name for f in self._feature_view.entity_columns}

        # Feast's BQ query will be ambiguous if the entity_df carries extra columns
        # that also exist in the feature source table — keep only join keys + timestamp.
        keep = entity_col_names | {TIMESTAMP_FIELD_NAME}
        entity_df = entity_df[[c for c in entity_df.columns if c in keep]].copy()

        if TIMESTAMP_FIELD_NAME not in entity_df.columns:
            ts = (
                timestamp
                if timestamp is not None
                else pd.Timestamp.now(tz=timezone.utc)
            )
            if ts.tzinfo is None:
                ts = ts.tz_localize(timezone.utc)
            entity_df[TIMESTAMP_FIELD_NAME] = ts

        feature_refs = [
            f"{self._feature_view.name}:{field.name}"
            for field in self._feature_view.schema
            if field.name not in entity_col_names
        ]
        return self._store.get_historical_features(
            entity_df=entity_df,
            features=feature_refs,
        ).to_df()


class FeastFeatureService(FeastFeatureSource):
    def __init__(self, store: FeatureStore, feature_service: FeatureService) -> None:
        self._store = store
        self._feature_service = feature_service

    def get_historical_features(
        self, entity_df: pd.DataFrame, *, timestamp: pd.Timestamp | None = None
    ) -> pd.DataFrame:

        if TIMESTAMP_FIELD_NAME not in entity_df.columns:
            ts = (
                timestamp
                if timestamp is not None
                else pd.Timestamp.now(tz=timezone.utc)
            )
            entity_df[TIMESTAMP_FIELD_NAME] = ts

        return self._store.get_historical_features(
            entity_df=entity_df,
            features=self._feature_service,
        ).to_df()


class FeastDataset(AbstractDataset):
    """Kedro dataset backed by a Feast FeatureView.

    For table-backed feature views (``BigQuerySource(table=...)``): supports both
    load and save.  Load returns a ``FeastFeatureView`` for point-in-time queries;
    save writes directly to the backing BigQuery table.

    For query-backed feature views (``BigQuerySource(query=...)``): load-only.
    Attempting to save raises a ``DatasetError``.

    Example catalog.yml entry::

    .. code-block:: yaml

        feast_features:
          type: kedro_utils.datasets.FeastDataset
          repo:
            path: gs://bucket/feast
            project: project_name
            provider: gcp
          save_args:
            feature_view_name: features_online_push
            if_exists: append
            allow_schema_evolution: true
          load_args:
            feature_view_name: features_online_push

    ``save_args`` accepts, in addition to the underlying ``GBQTableDataset``
    options:

    * ``feature_view_name`` (required): the feature view whose backing
      BigQuery table is written to.
    * ``allow_schema_evolution`` (optional, default ``True``): when ``True``,
      the backing table is created if missing and any columns present in the
      Feast schema but absent in BigQuery are added before writing. Set to
      ``False`` to write against the existing table schema unchanged.
    """

    def __init__(
        self,
        *,
        repo: dict[str, Any],
        credentials: dict[str, Any] | str | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Create a new ``FeastDataset``.

        Args:
            repo: Specification of the Feast feature repo. Supports ``path``
                (the registry path), ``project`` and ``provider``. Only the
                ``gcp`` provider is supported for now; any other provider or
                repo specification raises ``NotImplementedError``.
        """
        repo = repo or {}
        path = repo.get("path")
        project = repo.get("project")
        provider = repo.get("provider")

        if path is None or project is None or provider is None or provider != "gcp":
            raise NotImplementedError(
                "FeastDataset only supports specifying a Feast repo via "
                "'path', 'project' and 'provider' for now, e.g. "
                "repo: {path: gs://bucket/feast, project: project, provider: gcp}."
            )

        repo_config = RepoConfig(
            registry=RegistryConfig(path=path),
            project=project,
            provider=provider,
            offline_store={"type": "bigquery"},
            online_store="null",  # null as Kedro should not write directly to the online store
        )
        self._repo = repo
        self._feature_store = FeatureStore(config=repo_config)
        self._credentials = credentials
        self._load_args = load_args
        self._save_args = save_args

    def _describe(self) -> dict[str, Any]:
        desc: dict[str, Any] = {
            "repo": self._repo,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

        return desc

    def load(self) -> FeastFeatureView:
        if self._load_args is None:
            raise DatasetError("load function called but load_args is empty")

        elif (
            feature_view_name := self._load_args.get("feature_view_name")
        ) is not None:
            feature_view = self._feature_store.get_feature_view(feature_view_name)
            _ = self._get_feature_view_batch_source(feature_view)
            return FeastFeatureView(self._feature_store, feature_view)

        elif (
            feature_service_name := self._load_args.get("feature_service_name")
        ) is not None:
            feature_service = self._feature_store.get_feature_service(
                feature_service_name
            )
            _ = self._get_feature_service_batch_sources(feature_service)
            return FeastFeatureService(self._feature_store, feature_service)

        else:
            raise DatasetError(
                "load_args must contain either a feature_view_name or feature_service_name"
            )

    def save(self, data: pd.DataFrame) -> None:
        if self._save_args is None:
            raise DatasetError("save function called but save_args is empty")
        elif "feature_view_name" not in self._save_args:
            raise DatasetError("save_args must contain a feature_view_name")

        # Nothing to append (e.g. idempotency filter skipped every row).
        if data.empty:
            return

        # 1. Get BQ table from feature view
        feature_view_name = self._save_args["feature_view_name"]
        feature_view = self._feature_store.get_feature_view(feature_view_name)

        # PushSource wraps a batch_source; plain BigQuerySource is its own batch_source.
        batch_source = self._get_feature_view_batch_source(feature_view)
        timestamp_field = batch_source.timestamp_field or TIMESTAMP_FIELD_NAME

        # Query-based sources have no backing table and are read-only.
        if not batch_source.table:
            raise DatasetError(
                f"Feature view '{feature_view_name}' uses a query-based source "
                f"and is read-only. Add a 'table' to its BigQuerySource to enable writes."
            )

        project, bq_dataset, table_name = _parse_bq_table(batch_source.table)
        bq_table = GBQTableDataset(
            dataset=bq_dataset,
            table_name=table_name,
            project=project,
            credentials=self._credentials,
            load_args=_strip_feast_keys(self._load_args),
            save_args=_strip_feast_keys(self._save_args),
        )

        # 2. Prepare data for saving
        data = data.copy()
        if timestamp_field not in data.columns:
            data[timestamp_field] = pd.Timestamp.now(tz=timezone.utc)

        bq_schema = self._build_bq_schema(feature_view, timestamp_field)
        known_cols = {f.name for f in bq_schema}
        data = data[[c for c in data.columns if c in known_cols]]

        # 3. Update BQ table schema if necessary (opt-out via save_args).
        if self._save_args.get("allow_schema_evolution", True):
            self._update_bq_table_schema(bq_table, bq_schema, timestamp_field)

        # 4. Save data to BQ table
        bq_table.save(data)

    @staticmethod
    def _get_feature_view_batch_source(feature_view: FeatureView) -> BigQuerySource:
        source = feature_view.source
        batch_source = getattr(source, "batch_source", source)
        if not isinstance(batch_source, BigQuerySource):
            raise DatasetError(
                f"FeastDataset only supports BigQuerySource batch sources, "
                f"got {type(batch_source).__name__!r} for feature view {feature_view.name!r}."
            )
        return batch_source

    @staticmethod
    def _get_feature_service_batch_sources(feature_service: FeatureService):
        batch_sources = [
            getattr(source, "batch_source", None)
            for source in feature_service.feature_view_projections
            if getattr(source, "batch_source", None) is not None
        ]

        for batch_source in batch_sources:
            if not isinstance(batch_source, BigQuerySource):
                raise DatasetError(
                    f"FeastDataset only supports BigQuerySource batch sources, "
                    f"got {type(batch_source).__name__!r} for feature service {feature_service.name!r}."
                )

        return batch_sources

    @staticmethod
    def _build_bq_schema(
        feature_view: FeatureView, timestamp_field: str
    ) -> list[bigquery.SchemaField]:
        """Build BigQuery schema: entity join keys first, then feature fields, then timestamp."""
        fields: list[bigquery.SchemaField] = []

        # entity_columns are the join-key fields (Field objects, same dtype system as schema)
        entity_col_names: set[str] = set()
        for field in feature_view.entity_columns:
            bq_type = _feast_field_dtype_to_bq(field.dtype)
            fields.append(bigquery.SchemaField(field.name, bq_type))
            entity_col_names.add(field.name)

        for field in feature_view.schema:
            if field.name in entity_col_names:
                continue
            bq_type = _feast_field_dtype_to_bq(field.dtype)
            fields.append(bigquery.SchemaField(field.name, bq_type, mode="NULLABLE"))

        fields.append(bigquery.SchemaField(timestamp_field, "TIMESTAMP"))

        return fields

    @staticmethod
    def _update_bq_table_schema(
        bq_table: GBQTableDataset,
        bq_schema: list[bigquery.SchemaField],
        timestamp_field: str,
    ) -> None:
        """Create the table if absent, or add any columns present in Feast but missing in BQ."""
        desired = {f.name: f for f in bq_schema}

        if not bq_table._exists():
            full_table_id = (
                f"{bq_table._project_id}.{bq_table._dataset}.{bq_table._table_name}"
            )
            table = bigquery.Table(full_table_id, schema=list(desired.values()))
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=timestamp_field,
            )
            bq_table._connection.create_table(table)
            return

        table_ref = bq_table._connection.dataset(bq_table._dataset).table(
            bq_table._table_name
        )
        table = bq_table._connection.get_table(table_ref)
        existing = {f.name for f in table.schema}
        new_fields = [f for name, f in desired.items() if name not in existing]
        if new_fields:
            table.schema = table.schema + new_fields
            bq_table._connection.update_table(table, ["schema"])
