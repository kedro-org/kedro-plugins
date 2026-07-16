from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from feast import FeatureService, FeatureStore, FeatureView
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.repo_config import RepoConfig
from google.cloud import bigquery
from kedro.io import AbstractDataset
from kedro.io.core import DatasetError

# Maps feast.types enum names (.name, uppercase) → BigQuery type strings.
# PrimitiveFeastType members (Bool, Float64, …) expose .name; Array falls back
# to the class name.
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

# A fully-qualified BigQuery table reference is ``project.dataset.table``.
_FULLY_QUALIFIED_BQ_TABLE_PARTS = 3


class FeastFeatureSource:
    """Returned by ``FeastDataset.load()``.  Provides historical feature
    retrieval against the Feast offline store for the given features, in either
    an entity-driven (point-in-time join) or timestamp-range mode.
    """

    def __init__(
        self,
        store: FeatureStore,
        features: list[str] | FeatureService
    ) -> None:
        self._store = store
        self._features = features

    def get_historical_features(
        self,
        entity_df: pd.DataFrame | None = None,
        *,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> pd.DataFrame:
        """Retrieve historical features from the offline store.

        Feast supports two mutually exclusive retrieval modes; pass the
        arguments for exactly one of them:

        * **Entity-driven** — pass ``entity_df``: a point-in-time join. The
          DataFrame holds the entity join key column(s) and an event-timestamp
          column that defines the point-in-time context for each row (Feast
          requires the timestamp; it is not fabricated).
        * **Timestamp range** — pass ``start_date``/``end_date`` and *no*
          ``entity_df``: retrieve feature rows within the window without an
          entity join (e.g. for batch scoring). ``end_date`` defaults to the
          current time when omitted.

        Args:
            entity_df: Entity DataFrame for a point-in-time join. Mutually
                exclusive with ``start_date``/``end_date``.
            start_date: Start of the retrieval window (timestamp-range mode).
            end_date: End of the retrieval window (timestamp-range mode);
                defaults to "now" when omitted.

        Raises:
            ValueError: If ``entity_df`` is combined with ``start_date``/
                ``end_date`` (Feast rejects the combination).
        """
        return self._store.get_historical_features(
            entity_df=entity_df,
            features=self._features,
            start_date=start_date,
            end_date=end_date,
        ).to_df()

    def get_online_features(self, entity_df: pd.DataFrame) -> pd.DataFrame:
        """Retrieve the latest feature values from the online store.

        Args:
            entity_df: DataFrame with the entity join key column(s). Unlike the
                offline path, the online store serves the latest values, so no
                event timestamp is needed.
        """
        return self._store.get_online_features(
            features=self._features,
            entity_rows=entity_df.to_dict(orient="records"),
        ).to_df()

class FeastDataset(AbstractDataset):
    """Kedro dataset backed by a Feast feature view or feature service.

    Load returns a ``FeastFeatureSource`` for point-in-time queries against the
    offline store. Save persists a DataFrame to a feature view's batch source
    via Feast's ``write_to_offline_store`` (which matches the DataFrame to the
    feature view's schema and appends) and, when ``write_mode`` is
    ``online_and_offline``, also to the online store. Feast infers the write
    schema from the *existing* table; set ``create_table: true`` to bootstrap a
    (BigQuery) table from the feature view schema when it does not yet exist.

    Example catalog.yml entry::

    .. code-block:: yaml

        feast_features:
          type: kedro_datasets_experimental.feast.FeastDataset
          repo:  # forwarded to feast.RepoConfig
            registry: gs://bucket/feast
            project: project_name
            provider: gcp # default is local
            offline_store:
              type: bigquery # default is bigquery
              location: EU
            online_store:  # optional; required for online_and_offline writes
              type: sqlite
              path: /tmp/online_store.db
          save_args:
            feature_view_name: features
            write_mode: online_and_offline  # or "offline" (default)
            create_table: true  # bootstrap the BigQuery table from the FV schema
          load_args:
            feature_view_name: features
    """

    #: Accepted values for the ``write_mode`` save arg.
    _WRITE_MODES = frozenset({"offline", "online_and_offline"})

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
            repo: Feast repo configuration, forwarded to ``feast.RepoConfig``
                (which validates it). Any ``RepoConfig`` field is accepted, e.g.
                ``registry`` (a path string or dict), ``project``, ``provider``
                and ``offline_store``. ``provider`` defaults to ``"local"`` and
                ``offline_store`` to ``{"type": "bigquery"}`` when not set;
                either can be overridden via ``repo``. ``online_store`` defaults
                to disabled (``None``); set it in ``repo`` to enable
                ``online_and_offline`` writes.
            credentials: Not supported; Feast authenticates via Application
                Default Credentials (the ambient environment). Passing anything
                other than ``None`` raises ``NotImplementedError``.
            load_args: Must contain either ``feature_view_name`` or
                ``feature_service_name`` identifying what ``load`` retrieves.
            save_args: Must contain ``feature_view_name`` — the feature view to
                write to. Optional ``write_mode`` selects where to write:
                ``"offline"`` (default) writes only the offline store;
                ``"online_and_offline"`` also pushes to the online store
                (requires an ``online_store`` in ``repo``). Optional
                ``create_table`` (default ``False``): when ``True``, create the
                backing table from the feature view schema if it does not exist
                (BigQuery sources only; raises a ``DatasetError`` otherwise).
        """
        if credentials is not None:
            raise NotImplementedError("`FeastDataset` supports only Application Default Credentials")

        repo = repo or {}
        repo_config = RepoConfig(
            **{
                "provider": "local",
                "offline_store": {"type": "bigquery"},
                "online_store": None,
                **repo,
            }
        )

        self._repo = repo
        self._feature_store = FeatureStore(config=repo_config)
        self._load_args = load_args or {}
        self._save_args = save_args or {}

    def _describe(self) -> dict[str, Any]:
        return {
            "repo": self._repo,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    def load(self) -> FeastFeatureSource:
        if (feature_view_name := self._load_args.get("feature_view_name")) is not None:
            feature_view = self._feature_store.get_feature_view(feature_view_name)
            features = [
                f"{feature_view.name}:{field.name}" for field in feature_view.features
            ]
            return FeastFeatureSource(self._feature_store, features)

        if (
            feature_service_name := self._load_args.get("feature_service_name")
        ) is not None:
            feature_service = self._feature_store.get_feature_service(
                feature_service_name
            )
            return FeastFeatureSource(self._feature_store, feature_service)

        msg = (
            "load_args must contain either a `feature_view_name` or "
            "`feature_service_name`"
        )
        raise DatasetError(msg)

    def save(self, data: pd.DataFrame) -> None:
        feature_view_name = self._save_args.get("feature_view_name")
        if feature_view_name is None:
            msg = "save_args must contain a `feature_view_name`"
            raise DatasetError(msg)

        write_mode = self._save_args.get("write_mode", "offline")
        if write_mode not in self._WRITE_MODES:
            msg = (
                f"save_args['write_mode'] must be one of "
                f"{sorted(self._WRITE_MODES)}, got {write_mode!r}."
            )
            raise DatasetError(msg)

        # Optionally bootstrap the backing table from the feature view schema.
        # Feast's write infers its schema from the *existing* table, so this
        # must happen first. Only BigQuery-backed sources are supported for now.
        if self._save_args.get("create_table", False):
            self._create_bigquery_table(feature_view_name)

        # Persist the DataFrame into the feature view's batch source: Feast
        # matches/reorders columns to the Feast schema and appends. Fails if
        # columns don't match.
        self._feature_store.write_to_offline_store(
            feature_view_name, data, reorder_columns=True
        )

        # Optionally also push to the online store (requires an online_store to
        # be configured in `repo`; it is disabled by default).
        if write_mode == "online_and_offline":
            self._feature_store.write_to_online_store(feature_view_name, data)

    def _create_bigquery_table(self, feature_view_name: str) -> None:
        """Create the feature view's backing BigQuery table if it doesn't exist.

        The table schema is derived from the feature view (entity keys, features,
        then the event timestamp). Raises a``DatasetError`` for non-BigQuery sources.
        """
        feature_view = self._feature_store.get_feature_view(feature_view_name)
        source = feature_view.source
        batch_source = getattr(source, "batch_source", source)
        if not isinstance(batch_source, BigQuerySource):
            msg = (
                f"save_args['create_table'] is only supported for BigQuerySource "
                f"batch sources, got {type(batch_source).__name__!r} for "
                f"'{feature_view_name}'."
            )
            raise DatasetError(msg)
        if not batch_source.table:
            msg = (
                f"Feature view '{feature_view_name}' uses a query-based source "
                f"with no table to create."
            )
            raise DatasetError(msg)

        timestamp_field = batch_source.timestamp_field
        schema = self._build_bq_schema(feature_view, timestamp_field)

        # Run against the project the source table lives in (a fully-qualified
        # `project.dataset.table` ref), falling back to the offline store's.
        offline = self._feature_store.config.offline_store
        parts = batch_source.table.split(".")
        is_fully_qualified = len(parts) == _FULLY_QUALIFIED_BQ_TABLE_PARTS
        table_project = (
            parts[0]
            if is_fully_qualified
            else (offline.billing_project_id or offline.project_id)
        )
        table_id = (
            batch_source.table
            if is_fully_qualified
            else f"{table_project}.{batch_source.table}"
        )
        table = bigquery.Table(table_id, schema=schema)
        client = bigquery.Client(project=table_project, location=offline.location)
        client.create_table(table, exists_ok=True)

    @staticmethod
    def _build_bq_schema(
        feature_view: FeatureView, timestamp_field: str
    ) -> list[bigquery.SchemaField]:
        """Build the BigQuery schema: entity keys, then features, then timestamp."""

        def bq_type(dtype: Any) -> str:
            key = dtype.name if hasattr(dtype, "name") else type(dtype).__name__
            return _FEAST_FIELD_TYPE_TO_BQ.get(key, "STRING")

        fields: list[bigquery.SchemaField] = []
        entity_names: set[str] = set()
        for field in feature_view.entity_columns:
            fields.append(bigquery.SchemaField(field.name, bq_type(field.dtype)))
            entity_names.add(field.name)

        for field in feature_view.schema:
            if field.name not in entity_names:
                fields.append(
                    bigquery.SchemaField(
                        field.name, bq_type(field.dtype), mode="NULLABLE"
                    )
                )

        fields.append(bigquery.SchemaField(timestamp_field, "TIMESTAMP"))
        return fields
