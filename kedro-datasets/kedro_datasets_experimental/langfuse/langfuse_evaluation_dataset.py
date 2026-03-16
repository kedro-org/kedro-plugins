import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from kedro.io import AbstractDataset, DatasetError

if TYPE_CHECKING:
    from kedro_datasets.json import JSONDataset
    from kedro_datasets.yaml import YAMLDataset
    from langfuse._client.datasets import DatasetClient

from langfuse import Langfuse
from langfuse.api import Error as LangfuseApiError
from langfuse.api import NotFoundError as LangfuseNotFoundError

from kedro_datasets._typing import JSONPreview

logger = logging.getLogger(__name__)

SUPPORTED_FILE_EXTENSIONS = {".json", ".yaml", ".yml"}
REQUIRED_LANGFUSE_CREDENTIALS = {"public_key", "secret_key"}
OPTIONAL_LANGFUSE_CREDENTIALS = {"host"}
VALID_SYNC_POLICIES = {"local", "remote"}


class LangfuseEvaluationDataset(AbstractDataset[list[dict[str, Any]], "DatasetClient"]):
    """Kedro dataset for Langfuse evaluation datasets.

    Connects to a Langfuse evaluation dataset and returns a ``DatasetClient``
    on ``load()``, which can be used to run experiments via
    ``dataset.run_experiment()``. Supports an optional local JSON/YAML file
    as the authoring surface for evaluation items.

    **On load / save behaviour:**

    - **On load:** Creates the remote dataset if it does not exist,
      synchronises based on ``sync_policy``, and returns a ``DatasetClient``.
    - **On save:** Always uploads new items to the remote dataset
      (deduplicating by ``id``). In ``local`` mode, also merges them into
      the local file. In ``remote`` mode, only the remote upload occurs.

    **Item format:**

    The local file and ``save()`` data must be a list of dicts. Each item
    accepts the same keys as
    `Langfuse.create_dataset_item() <https://langfuse.com/docs/evaluation/experiments/datasets#create-items-from-production-data>`_:

    - ``input`` (**required**) — the evaluation input payload.
    - ``id`` — stable identifier used for deduplication on sync and upload.
    - ``expected_output`` — ground-truth value for scoring.
    - ``metadata`` — arbitrary metadata dict attached to the item.
    - ``source_trace_id`` — Langfuse trace ID to link the item to.
    - ``source_observation_id`` — observation ID within the source trace.
    - ``status`` — ``"ACTIVE"`` (default) or ``"ARCHIVED"``.

    ```json
    [
      {
        "id": "q1",
        "input": {"text": "cancel my order"},
        "expected_output": "cancel_order",
        "metadata": {"source": "production"}
      }
    ]
    ```

    Items without an ``id`` cannot be deduplicated and will be re-uploaded
    on every ``load()`` or ``save()`` call.

    **Sync policies:**

    - **local** (default): The local file is the source of truth. On
      ``load()``, any local items missing from the remote (compared by ``id``)
      are uploaded. Existing remote items are never modified or deleted.
      Items without an ``id`` field cannot be deduplicated and will be
      re-uploaded on every load.
    - **remote**: The remote Langfuse dataset is the sole source of truth.
      ``load()`` fetches the remote dataset as-is with no local file
      interaction. ``save()`` uploads new items to remote (deduplicating
      by ``id``) but does not write to any local file. An optional
      ``version`` (ISO 8601 timestamp) can pin ``load()`` to a historical
      snapshot (requires ``langfuse>=3.14.0``).

    Examples:
        Using catalog YAML configuration:

        ```yaml
        # Local sync policy - local file seeds and syncs to remote
        evaluation_dataset:
          type: kedro_datasets_experimental.langfuse.LangfuseEvaluationDataset
          dataset_name: intent-detection-eval
          filepath: data/evaluation/intent_items.json
          sync_policy: local
          credentials: langfuse_credentials
          metadata:
            project: intent-detection

        # Remote sync policy - Langfuse is the source of truth
        production_eval:
          type: kedro_datasets_experimental.langfuse.LangfuseEvaluationDataset
          dataset_name: intent-detection-eval
          sync_policy: remote
          credentials: langfuse_credentials

        # Pinned to a historical snapshot for reproducibility
        eval_snapshot:
          type: kedro_datasets_experimental.langfuse.LangfuseEvaluationDataset
          dataset_name: intent-detection-eval
          sync_policy: remote
          version: "2026-01-15T00:00:00Z"
          credentials: langfuse_credentials
        ```

        Using Python API:

        ```python
        from kedro_datasets_experimental.langfuse import LangfuseEvaluationDataset

        dataset = LangfuseEvaluationDataset(
            dataset_name="intent-detection-eval",
            credentials={
                "public_key": "pk_...",
                "secret_key": "sk_...",  # pragma: allowlist secret
            },
            filepath="data/evaluation/intent_items.json",
        )

        # Load returns a DatasetClient for running experiments
        eval_dataset = dataset.load()
        for item in eval_dataset.items:
            print(item.input, item.expected_output)

        # Save new evaluation items
        dataset.save(
            [
                {"id": "q1", "input": {"text": "cancel order"}, "expected_output": "cancel"},
            ]
        )
        ```
    """

    def __init__(
        self,
        dataset_name: str,
        credentials: dict[str, str],
        filepath: str | None = None,
        sync_policy: Literal["local", "remote"] = "local",
        metadata: dict[str, Any] | None = None,
        version: str | None = None,
    ):
        """Initialise ``LangfuseEvaluationDataset``.

        Args:
            dataset_name: Name of the evaluation dataset in Langfuse.
            credentials: Langfuse authentication credentials.
                Required: ``public_key``, ``secret_key``.
                Optional: ``host`` (defaults to Langfuse cloud).
            filepath: Path to a local JSON/YAML file for authoring evaluation
                items. Supports ``.json``, ``.yaml``, and ``.yml`` extensions.
                When ``None``, no local file interaction occurs.
            sync_policy: Controls the source of truth for reads and whether
                a local file is involved:
                ``"local"`` (default) — local file seeds the remote dataset
                on ``load()``; ``save()`` uploads to remote and merges into
                the local file.
                ``"remote"`` — ``load()`` fetches remote as-is; ``save()``
                uploads to remote without local file interaction.
            metadata: Optional metadata dict passed to Langfuse when creating
                the remote dataset for the first time.
            version: ISO 8601 timestamp to pin ``load()`` to a historical
                snapshot (e.g. ``"2026-01-15T00:00:00Z"``). Only valid with
                ``sync_policy="remote"``. When omitted, the latest dataset
                state is returned. Requires ``langfuse>=3.14.0`` (dataset
                versioning was introduced in the
                `Feb 2026 release <https://langfuse.com/changelog/2026-02-11-versioned-dataset-experiments>`_).

        Raises:
            DatasetError: If credentials are missing or empty, sync_policy is
                invalid, filepath has an unsupported extension, or version
                is used with ``sync_policy="local"``.
        """
        self._validate_init_params(credentials, filepath, sync_policy, version)

        self._dataset_name = dataset_name
        self._dataset: DatasetClient | None = None
        self._filepath = Path(filepath) if filepath else None
        self._sync_policy = sync_policy
        self._metadata = metadata
        self._version = self._parse_version(version)
        self._client = Langfuse(
            public_key=credentials["public_key"],
            secret_key=credentials["secret_key"],
            host=credentials.get("host"),
        )
        self._file_dataset = None

    @staticmethod
    def _validate_init_params(
        credentials: dict[str, str],
        filepath: str | None,
        sync_policy: str,
        version: str | None,
    ) -> None:
        LangfuseEvaluationDataset._validate_credentials(credentials)
        LangfuseEvaluationDataset._validate_sync_policy(sync_policy)
        LangfuseEvaluationDataset._validate_filepath(filepath)
        if version is not None and sync_policy != "remote":
            raise DatasetError(
                "The 'version' parameter can only be used with "
                "sync_policy='remote'. A versioned load returns a historical "
                "snapshot which is incompatible with local-to-remote sync."
            )

    @staticmethod
    def _validate_credentials(credentials: dict[str, str]) -> None:
        for key in REQUIRED_LANGFUSE_CREDENTIALS:
            if key not in credentials:
                raise DatasetError(
                    f"Missing required Langfuse credential: '{key}'."
                )
            if not credentials[key] or not str(credentials[key]).strip():
                raise DatasetError(
                    f"Langfuse credential '{key}' cannot be empty."
                )
        for key in OPTIONAL_LANGFUSE_CREDENTIALS:
            if key in credentials and (
                not credentials[key] or not str(credentials[key]).strip()
            ):
                raise DatasetError(
                    f"Langfuse credential '{key}' cannot be empty if provided."
                )

    @staticmethod
    def _validate_sync_policy(sync_policy: str) -> None:
        if sync_policy not in VALID_SYNC_POLICIES:
            raise DatasetError(
                f"Invalid sync_policy '{sync_policy}'. "
                f"Must be one of: {', '.join(sorted(VALID_SYNC_POLICIES))}."
            )

    @staticmethod
    def _validate_filepath(filepath: str | None) -> None:
        if filepath is None:
            return
        suffix = Path(filepath).suffix.lower()
        if suffix not in SUPPORTED_FILE_EXTENSIONS:
            raise DatasetError(
                f"Unsupported file extension '{suffix}'. "
                f"Supported formats: {', '.join(sorted(SUPPORTED_FILE_EXTENSIONS))}."
            )

    @staticmethod
    def _parse_version(version: str | None) -> datetime | None:
        """Parse an ISO 8601 version string into a timezone-aware UTC datetime."""
        if version is None:
            return None
        try:
            dt = datetime.fromisoformat(version)
        except (ValueError, TypeError) as exc:
            raise DatasetError(
                f"Invalid version '{version}'. "
                f"Expected ISO 8601 format (e.g. '2026-01-15T00:00:00Z')."
            ) from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    @property
    def file_dataset(self) -> "JSONDataset | YAMLDataset":
        """Return JSON/YAML file dataset based on extension."""
        if not self._filepath:
            raise DatasetError("filepath must be provided for file dataset operations.")
        if self._file_dataset is None:
            if self._filepath.suffix.lower() in (".yaml", ".yml"):
                from kedro_datasets.yaml import YAMLDataset  # noqa: PLC0415
                self._file_dataset = YAMLDataset(filepath=str(self._filepath))
            else:
                from kedro_datasets.json import JSONDataset  # noqa: PLC0415
                self._file_dataset = JSONDataset(filepath=str(self._filepath))
        return self._file_dataset

    def _get_or_create_remote_dataset(self) -> "DatasetClient":
        """Ensure the remote Langfuse dataset exists, creating it if not found.

        Returns the latest ``DatasetClient``.
        """
        try:
            return self._client.get_dataset(name=self._dataset_name)
        except LangfuseNotFoundError:
            pass
        except LangfuseApiError as exc:
            raise DatasetError(
                f"Langfuse API error while fetching dataset '{self._dataset_name}': {exc}"
            ) from exc

        try:
            logger.info(
                "Dataset '%s' not found on Langfuse, creating it.",
                self._dataset_name,
            )
            self._client.create_dataset(
                name=self._dataset_name,
                metadata=self._metadata or {},
            )
            return self._client.get_dataset(name=self._dataset_name)
        except LangfuseApiError as exc:
            raise DatasetError(
                f"Langfuse API error while creating dataset '{self._dataset_name}': {exc}"
            ) from exc

    @staticmethod
    def _validate_items(items: list[dict[str, Any]]) -> None:
        """Validate that all items contain the required 'input' key."""
        for i, item in enumerate(items):
            if "input" not in item:
                raise DatasetError(
                    f"Dataset item at index {i} is missing required 'input' key."
                )

    @staticmethod
    def _merge_items(
        existing: list[dict[str, Any]],
        new: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Merge new items into existing list, deduplicating by 'id'.

        Items without an 'id' key are always appended.
        For items with an 'id', existing items take precedence (new duplicates
        are dropped).
        """
        seen_ids: set[str] = set()
        merged: list[dict[str, Any]] = []

        for item in existing:
            item_id = item.get("id")
            if item_id is not None:
                seen_ids.add(item_id)
            merged.append(item)

        for item in new:
            item_id = item.get("id")
            if item_id is not None and item_id in seen_ids:
                continue
            if item_id is not None:
                seen_ids.add(item_id)
            merged.append(item)

        return merged

    def _upload_items(self, items: list[dict[str, Any]]) -> None:
        """Upload items to the remote Langfuse dataset.

        Passes through all keys accepted by ``Langfuse.create_dataset_item()``.
        Callers are responsible for validating items before calling this method.
        """
        for item in items:
            self._client.create_dataset_item(
                dataset_name=self._dataset_name,
                id=item.get("id"),
                input=item["input"],
                expected_output=item.get("expected_output"),
                metadata=item.get("metadata"),
                source_trace_id=item.get("source_trace_id"),
                source_observation_id=item.get("source_observation_id"),
                status=item.get("status"),
            )

    def _filter_new_items(
        self,
        items: list[dict[str, Any]],
        dataset: "DatasetClient",
    ) -> list[dict[str, Any]]:
        """Return items not already present on remote, compared by 'id'.

        Items without an 'id' key are always included (cannot be deduplicated).
        """
        items_with_id = [item for item in items if "id" in item]
        items_without_id = [item for item in items if "id" not in item]

        if items_without_id:
            logger.warning(
                "Found %d item(s) without an 'id' field. "
                "Items without 'id' cannot be deduplicated and will be "
                "uploaded every time. Consider adding unique 'id' fields.",
                len(items_without_id),
            )

        remote_ids = {item.id for item in dataset.items}
        missing_items = [
            item for item in items_with_id
            if item["id"] not in remote_ids
        ]

        return missing_items + items_without_id

    def _sync_local_to_remote(self, dataset: "DatasetClient") -> "DatasetClient":
        """Sync local items to remote, uploading only items missing from remote.

        Compares local items against remote items by 'id'. Items present in the
        local file but absent from the remote dataset are uploaded. Existing
        remote items are never modified or deleted.

        Returns the (possibly refreshed) DatasetClient.
        """
        if not self._filepath or not self._filepath.exists():
            return dataset

        local_items = self.file_dataset.load()
        self._validate_items(local_items)

        new_items = self._filter_new_items(local_items, dataset)
        if not new_items:
            return dataset

        logger.info(
            "Syncing %d new item(s) from '%s' to remote dataset '%s'.",
            len(new_items),
            self._filepath,
            self._dataset_name,
        )
        self._upload_items(new_items)
        return self._client.get_dataset(name=self._dataset_name)

    def load(self) -> "DatasetClient":
        """Load the evaluation dataset from Langfuse.

        Creates the remote dataset if it does not exist. In ``local`` mode,
        items from the local file that are missing on remote (compared by
        ``id``) are uploaded first. In ``remote`` mode with ``version`` set,
        returns items as they existed at that point in time.

        Returns:
            DatasetClient: Langfuse dataset client that can be used to
                iterate items or call ``run_experiment()``.

        Raises:
            DatasetError: If the Langfuse API is unreachable or returns
                an unexpected error.
        """
        dataset = self._get_or_create_remote_dataset()

        if self._version is not None:
            logger.info(
                "Loading versioned snapshot of '%s' at %s.",
                self._dataset_name,
                self._version.isoformat(),
            )
            dataset = self._client.get_dataset(
                name=self._dataset_name, version=self._version
            )

        if self._sync_policy == "local":
            dataset = self._sync_local_to_remote(dataset)

        logger.info(
            "Loaded dataset '%s' with %d item(s) (sync_policy='%s').",
            self._dataset_name,
            len(dataset.items),
            self._sync_policy,
        )
        self._dataset = dataset
        return dataset

    def save(self, data: list[dict[str, Any]]) -> None:
        """Save evaluation items to the remote dataset.

        Uploads items to Langfuse, skipping any that already exist on
        remote (matched by ``id``). In ``local`` mode, items are also
        merged into the local file with the same id-based deduplication.
        In ``remote`` mode, only the remote upload occurs — no local
        file is written.

        Args:
            data: List of evaluation item dicts. Each item must contain
                an ``input`` key. See class docstring for the full list of
                accepted keys (mirrors ``Langfuse.create_dataset_item()``).

        Raises:
            DatasetError: If any item is missing the required ``input`` key
                or the Langfuse API returns an error.
        """
        dataset = self._get_or_create_remote_dataset()
        self._validate_items(data)
        new_items = self._filter_new_items(data, dataset)
        if new_items:
            logger.info(
                "Uploading %d new item(s) to remote dataset '%s'.",
                len(new_items),
                self._dataset_name,
            )
            self._upload_items(new_items)
        else:
            logger.info(
                "No new items to upload to remote dataset '%s'.",
                self._dataset_name,
            )

        if self._sync_policy == "local" and self._filepath:
            existing = []
            if self._filepath.exists():
                existing = self.file_dataset.load()
            merged = self._merge_items(existing, data)
            self.file_dataset.save(merged)

    def _exists(self) -> bool:
        try:
            self._client.get_dataset(name=self._dataset_name)
            return True
        except LangfuseNotFoundError:
            return False
        except LangfuseApiError as exc:
            raise DatasetError(
                f"Langfuse API error while checking dataset '{self._dataset_name}': {exc}"
            ) from exc

    def _describe(self) -> dict[str, Any]:
        return {
            "dataset_name": self._dataset_name,
            "filepath": str(self._filepath) if self._filepath else None,
            "sync_policy": self._sync_policy,
            "version": self._version.isoformat() if self._version else None,
            "metadata": self._metadata,
        }

    def preview(self) -> JSONPreview:
        """Generate a JSON-compatible preview of the local evaluation data.

        Returns:
            JSONPreview: Serialised JSON string for Kedro-Viz. Returns a
                descriptive message if ``filepath`` is not configured or
                the file does not exist.
        """
        if not self._filepath:
            return JSONPreview("No filepath configured.")

        if not self._filepath.exists():
            return JSONPreview("Local file does not exist.")

        local_data = self.file_dataset.load()

        if isinstance(local_data, str):
            local_data = {"content": local_data}

        return JSONPreview(json.dumps(local_data))
