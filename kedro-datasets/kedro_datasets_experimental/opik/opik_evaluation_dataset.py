import json
import logging
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from kedro.io import AbstractDataset, DatasetError
from opik import Opik
from opik.api_objects.dataset.dataset import Dataset
from opik.rest_api.core.api_error import ApiError

from kedro_datasets._typing import JSONPreview

if TYPE_CHECKING:
    from kedro_datasets.json import JSONDataset
    from kedro_datasets.yaml import YAMLDataset

logger = logging.getLogger(__name__)

SUPPORTED_FILE_EXTENSIONS = {".json", ".yaml", ".yml"}
REQUIRED_OPIK_CREDENTIALS = {"api_key"}
OPTIONAL_OPIK_CREDENTIALS = {"workspace", "host", "project_name"}
VALID_SYNC_POLICIES = {"local", "remote"}
HTTP_NOT_FOUND = 404


class OpikEvaluationDataset(AbstractDataset):
    """Kedro dataset for Opik evaluation datasets.

    Connects to an Opik evaluation dataset and returns an ``opik.Dataset``
    on ``load()``, which can be passed to ``opik.evaluation.evaluate()`` to
    run experiments. Supports an optional local JSON/YAML file as the
    authoring surface for evaluation items.

    **On load / save behaviour:**

    - **On load:** Creates the remote dataset if it does not exist,
      synchronises based on ``sync_policy``, and returns an ``opik.Dataset``.
    - **On save:** Inserts all items to the remote dataset. Unchanged items
      are deduplicated by the Opik SDK (content hash); changed items create
      new remote rows. In ``local`` mode, items are also merged into the
      local file (new items take precedence). In ``remote`` mode, only the
      remote insert occurs.

    **Item format:**

    The local file and ``save()`` data must be a list of dicts. Each item
    accepts the following keys:

    - ``input`` (**required**) — the evaluation input payload.
    - ``id`` — stable identifier used for local deduplication. If ``id`` is
      a valid UUID it is forwarded to Opik, giving the remote row a stable
      identity. Human-readable IDs (e.g. ``"intent_001"``) are stripped
      before upload so the API is not fed invalid values — Opik
      auto-generates a UUID in those cases. Items without an ``id`` are
      uploaded without one and Opik assigns a UUID automatically.
    - ``expected_output`` — ground-truth value for scoring.
    - ``metadata`` — arbitrary metadata dict attached to the item.

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

    **Sync policies:**

    - **local** (default): The local file is the source of truth. On
      ``load()``, all local items are re-inserted to remote on every sync.
      The Opik SDK deduplicates by content hash — items whose content has
      not changed since the last sync are ignored. If a local item's content
      changes (even when its ``id`` is kept the same), a new remote row is
      added; the previous version is **not** replaced. ``save()`` inserts to
      remote and merges into the local file (new data takes precedence).
    - **remote**: The remote Opik dataset is the sole source of truth.
      ``load()`` fetches the remote dataset as-is with no local file
      interaction. ``save()`` inserts all items to remote without writing
      to any local file. If the remote dataset does not exist yet, it is
      created empty — **no items are pushed from the local file**. To seed
      a new remote dataset, run with ``sync_policy="local"`` at least once,
      or create and populate the dataset directly via the Opik UI.

    Examples:
        Using catalog YAML configuration:

        ```yaml
        # Local sync policy — local file seeds and syncs to remote
        evaluation_dataset:
          type: kedro_datasets_experimental.opik.OpikEvaluationDataset
          dataset_name: intent-detection-eval
          filepath: data/evaluation/intent_items.json
          sync_policy: local
          credentials: opik_credentials
          metadata:
            project: intent-detection

        # Remote sync policy — Opik is the source of truth
        production_eval:
          type: kedro_datasets_experimental.opik.OpikEvaluationDataset
          dataset_name: intent-detection-eval
          sync_policy: remote
          credentials: opik_credentials
        ```

        Using Python API:

        ```python
        from kedro_datasets_experimental.opik import OpikEvaluationDataset

        dataset = OpikEvaluationDataset(
            dataset_name="intent-detection-eval",
            credentials={"api_key": "..."},  # pragma: allowlist secret
            filepath="data/evaluation/intent_items.json",
        )

        # Load returns an opik.Dataset for running experiments
        from opik.evaluation import evaluate

        eval_dataset = dataset.load()
        evaluate(
            dataset=eval_dataset,
            task=my_task,
            scoring_functions=[my_scorer],
            experiment_name="my-experiment",
        )

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
    ):
        """Initialise ``OpikEvaluationDataset``.

        Args:
            dataset_name: Name of the evaluation dataset in Opik.
            credentials: Opik authentication credentials.
                Required: ``api_key``.
                Optional: ``workspace``, ``host``, ``project_name``.
            filepath: Path to a local JSON/YAML file for authoring evaluation
                items. Supports ``.json``, ``.yaml``, and ``.yml`` extensions.
                When ``None``, no local file interaction occurs.
            sync_policy: Controls the source of truth for reads and whether
                a local file is involved:
                ``"local"`` (default) — all local items are re-inserted to
                remote on ``load()``; ``save()`` inserts to remote and
                merges into the local file (new data takes precedence).
                ``"remote"`` — ``load()`` fetches remote as-is; ``save()``
                inserts to remote without local file interaction.
            metadata: Optional metadata dict stored locally and returned by
                ``_describe()``. Note: Opik's ``create_dataset()`` does not
                accept a metadata argument, so this value is not propagated
                to the remote dataset.
        """
        self._validate_init_params(credentials, filepath, sync_policy)

        self._dataset_name = dataset_name
        self._filepath = Path(filepath) if filepath else None
        self._sync_policy = sync_policy
        self._metadata = metadata
        self._file_dataset = None

        try:
            self._client = Opik(**credentials)
        except Exception as e:
            raise DatasetError(f"Failed to initialise Opik client: {e}") from e

    @staticmethod
    def _validate_init_params(
        credentials: dict[str, str],
        filepath: str | None,
        sync_policy: str,
    ) -> None:
        OpikEvaluationDataset._validate_credentials(credentials)
        OpikEvaluationDataset._validate_sync_policy(sync_policy)
        OpikEvaluationDataset._validate_filepath(filepath)

    @staticmethod
    def _validate_credentials(credentials: dict[str, str]) -> None:
        for key in REQUIRED_OPIK_CREDENTIALS:
            if key not in credentials:
                raise DatasetError(
                    f"Missing required Opik credential: '{key}'."
                )
            if not credentials[key] or not str(credentials[key]).strip():
                raise DatasetError(
                    f"Opik credential '{key}' cannot be empty."
                )
        for key in OPTIONAL_OPIK_CREDENTIALS:
            if key in credentials and (
                not credentials[key] or not str(credentials[key]).strip()
            ):
                raise DatasetError(
                    f"Opik credential '{key}' cannot be empty if provided."
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

    @property
    def file_dataset(self) -> "JSONDataset | YAMLDataset":
        """Return a JSON or YAML file dataset based on the filepath extension."""
        if not self._filepath:
            raise DatasetError("filepath must be provided for file dataset operations.")
        if self._file_dataset is None:
            suffix = self._filepath.suffix.lower()
            if suffix in (".yaml", ".yml"):
                from kedro_datasets.yaml import YAMLDataset  # noqa: PLC0415
                self._file_dataset = YAMLDataset(filepath=str(self._filepath))
            else:
                from kedro_datasets.json import JSONDataset  # noqa: PLC0415
                self._file_dataset = JSONDataset(filepath=str(self._filepath))
        return self._file_dataset

    def _get_or_create_remote_dataset(self) -> Dataset:
        """Ensure the remote Opik dataset exists, creating it if not found.

        Returns the latest ``Dataset`` object.

        Raises:
            DatasetError: If the Opik API returns an unexpected error or is
                unreachable.
        """
        try:
            return self._client.get_dataset(name=self._dataset_name)
        except ApiError as e:
            if e.status_code != HTTP_NOT_FOUND:
                raise DatasetError(
                    f"Opik API error while fetching dataset '{self._dataset_name}': {e}"
                ) from e
        except Exception as e:
            raise DatasetError(
                f"Failed to connect to Opik while fetching dataset "
                f"'{self._dataset_name}': {e}"
            ) from e

        try:
            logger.info(
                "Dataset '%s' not found on Opik, creating it.",
                self._dataset_name,
            )
            return self._client.create_dataset(
                name=self._dataset_name,
                description=f"Created by Kedro (sync_policy={self._sync_policy})",
            )
        except ApiError as e:
            raise DatasetError(
                f"Opik API error while creating dataset '{self._dataset_name}': {e}"
            ) from e
        except Exception as e:
            raise DatasetError(
                f"Failed to connect to Opik while creating dataset "
                f"'{self._dataset_name}': {e}"
            ) from e

    @staticmethod
    def _validate_items(items: list[dict[str, Any]]) -> None:
        """Validate that all items contain the required ``input`` key.

        Raises:
            DatasetError: If any item is missing the ``input`` key.
        """
        for i, item in enumerate(items):
            if "input" not in item:
                raise DatasetError(
                    f"Dataset item at index {i} is missing required 'input' key."
                )

    def _upload_items(self, dataset: Dataset, items: list[dict[str, Any]]) -> None:
        """Insert items into the remote Opik dataset.

        If an item's ``id`` is a valid UUID it is forwarded as-is, giving the
        remote row a stable identity. Human-readable IDs (e.g. ``"intent_001"``)
        are stripped before upload so the API is not fed invalid values — Opik
        auto-generates a UUID in those cases. Items with no ``id`` are uploaded
        without one and Opik assigns a UUID automatically.

        Deduplication is content-hash-based. Unchanged items are no-ops
        regardless of whether an ``id`` is present.

        Callers are responsible for validating items before calling this method.
        """
        items_to_insert = []
        for item in items:
            if "id" not in item:
                items_to_insert.append(item)
            elif not item["id"]:
                items_to_insert.append({k: v for k, v in item.items() if k != "id"})
            else:
                try:
                    uuid.UUID(str(item["id"]))
                    items_to_insert.append(item)
                except ValueError:
                    items_to_insert.append({k: v for k, v in item.items() if k != "id"})
        dataset.insert(items_to_insert)

    def _sync_local_to_remote(self, dataset: Dataset) -> Dataset:
        """Insert all local items into the remote dataset.

        Reads the local file and inserts all items into the remote dataset.
        The Opik SDK deduplicates by content hash, so re-inserting unchanged
        items is a no-op. Returns a refreshed ``Dataset`` object.
        """
        if not self._filepath or not self._filepath.exists():
            return dataset

        local_items = self.file_dataset.load()
        self._validate_items(local_items)

        if not local_items:
            return dataset

        items_without_stable_id = [
            item for item in local_items
            if "id" not in item or not item.get("id")
        ]
        if items_without_stable_id:
            logger.warning(
                "Found %d item(s) with a missing, None, or empty 'id' field in '%s'. "
                "These cannot be tracked across syncs and will create new remote "
                "rows on every load.",
                len(items_without_stable_id),
                self._filepath,
            )

        items_with_non_uuid_id = []
        for item in local_items:
            if item.get("id"):  # present and non-empty/non-None
                try:
                    uuid.UUID(str(item["id"]))
                except ValueError:
                    items_with_non_uuid_id.append(item)
        if items_with_non_uuid_id:
            logger.warning(
                "Found %d item(s) with non-UUID 'id' values in '%s' "
                "(e.g. '%s'). These IDs will be stripped before upload — "
                "Opik will auto-generate UUIDs and remote rows will not "
                "have stable identities.",
                len(items_with_non_uuid_id),
                self._filepath,
                items_with_non_uuid_id[0]["id"],
            )

        logger.info(
            "Syncing %d item(s) from '%s' to remote dataset '%s'.",
            len(local_items),
            self._filepath,
            self._dataset_name,
        )
        self._upload_items(dataset, local_items)
        self._client.flush()

        return self._client.get_dataset(name=self._dataset_name)

    @staticmethod
    def _merge_items(
        existing: list[dict[str, Any]],
        new: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Merge new items into an existing list, deduplicating by ``id``.

        Items without an ``id`` key are always appended. For items with an
        ``id``, new items take precedence — existing entries with the same
        ``id`` are replaced in place.
        """
        new_by_id: dict[str, dict[str, Any]] = {
            item["id"]: item for item in new if "id" in item
        }

        seen_ids: set[str] = set()
        merged: list[dict[str, Any]] = []

        for item in existing:
            item_id = item.get("id")
            if item_id is not None and item_id in new_by_id:
                merged.append(new_by_id[item_id])
                seen_ids.add(item_id)
            else:
                merged.append(item)
                if item_id is not None:
                    seen_ids.add(item_id)

        for item in new:
            item_id = item.get("id")
            if item_id is not None and item_id in seen_ids:
                continue
            if item_id is not None:
                seen_ids.add(item_id)
            merged.append(item)

        return merged

    def load(self) -> Dataset:
        """Load the Opik dataset, syncing local items to remote if sync_policy is ``local``.

        Creates the remote dataset if it does not exist. In ``local`` mode, all
        local items are re-inserted to remote on every load. The Opik SDK
        deduplicates by content hash — unchanged items are no-ops; changed items
        (even with the same local ``id``) create new remote rows.

        Returns:
            Dataset: The Opik dataset ready for use in experiments.

        Raises:
            DatasetError: If the Opik API returns an unexpected error or the
                server is unreachable.
        """
        dataset = self._get_or_create_remote_dataset()

        if self._sync_policy == "local":
            dataset = self._sync_local_to_remote(dataset)

        logger.info(
            "Loaded dataset '%s' (sync_policy='%s').",
            self._dataset_name,
            self._sync_policy,
        )
        return dataset

    def save(self, data: list[dict[str, Any]]) -> None:
        """Insert items into the Opik dataset and optionally update the local file.

        In ``remote`` mode, only the remote upload occurs. In ``local`` mode,
        items are also merged into the local file.

        Args:
            data: List of dicts, each containing at least an ``input`` key.

        Raises:
            DatasetError: If the Opik API call fails or any item is missing ``input``.
        """
        if self._sync_policy == "remote":
            logger.warning(
                "sync_policy='remote': save() uploads to remote only, "
                "local file '%s' will not be updated.",
                self._filepath,
            )

        self._validate_items(data)

        dataset = self._get_or_create_remote_dataset()
        self._upload_items(dataset, data)
        self._client.flush()

        if self._sync_policy == "local" and self._filepath:
            existing: list[dict] = []
            if self._filepath.exists():
                existing = self.file_dataset.load()
            self.file_dataset.save(self._merge_items(existing, data))

    def _exists(self) -> bool:
        try:
            self._client.get_dataset(name=self._dataset_name)
            return True
        except ApiError as e:
            if e.status_code == HTTP_NOT_FOUND:
                return False
            raise DatasetError(
                f"Opik API error while checking dataset '{self._dataset_name}': {e}"
            ) from e

    def _describe(self) -> dict[str, Any]:
        return {
            "dataset_name": self._dataset_name,
            "filepath": str(self._filepath) if self._filepath else None,
            "sync_policy": self._sync_policy,
            "metadata": self._metadata,
        }

    def preview(self) -> JSONPreview:
        """Generate a JSON-compatible preview of the local evaluation data for Kedro-Viz.

        Returns:
            JSONPreview: A Kedro-Viz-compatible object containing a serialized JSON string.
                Returns a descriptive message if filepath is not configured or does not exist.
        """
        if not self._filepath:
            return JSONPreview("No filepath configured.")

        if not self._filepath.exists():
            return JSONPreview("Local evaluation dataset does not exist.")

        local_data = self.file_dataset.load()

        if isinstance(local_data, str):
            local_data = {"content": local_data}

        return JSONPreview(json.dumps(local_data))
