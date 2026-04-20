"""Shared helpers for Langfuse datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union

from kedro.io import DatasetError

from kedro_datasets._typing import JSONPreview

if TYPE_CHECKING:
    from kedro_datasets.json import JSONDataset
    from kedro_datasets.yaml import YAMLDataset

REQUIRED_LANGFUSE_CREDENTIALS = {"public_key", "secret_key"}
OPTIONAL_LANGFUSE_CREDENTIALS = {"host"}
SUPPORTED_FILE_EXTENSIONS = {".json", ".yaml", ".yml"}


def validate_langfuse_credentials(credentials: dict[str, Any]) -> None:
    """Validate Langfuse credentials.

    Args:
        credentials: Credentials dictionary to validate.

    Raises:
        DatasetError: If required credentials are missing or empty.
    """
    for key in REQUIRED_LANGFUSE_CREDENTIALS:
        if key not in credentials:
            raise DatasetError(f"Missing required Langfuse credential: '{key}'")
        if not credentials[key] or not str(credentials[key]).strip():
            raise DatasetError(f"Langfuse credential '{key}' cannot be empty")

    for key in OPTIONAL_LANGFUSE_CREDENTIALS:
        if key in credentials:
            if not credentials[key] or not str(credentials[key]).strip():
                raise DatasetError(
                    f"Langfuse credential '{key}' cannot be empty if provided"
                )


def validate_sync_policy(sync_policy: str, valid_policies: set[str]) -> None:
    """Validate sync policy value.

    Args:
        sync_policy: Sync policy to validate.
        valid_policies: Set of allowed sync policy values.

    Raises:
        DatasetError: If sync policy is not in the valid set.
    """
    if sync_policy not in valid_policies:
        raise DatasetError(
            f"Invalid sync_policy '{sync_policy}'. "
            f"Must be one of: {', '.join(sorted(valid_policies))}"
        )


def validate_file_extension(filepath: str | Path) -> None:
    """Validate that *filepath* has a supported extension.

    Args:
        filepath: Path to validate.

    Raises:
        DatasetError: If the extension is not in ``SUPPORTED_FILE_EXTENSIONS``.
    """
    suffix = Path(filepath).suffix.lower()
    if suffix not in SUPPORTED_FILE_EXTENSIONS:
        raise DatasetError(
            f"Unsupported file extension '{suffix}'. "
            f"Supported formats: {', '.join(sorted(SUPPORTED_FILE_EXTENSIONS))}"
        )


def create_file_dataset(filepath: str | Path) -> Union["JSONDataset", "YAMLDataset"]:
    """Create the appropriate Kedro file dataset for *filepath*.

    Args:
        filepath: Path whose extension determines the dataset type.

    Returns:
        ``YAMLDataset`` for ``.yaml``/``.yml``, ``JSONDataset`` otherwise.
    """
    if Path(filepath).suffix.lower() in (".yaml", ".yml"):
        from kedro_datasets.yaml import YAMLDataset  # noqa: PLC0415

        return YAMLDataset(filepath=str(filepath))

    from kedro_datasets.json import JSONDataset  # noqa: PLC0415

    return JSONDataset(filepath=str(filepath))


def build_preview(
    filepath: Path | None,
    file_dataset: Union["JSONDataset", "YAMLDataset", None] = None,
) -> JSONPreview:
    """Build a JSON-compatible preview of local file data for Kedro-Viz.

    Args:
        filepath: Path to the local file, or ``None`` if not configured.
        file_dataset: Kedro file dataset used to load the data.  Only
            accessed when *filepath* exists.

    Returns:
        Serialised JSON string wrapped in ``JSONPreview``.  Returns a
        descriptive message when *filepath* is not configured or the
        file does not exist.
    """
    if not filepath:
        return JSONPreview("No filepath configured.")

    if not filepath.exists():
        return JSONPreview("Local file does not exist.")

    local_data = file_dataset.load()

    if isinstance(local_data, str):
        local_data = {"content": local_data}

    return JSONPreview(json.dumps(local_data))
