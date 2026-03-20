"""Security utilities for kedro-datasets."""
from __future__ import annotations

import posixpath

from kedro.io.core import DatasetError


def validate_sub_path(sub_path: str, base_path: str) -> None:
    """Validate that a sub-path does not escape the base directory via path traversal.

    Args:
        sub_path: The relative sub-path to validate (e.g. a partition ID or
            a plot filename supplied by the caller).
        base_path: The base directory path that ``sub_path`` must remain within.

    Raises:
        DatasetError: If ``sub_path`` resolves outside ``base_path``.
    """
    # Normalize only for validation - handle Windows backslashes
    # fsspec uses forward slashes internally, so we normalize to forward slashes
    path_to_check = sub_path.replace("\\", "/").lstrip("/")
    full_path = "/".join([base_path, path_to_check])

    # Normalize the path to resolve any '..' or '.' components for the security check.
    # posixpath is used intentionally here as fsspec normalizes all paths to
    # forward-slash separated strings regardless of OS (including Windows), so
    # this is safe for both local and remote (S3, GCS, etc.) filesystems as long
    # as paths have gone through fsspec's normalization before reaching this point.
    normalized_full_path = posixpath.normpath(full_path)
    normalized_base_path = posixpath.normpath(base_path)

    # Ensure the normalized path is within the base directory
    # Check that normalized path starts with base path followed by separator or is exactly base path
    if not (
        normalized_full_path == normalized_base_path
        or normalized_full_path.startswith(normalized_base_path + "/")
    ):
        raise DatasetError(
            f"Path '{sub_path}' resolves to '{normalized_full_path}' "
            f"which is outside the dataset directory '{base_path}'."
        )
