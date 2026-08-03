"""Filesystem path utilities shared across datasets."""
from __future__ import annotations

import os


def split_filepath(filepath: str | os.PathLike) -> tuple[str, str]:
    """Split a filepath into its protocol prefix (e.g. ``s3://``) and the rest.

    Unlike ``pathlib``-based parsing, this preserves the ``//`` after the
    protocol, so cloud/remote URIs (``s3://``, ``abfss://``, ``hf://``, ...)
    survive a round trip when the prefix is concatenated back onto the path.

    Args:
        filepath: Raw filepath, e.g. ``s3://bucket/data.parquet`` or a local path.

    Returns:
        A ``(prefix, path)`` tuple. ``prefix`` is ``""`` for local paths;
        otherwise it ends with ``://`` (e.g. ``s3://``).
    """
    split_ = str(filepath).split("://", 1)
    if len(split_) == 2:  # noqa: PLR2004
        return split_[0] + "://", split_[1]
    return "", split_[0]
