"""Hand-maintained overrides for the auto-generated Kedro catalog JSON schema.

The schema is produced by :mod:`kedro_datasets._generate_catalog_schema` from
dataset ``__init__`` signatures. Source introspection cannot recover genuinely
non-introspectable facts (for example, the closed set of allowed values for a
string parameter). Such facts live here and are deep-merged into the generated
``then`` block for the matching dataset type id.

Keep this file as small as possible: prefer improving the generator over adding
an override. An entry maps a dataset type id (``"<subpackage>.<ClassName>"``) to
a partial ``then`` fragment that is deep-merged on top of the generated one.
"""

from __future__ import annotations

from typing import Any

# Subpackages under ``kedro_datasets`` that are intentionally excluded from the
# catalog schema (e.g. langchain datasets are not configured through the
# catalog in the usual way).
EXCLUDED_SUBPACKAGES: set[str] = {"langchain"}

_HF_FS_DATASET_OVERRIDE: dict[str, Any] = {
    "required": ["path"],
    "properties": {
        "path": {
            "type": "string",
            "description": "Path to a file or directory for persisting Hugging Face datasets. Supports local paths, ``os.PathLike`` objects, and remote URIs (e.g. ``s3://bucket/data``).",
        },
        "version": {
            "type": ["object", "null"],
            "description": "Optional versioning configuration (see :class:`~kedro.io.core.Version`).",
        },
        "data_files": {
            "type": ["object", "null"],
            "description": "Mapping of split name to filename for loading and saving a ``DatasetDict`` from a directory (e.g. ``{\"train\": \"train.csv\"}``). The keys must match the split names of the ``DatasetDict`` being saved, and the filenames must use the correct extension for the format (e.g. ``.csv`` for ``CSVDataset``).",
        },
        "load_args": {
            "type": ["object", "null"],
            "description": "Additional keyword arguments passed to the underlying load function. This cannot include ``data_files``; use the top-level ``data_files`` argument instead.",
        },
        "save_args": {
            "type": ["object", "null"],
            "description": "Additional keyword arguments passed to the underlying save function. This cannot include ``data_files``; use the top-level ``data_files`` argument instead.",
        },
        "credentials": {
            "type": ["object", "null"],
            "description": "Credentials for the underlying filesystem (e.g. ``key``/``secret`` for S3). Passed to the ``storage_options`` parameter in the underlying ``datasets`` implementation.",
        },
        "fs_args": {
            "type": ["object", "null"],
            "description": "Extra arguments passed to the ``fsspec`` filesystem initialiser. Passed to the ``storage_options`` parameter in the underlying ``datasets`` implementation.",
        },
        "metadata": {
            "type": ["object", "null"],
            "description": "Any arbitrary metadata. This is ignored by Kedro but may be consumed by users or external plugins.",
        },
    },
}

# Dataset type id -> partial ``then`` fragment (deep-merged over generated output).
SCHEMA_OVERRIDES: dict[str, dict[str, Any]] = {
    "huggingface.ArrowDataset": _HF_FS_DATASET_OVERRIDE,
    "huggingface.CSVDataset": _HF_FS_DATASET_OVERRIDE,
    "huggingface.JSONDataset": _HF_FS_DATASET_OVERRIDE,
    "huggingface.ParquetDataset": _HF_FS_DATASET_OVERRIDE,
}
