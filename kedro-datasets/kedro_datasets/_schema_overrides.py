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

# Dataset type id -> partial ``then`` fragment (deep-merged over generated output).
SCHEMA_OVERRIDES: dict[str, dict[str, Any]] = {}
