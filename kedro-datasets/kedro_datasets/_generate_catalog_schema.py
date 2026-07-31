"""Generate the versioned Kedro catalog JSON schema by introspection.

Run as a module::

    python -m kedro_datasets._generate_catalog_schema          # write the schema
    python -m kedro_datasets._generate_catalog_schema --check  # verify it is current

The schema is derived from the source-level ``__init__`` signature and docstring
of every public dataset class exported by the ``kedro_datasets`` subpackages.
This module is the source of truth for ``static/jsonschema/<SCHEMA_FILENAME>``;
hand edits to that file are overwritten. Genuinely non-introspectable facts
belong in :mod:`kedro_datasets._schema_overrides`.

Generation reads Python source files instead of importing dataset classes, so it
does not require optional dataset dependencies such as PySpark or Dask.
"""

from __future__ import annotations

import argparse
import ast
import inspect
import json
import os
import pkgutil
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from types import UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints

import kedro_datasets
from kedro_datasets._schema_overrides import EXCLUDED_SUBPACKAGES, SCHEMA_OVERRIDES

# The catalog schema is versioned by Kedro *core* version. Only the latest file
# is auto-generated; older files are frozen. Bump this when Kedro releases a new
# schema-affecting version.
SCHEMA_FILENAME = "kedro-catalog-1.0.0.json"

_NoneType = type(None)

# Exact-type -> JSON type. Matched by identity first, then ``issubclass``.
_TYPE_MAP: dict[type, str] = {
    str: "string",
    bool: "boolean",
    int: "integer",
    float: "number",
    dict: "object",
    list: "array",
    os.PathLike: "string",
}

# Fallback keyed by annotation ``__name__`` for common non-builtin types that
# cannot be matched by identity (e.g. Kedro's ``Version``).
_NAME_TYPE_MAP: dict[str, str] = {
    "str": "string",
    "bool": "boolean",
    "int": "integer",
    "float": "number",
    "dict": "object",
    "list": "array",
    "tuple": "array",
    "Version": "object",
    "PathLike": "string",
    "Path": "string",
    "PurePath": "string",
    "PurePosixPath": "string",
}

# Sentinel for "no JSON type could be determined" -> renders as {"pattern": ".*"}.
_UNKNOWN = object()


@dataclass(frozen=True)
class DatasetParameter:
    """Constructor parameter details needed for schema generation."""

    name: str
    annotation: Any
    required: bool


@dataclass(frozen=True)
class DatasetSpec:
    """Source-level description of a public dataset class."""

    type_id: str
    parameters: list[DatasetParameter]
    docstring: str | None


def _is_union_annotation(annotation: Any) -> bool:
    origin = get_origin(annotation)
    return origin is Union or origin is UnionType


def _split_union_members(annotation: str) -> list[str]:
    """Split a PEP 604 union annotation string on top-level ``|`` tokens."""
    members: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(annotation):
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
        elif char == "|" and depth == 0:
            members.append(annotation[start:index].strip())
            start = index + 1
    members.append(annotation[start:].strip())
    return members


def _json_type_for_annotation_text(annotation: str) -> Any:
    """Map an annotation string from source code to a JSON Schema type."""
    if "|" in annotation:
        json_types: list[str] = []
        nullable = False
        for member in _split_union_members(annotation):
            if member == "None":
                nullable = True
                continue
            mapped = _json_type_for_annotation_text(member)
            if mapped is _UNKNOWN:
                return _UNKNOWN
            json_types.extend(mapped if isinstance(mapped, list) else [mapped])
        ordered = list(dict.fromkeys(json_types))
        if nullable and "null" not in ordered:
            ordered.append("null")
        return ordered[0] if len(ordered) == 1 else ordered

    base_name = annotation.split("[", 1)[0].rsplit(".", 1)[-1]
    return _NAME_TYPE_MAP.get(base_name, _UNKNOWN)


def _json_type_for_union(annotation: Any) -> Any:
    """Map Optional / Union annotations to a JSON Schema ``type`` value."""
    members = get_args(annotation)
    nullable = _NoneType in members
    json_types: list[str] = []

    for member in members:
        if member is _NoneType:
            continue
        mapped = _json_type_for(member)
        if mapped is _UNKNOWN:
            # A union with an unmappable member cannot be typed reliably.
            return _UNKNOWN
        json_types.extend(mapped if isinstance(mapped, list) else [mapped])

    if not json_types:
        return _UNKNOWN

    # De-duplicate preserving order.
    ordered = list(dict.fromkeys(json_types))
    if nullable and "null" not in ordered:
        ordered.append("null")
    return ordered[0] if len(ordered) == 1 else ordered


def _json_type_for_concrete(annotation: Any) -> Any:
    """Map a non-union annotation to a JSON Schema ``type`` value."""
    origin = get_origin(annotation)
    lookup = origin if origin is not None else annotation

    if isinstance(lookup, type):
        mapped = _TYPE_MAP.get(lookup)
        if mapped is not None:
            return mapped
        for base, json_type in _TYPE_MAP.items():
            try:
                if issubclass(lookup, base):
                    return json_type
            except TypeError:
                continue
        by_name = _NAME_TYPE_MAP.get(getattr(lookup, "__name__", ""))
        if by_name is not None:
            return by_name

    # Fall back to the annotation's bare name for unresolved / string annotations.
    name = getattr(annotation, "__name__", None) or str(annotation)
    return _NAME_TYPE_MAP.get(name, _UNKNOWN)


def _json_type_for(annotation: Any) -> Any:
    """Map a resolved Python annotation to a JSON Schema ``type`` value.

    Returns a JSON type string (e.g. ``"string"``), a list of type strings for
    nullable/union annotations (e.g. ``["object", "null"]``), or the ``_UNKNOWN``
    sentinel when no type can be determined.
    """
    if annotation is inspect.Parameter.empty or annotation is None:
        return _UNKNOWN
    if isinstance(annotation, str):
        return _json_type_for_annotation_text(annotation)
    if _is_union_annotation(annotation):
        return _json_type_for_union(annotation)
    return _json_type_for_concrete(annotation)


_ARGS_HEADER_RE = re.compile(r"^(\s*)Args:\s*$")
# A new parameter entry looks like ``name:`` or ``name (type):`` at the block indent.
_PARAM_RE = re.compile(r"^(\s*)(\*{0,2}\w+)\s*(?:\([^)]*\))?\s*:\s?(.*)$")


def _parse_docstring_args(doc: str | None) -> dict[str, str]:
    """Extract per-parameter descriptions from a Google-style ``Args:`` section.

    Continuation lines (indented further than the parameter name) are joined onto
    the parameter's description with a literal ``\\n`` separator, matching the
    formatting of the committed schema.
    """
    if not doc:
        return {}

    lines = doc.splitlines()
    # Locate the ``Args:`` header and the indent it sits at.
    start = None
    header_indent = 0
    for idx, line in enumerate(lines):
        match = _ARGS_HEADER_RE.match(line)
        if match:
            start = idx + 1
            header_indent = len(match.group(1))
            break
    if start is None:
        return {}

    descriptions: dict[str, str] = {}
    current: str | None = None
    param_indent = None
    for line in lines[start:]:
        stripped = line.strip()
        indent = len(line) - len(line.lstrip())
        # A blank line does not terminate the block (descriptions may wrap), but
        # a non-indented line at/above the header indent starts a new section.
        if stripped and indent <= header_indent:
            break
        param_match = _PARAM_RE.match(line)
        if param_match and (param_indent is None or indent == param_indent):
            param_indent = indent
            current = param_match.group(2)
            descriptions[current] = param_match.group(3).strip()
        elif current is not None and stripped:
            descriptions[current] = f"{descriptions[current]}\n{stripped}".strip()
    return {name: desc for name, desc in descriptions.items() if desc}


def _iter_subpackages() -> list[str]:
    """Return the names of importable ``kedro_datasets`` subpackages to introspect."""
    names = []
    for info in pkgutil.iter_modules(kedro_datasets.__path__):
        if not info.ispkg:
            continue
        if info.name.startswith("_") or info.name in EXCLUDED_SUBPACKAGES:
            continue
        names.append(info.name)
    return sorted(names)


def _package_root() -> Path:
    return Path(kedro_datasets.__file__).parent


def _submodule_attrs_for(subpackage: str) -> dict[str, str]:
    """Return ``ClassName -> module_name`` from a subpackage's lazy exports."""
    init_path = _package_root() / subpackage / "__init__.py"
    module = ast.parse(init_path.read_text(encoding="utf-8"))

    for node in ast.walk(module):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg != "submod_attrs" or not isinstance(keyword.value, ast.Dict):
                continue
            class_to_module: dict[str, str] = {}
            for key, value in zip(keyword.value.keys, keyword.value.values):
                if key is None:
                    continue
                module_name = ast.literal_eval(key)
                for class_name in ast.literal_eval(value):
                    class_to_module[class_name] = module_name
            return class_to_module

    raise SystemExit(f"Cannot find lazy exports for kedro_datasets.{subpackage}.")


def _annotation_for(arg: ast.arg) -> Any:
    if arg.annotation is None:
        return inspect.Parameter.empty
    return ast.unparse(arg.annotation)


def _parameters_from_init(init_node: ast.FunctionDef) -> list[DatasetParameter]:
    """Extract constructor parameters from an AST ``__init__`` method."""
    parameters: list[DatasetParameter] = []
    positional_args = [
        arg
        for arg in [*init_node.args.posonlyargs, *init_node.args.args]
        if arg.arg != "self"
    ]
    positional_defaults = [None] * (
        len(positional_args) - len(init_node.args.defaults)
    ) + list(init_node.args.defaults)

    for arg, default in zip(positional_args, positional_defaults):
        parameters.append(
            DatasetParameter(
                name=arg.arg,
                annotation=_annotation_for(arg),
                required=default is None,
            )
        )

    for arg, default in zip(init_node.args.kwonlyargs, init_node.args.kw_defaults):
        parameters.append(
            DatasetParameter(
                name=arg.arg,
                annotation=_annotation_for(arg),
                required=default is None,
            )
        )

    return parameters


def _dataset_spec_from_source(
    subpackage: str, module_name: str, class_name: str
) -> DatasetSpec:
    """Build a dataset spec from the source file that defines ``class_name``."""
    source_path = _package_root() / subpackage / f"{module_name}.py"
    module = ast.parse(source_path.read_text(encoding="utf-8"))

    for node in module.body:
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        for child in node.body:
            if isinstance(child, ast.FunctionDef) and child.name == "__init__":
                return DatasetSpec(
                    type_id=f"{subpackage}.{class_name}",
                    parameters=_parameters_from_init(child),
                    docstring=ast.get_docstring(child),
                )
        return DatasetSpec(
            type_id=f"{subpackage}.{class_name}",
            parameters=[],
            docstring=None,
        )

    raise SystemExit(f"Cannot find {class_name} in {source_path}.")


def _iter_dataset_specs() -> list[DatasetSpec]:
    """Enumerate every public dataset class without importing optional deps."""
    specs: list[DatasetSpec] = []
    for subpackage in _iter_subpackages():
        class_to_module = _submodule_attrs_for(subpackage)
        for class_name in sorted(class_to_module):
            specs.append(
                _dataset_spec_from_source(
                    subpackage, class_to_module[class_name], class_name
                )
            )
    return specs


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` into ``base`` and return ``base``."""
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _property_schema(annotation: Any, description: str | None) -> dict[str, Any]:
    """Build a JSON Schema property fragment for one constructor parameter."""
    json_type = _json_type_for(annotation)
    if json_type is _UNKNOWN:
        schema: dict[str, Any] = {"pattern": ".*"}
    else:
        schema = {"type": json_type}

    if description:
        schema["description"] = description
    return schema


def _resolved_type_hints(init: Any) -> dict[str, Any]:
    """Return resolved annotations for ``init``.

    Dataset modules use ``from __future__ import annotations``. ``inspect`` keeps
    those annotations as strings, so resolve them explicitly. Some exotic
    annotations may still fail to resolve; in that case the generator falls back
    to the raw signature annotations and unknown values render permissively.
    """
    try:
        return get_type_hints(init)
    except (AttributeError, NameError, TypeError):
        return {}


def _dataset_then_schema(spec: DatasetSpec) -> dict[str, Any]:
    """Build the ``then`` schema fragment for a single dataset class."""
    descriptions = _parse_docstring_args(spec.docstring)

    required: list[str] = []
    properties: dict[str, dict[str, Any]] = {}

    for parameter in spec.parameters:
        properties[parameter.name] = _property_schema(
            parameter.annotation, descriptions.get(parameter.name)
        )
        if parameter.required:
            required.append(parameter.name)

    then: dict[str, Any] = {"properties": properties}
    if required:
        then = {"required": required, **then}

    override = SCHEMA_OVERRIDES.get(spec.type_id)
    if override:
        then = _deep_merge(then, override.copy())
    return then


def _dataset_condition(spec: DatasetSpec) -> dict[str, Any]:
    """Build one ``if``/``then`` entry for the catalog item's ``allOf``."""
    return {
        "if": {
            "properties": {
                "type": {
                    "const": spec.type_id,
                }
            }
        },
        "then": _dataset_then_schema(spec),
    }


def build_schema() -> dict[str, Any]:
    """Build the complete Kedro catalog JSON schema."""
    dataset_specs = _iter_dataset_specs()
    dataset_types = [spec.type_id for spec in dataset_specs]

    return {
        "type": "object",
        "patternProperties": {
            "^[a-z0-9-_]+$": {
                "required": [
                    "type",
                ],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": dataset_types,
                    }
                },
                "allOf": [_dataset_condition(spec) for spec in dataset_specs],
            }
        },
    }


def _schema_path() -> Path:
    return (
        Path(kedro_datasets.__file__).parent.parent
        / "static"
        / "jsonschema"
        / SCHEMA_FILENAME
    )


def _format_schema(schema: dict[str, Any]) -> str:
    return f"{json.dumps(schema, indent=2)}\n"


def write_schema() -> None:
    """Write the generated schema to disk."""
    _schema_path().write_text(_format_schema(build_schema()), encoding="utf-8")


def check_schema() -> bool:
    """Return whether the committed schema matches generated output."""
    path = _schema_path()
    if not path.exists():
        return False
    expected = _format_schema(build_schema())
    return path.read_text(encoding="utf-8") == expected


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the committed schema is current without writing it",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.check:
        if check_schema():
            return 0
        print(  # noqa: T201
            f"{_schema_path()} is not up to date. "
            "Run `python -m kedro_datasets._generate_catalog_schema`.",
            file=sys.stderr,
        )
        return 1

    write_schema()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
