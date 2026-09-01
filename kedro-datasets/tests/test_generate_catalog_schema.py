from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

import pytest

from kedro_datasets import _generate_catalog_schema as generator


class SampleDataset:
    def __init__(
        self,
        filepath: str,
        *args: Any,
        options: dict[str, Any] | None = None,
        value=None,
        **kwargs: Any,
    ) -> None:
        """Create a sample dataset.

        Args:
            filepath: The file path.
            options: Optional arguments.
                Continued detail.
            value: Untyped value.
        """


SAMPLE_SPEC = generator.DatasetSpec(
    type_id="sample.SampleDataset",
    parameters=[
        generator.DatasetParameter("filepath", "str", True),
        generator.DatasetParameter("options", "dict[str, Any] | None", False),
        generator.DatasetParameter("value", inspect.Parameter.empty, False),
    ],
    docstring=inspect.getdoc(SampleDataset.__init__),
)


def test_json_type_for_known_nullable_and_unknown_annotations():
    assert generator._json_type_for("str") == "string"
    assert generator._json_type_for("pathlib.Path") == "string"
    assert generator._json_type_for("os.PathLike[str]") == "string"
    assert generator._json_type_for("Version") == "object"
    assert generator._json_type_for("dict[str, Any] | None") == ["object", "null"]
    assert generator._json_type_for("str | dict[str, Any]") == ["string", "object"]
    assert generator._json_type_for(str | Any) is generator._UNKNOWN
    assert generator._json_type_for("str | UnknownType") is generator._UNKNOWN
    assert generator._json_type_for(inspect.Parameter.empty) is generator._UNKNOWN
    assert generator._json_type_for(None) is generator._UNKNOWN
    assert generator._json_type_for("str | os.PathLike") == "string"
    assert generator._json_type_for(Path) is generator._UNKNOWN


def test_property_schema_renders_unknown_as_pattern():
    assert generator._property_schema(Any, None) == {"pattern": ".*"}
    assert generator._property_schema("str", "A value.") == {
        "type": "string",
        "description": "A value.",
    }


def test_parse_docstring_args_handles_missing_and_wrapped_descriptions():
    assert generator._parse_docstring_args(None) == {}
    assert generator._parse_docstring_args("No args here.") == {}

    doc = """Create a dataset.

    Args:
        filepath: The file path.
            Continued detail.
        load_args (dict): Load options.

    Raises:
        ValueError: Bad value.
    """

    assert generator._parse_docstring_args(doc) == {
        "filepath": "The file path.\nContinued detail.",
        "load_args": "Load options.",
    }


def test_iter_subpackages_filters_private_and_excluded_packages(tmp_path, monkeypatch):
    package_path = tmp_path / "kedro_datasets"
    package_path.mkdir()
    for name in ["api", "_private", "langchain"]:
        (package_path / name).mkdir()
        (package_path / name / "__init__.py").touch()
    (package_path / "not_a_package.py").touch()

    monkeypatch.setattr(generator.kedro_datasets, "__path__", [str(package_path)])

    assert generator._iter_subpackages() == ["api"]


def test_submodule_attrs_for_reads_lazy_exports(tmp_path, monkeypatch):
    package_path = tmp_path / "kedro_datasets"
    subpackage_path = package_path / "sample"
    subpackage_path.mkdir(parents=True)
    (package_path / "__init__.py").touch()
    (subpackage_path / "__init__.py").write_text(
        "import lazy_loader as lazy\n"
        "lazy.attach(__name__, submodules=[])\n"
        "__getattr__, __dir__, __all__ = lazy.attach(\n"
        "    __name__,\n"
        "    submod_attrs={**{}, 'sample_dataset': ['SampleDataset']},\n"
        ")\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        generator.kedro_datasets, "__file__", str(package_path / "__init__.py")
    )

    assert generator._submodule_attrs_for("sample") == {
        "SampleDataset": "sample_dataset"
    }


def test_submodule_attrs_for_aborts_without_lazy_exports(tmp_path, monkeypatch):
    package_path = tmp_path / "kedro_datasets"
    subpackage_path = package_path / "sample"
    subpackage_path.mkdir(parents=True)
    (package_path / "__init__.py").touch()
    (subpackage_path / "__init__.py").write_text("", encoding="utf-8")
    monkeypatch.setattr(
        generator.kedro_datasets, "__file__", str(package_path / "__init__.py")
    )

    with pytest.raises(SystemExit, match="Cannot find lazy exports"):
        generator._submodule_attrs_for("sample")


def test_dataset_spec_from_source_extracts_init_parameters(tmp_path, monkeypatch):
    package_path = tmp_path / "kedro_datasets"
    subpackage_path = package_path / "sample"
    subpackage_path.mkdir(parents=True)
    (package_path / "__init__.py").touch()
    (subpackage_path / "sample_dataset.py").write_text(
        "from __future__ import annotations\n"
        "from typing import Any\n\n"
        "class SampleDataset:\n"
        "    def __init__(self, filepath: str, *, options: dict[str, Any] | None = None, value=None) -> None:\n"
        "        '''Create a sample.\n\n"
        "        Args:\n"
        "            filepath: The file path.\n"
        "            options: Optional arguments.\n"
        "            value: Untyped value.\n"
        "        '''\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        generator.kedro_datasets, "__file__", str(package_path / "__init__.py")
    )

    assert generator._dataset_spec_from_source(
        "sample", "sample_dataset", "SampleDataset"
    ) == generator.DatasetSpec(
        type_id="sample.SampleDataset",
        parameters=[
            generator.DatasetParameter("filepath", "str", True),
            generator.DatasetParameter("options", "dict[str, Any] | None", False),
            generator.DatasetParameter("value", inspect.Parameter.empty, False),
        ],
        docstring=(
            "Create a sample.\n\n"
            "Args:\n"
            "    filepath: The file path.\n"
            "    options: Optional arguments.\n"
            "    value: Untyped value."
        ),
    )


def test_dataset_spec_from_source_handles_missing_init_with_override(
    tmp_path, monkeypatch
):
    package_path = tmp_path / "kedro_datasets"
    subpackage_path = package_path / "sample"
    subpackage_path.mkdir(parents=True)
    (package_path / "__init__.py").touch()
    (subpackage_path / "sample_dataset.py").write_text(
        "class SampleDataset:\n    pass\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        generator.kedro_datasets, "__file__", str(package_path / "__init__.py")
    )
    monkeypatch.setitem(generator.SCHEMA_OVERRIDES, "sample.SampleDataset", {})

    assert generator._dataset_spec_from_source(
        "sample", "sample_dataset", "SampleDataset"
    ) == generator.DatasetSpec("sample.SampleDataset", [], None)


def test_dataset_spec_from_source_aborts_on_missing_init_without_override(
    tmp_path, monkeypatch
):
    package_path = tmp_path / "kedro_datasets"
    subpackage_path = package_path / "sample"
    subpackage_path.mkdir(parents=True)
    (package_path / "__init__.py").touch()
    (subpackage_path / "sample_dataset.py").write_text(
        "class SampleDataset:\n    pass\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        generator.kedro_datasets, "__file__", str(package_path / "__init__.py")
    )

    with pytest.raises(ValueError, match="defines no __init__ method"):
        generator._dataset_spec_from_source("sample", "sample_dataset", "SampleDataset")


def test_dataset_spec_from_source_aborts_when_class_is_missing(tmp_path, monkeypatch):
    package_path = tmp_path / "kedro_datasets"
    subpackage_path = package_path / "sample"
    subpackage_path.mkdir(parents=True)
    (package_path / "__init__.py").touch()
    (subpackage_path / "sample_dataset.py").write_text("", encoding="utf-8")
    monkeypatch.setattr(
        generator.kedro_datasets, "__file__", str(package_path / "__init__.py")
    )

    with pytest.raises(SystemExit, match="Cannot find SampleDataset"):
        generator._dataset_spec_from_source("sample", "sample_dataset", "SampleDataset")


def test_iter_dataset_specs_uses_lazy_export_order(monkeypatch):
    monkeypatch.setattr(generator, "_iter_subpackages", lambda: ["sample"])
    monkeypatch.setattr(
        generator,
        "_submodule_attrs_for",
        lambda subpackage: {"BDataset": "b_dataset", "ADataset": "a_dataset"},
    )
    monkeypatch.setattr(
        generator,
        "_dataset_spec_from_source",
        lambda subpackage, module_name, class_name: generator.DatasetSpec(
            f"{subpackage}.{class_name}", [], None
        ),
    )

    assert generator._iter_dataset_specs() == [
        generator.DatasetSpec("sample.ADataset", [], None),
        generator.DatasetSpec("sample.BDataset", [], None),
    ]


def test_deep_merge_merges_nested_dicts_and_replaces_values():
    base = {"properties": {"filepath": {"type": "string"}}, "required": ["filepath"]}
    override = {"properties": {"filepath": {"description": "Path."}}, "required": []}

    assert generator._deep_merge(base, override) == {
        "properties": {"filepath": {"type": "string", "description": "Path."}},
        "required": [],
    }


def test_dataset_then_schema_uses_signature_docstring_and_overrides(monkeypatch):
    monkeypatch.setitem(
        generator.SCHEMA_OVERRIDES,
        "sample.SampleDataset",
        {"properties": {"filepath": {"description": "Overridden."}}},
    )

    assert generator._dataset_then_schema(SAMPLE_SPEC) == {
        "required": ["filepath"],
        "properties": {
            "filepath": {"type": "string", "description": "Overridden."},
            "options": {
                "type": ["object", "null"],
                "description": "Optional arguments.\nContinued detail.",
            },
            "value": {"pattern": ".*", "description": "Untyped value."},
        },
    }


def test_build_schema_uses_dataset_classes(monkeypatch):
    monkeypatch.setattr(
        generator,
        "_iter_dataset_specs",
        lambda: [SAMPLE_SPEC],
    )

    assert generator.build_schema() == {
        "type": "object",
        "patternProperties": {
            "^[a-z0-9-_]+$": {
                "required": ["type"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["sample.SampleDataset"],
                    }
                },
                "allOf": [
                    {
                        "if": {
                            "properties": {"type": {"const": "sample.SampleDataset"}}
                        },
                        "then": generator._dataset_then_schema(SAMPLE_SPEC),
                    }
                ],
            }
        },
    }


def test_schema_io_paths(monkeypatch, tmp_path):
    schema_path = tmp_path / generator.SCHEMA_FILENAME
    schema = {"type": "object"}
    monkeypatch.setattr(generator, "_schema_path", lambda: schema_path)
    monkeypatch.setattr(generator, "build_schema", lambda: schema)

    assert generator.check_schema() is False
    generator.write_schema()
    assert schema_path.read_text(encoding="utf-8") == '{\n  "type": "object"\n}\n'
    assert generator.check_schema() is True


def test_committed_schema_is_current():
    assert generator.check_schema()


def test_schema_path_points_to_static_schema_file():
    assert generator._schema_path().name == generator.SCHEMA_FILENAME
    assert generator._schema_path().parent.name == "jsonschema"


def test_main_writes_or_checks_schema(monkeypatch, tmp_path, capsys):
    schema_path = tmp_path / generator.SCHEMA_FILENAME
    calls = []

    monkeypatch.setattr(generator, "_schema_path", lambda: schema_path)
    monkeypatch.setattr(generator, "write_schema", lambda: calls.append("write"))
    monkeypatch.setattr(generator, "check_schema", lambda: True)

    assert generator.main([]) == 0
    assert calls == ["write"]
    assert generator.main(["--check"]) == 0

    monkeypatch.setattr(generator, "check_schema", lambda: False)

    assert generator.main(["--check"]) == 1
    assert "is not up to date" in capsys.readouterr().err
