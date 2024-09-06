"""Testing module for CLI tools"""

import shutil
from collections import namedtuple
from pathlib import Path

import pytest
from kedro import __version__ as kedro_version
from kedro.framework.cli.cli import KedroCLI, cli
from kedro.framework.startup import ProjectMetadata

from kedro_telemetry.masking import (
    MASK,
    _get_cli_structure,
    _mask_kedro_cli,
)

REPO_NAME = "cli_tools_dummy_project"
PACKAGE_NAME = "cli_tools_dummy_package"
DEFAULT_KEDRO_COMMANDS = [
    "catalog",
    "info",
    "ipython",
    "jupyter",
    "micropkg",
    "new",
    "package",
    "pipeline",
    "registry",
    "run",
    "starter",
    "--version",
    "-V",
    "--help",
]


@pytest.fixture
def fake_root_dir(tmp_path):
    try:
        yield Path(tmp_path).resolve()
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.fixture
def fake_metadata(fake_root_dir):
    metadata = ProjectMetadata(
        config_file=fake_root_dir / REPO_NAME / "pyproject.toml",
        package_name=PACKAGE_NAME,
        project_name="CLI Tools Testing Project",
        project_path=fake_root_dir / REPO_NAME,
        source_dir=fake_root_dir / REPO_NAME / "src",
        kedro_init_version=kedro_version,
        tools=[],
        example_pipeline="No",
    )
    return metadata


class TestCLIMasking:
    def test_get_cli_structure_raw(self, mocker, fake_metadata):
        Module = namedtuple("Module", ["cli"])
        mocker.patch("kedro.framework.cli.cli._is_project", return_value=True)
        mocker.patch(
            "kedro.framework.cli.cli.bootstrap_project", return_value=fake_metadata
        )
        mocker.patch(
            "kedro.framework.cli.cli.importlib.import_module",
            return_value=Module(cli=cli),
        )
        kedro_cli = KedroCLI(fake_metadata.project_path)
        raw_cli_structure = _get_cli_structure(kedro_cli, get_help=False)

        # raw CLI structure tests
        assert isinstance(raw_cli_structure, dict)
        assert isinstance(raw_cli_structure["kedro"], dict)

        for k, v in raw_cli_structure["kedro"].items():
            assert isinstance(k, str)

        assert sorted(list(raw_cli_structure["kedro"])) == sorted(
            DEFAULT_KEDRO_COMMANDS
        )

    def test_get_cli_structure_depth(self, mocker, fake_metadata):
        Module = namedtuple("Module", ["cli"])
        mocker.patch("kedro.framework.cli.cli._is_project", return_value=True)
        mocker.patch(
            "kedro.framework.cli.cli.bootstrap_project", return_value=fake_metadata
        )
        mocker.patch(
            "kedro.framework.cli.cli.importlib.import_module",
            return_value=Module(cli=cli),
        )
        kedro_cli = KedroCLI(fake_metadata.project_path)
        raw_cli_structure = _get_cli_structure(kedro_cli, get_help=False)
        assert isinstance(raw_cli_structure["kedro"]["new"], dict)
        assert sorted(list(raw_cli_structure["kedro"]["new"].keys())) == sorted(
            [
                "--verbose",
                "-v",
                "--config",
                "-c",
                "--starter",
                "-s",
                "--checkout",
                "--directory",
                "--help",
                "--example",
                "--name",
                "--tools",
                "--telemetry",
                "-e",
                "-n",
                "-t",
                "-tc",
            ]
        )
        # now check that once params and args are reached, the values are None
        assert raw_cli_structure["kedro"]["new"]["--starter"] is None
        assert raw_cli_structure["kedro"]["new"]["--checkout"] is None
        assert raw_cli_structure["kedro"]["new"]["--help"] is None
        assert raw_cli_structure["kedro"]["new"]["-c"] is None

    def test_get_cli_structure_help(self, mocker, fake_metadata):
        Module = namedtuple("Module", ["cli"])
        mocker.patch("kedro.framework.cli.cli._is_project", return_value=True)
        mocker.patch(
            "kedro.framework.cli.cli.bootstrap_project", return_value=fake_metadata
        )
        mocker.patch(
            "kedro.framework.cli.cli.importlib.import_module",
            return_value=Module(cli=cli),
        )
        kedro_cli = KedroCLI(fake_metadata.project_path)
        help_cli_structure = _get_cli_structure(kedro_cli, get_help=True)

        assert isinstance(help_cli_structure, dict)
        assert isinstance(help_cli_structure["kedro"], dict)

        for k, v in help_cli_structure["kedro"].items():
            assert isinstance(k, str)
            if isinstance(v, dict):
                for sub_key in v:
                    assert isinstance(help_cli_structure["kedro"][k][sub_key], str)
                    assert help_cli_structure["kedro"][k][sub_key].startswith(
                        "Usage:  [OPTIONS]"
                    )
            elif isinstance(v, str):
                assert v.startswith("Usage:  [OPTIONS]")

    @pytest.mark.parametrize(
        "input_command_args, expected_masked_args",
        [
            ([], []),
            (
                ["info"],
                ["info"],
            ),
            (
                ["run", "--pipeline=data_science"],
                ["run", "--pipeline", MASK],
            ),
            (
                ["catalog", "list"],
                ["catalog", "list"],
            ),
            (
                ["pipeline", "create", "mypipeline"],
                ["pipeline", "create", MASK],
            ),
            (
                ["run", "-p", "bar"],
                ["run", "-p", MASK],
            ),
            (
                ["run", "--params=hello=4", "--pipeline=my_pipeline"],
                ["run", "--params", MASK, "--pipeline", MASK],
            ),
        ],
    )
    def test_mask_kedro_cli(
        self, input_command_args, expected_masked_args, fake_metadata, mocker
    ):
        Module = namedtuple("Module", ["cli"])
        mocker.patch("kedro.framework.cli.cli._is_project", return_value=True)
        mocker.patch(
            "kedro.framework.cli.cli.bootstrap_project", return_value=fake_metadata
        )
        mocker.patch(
            "kedro.framework.cli.cli.importlib.import_module",
            return_value=Module(cli=cli),
        )
        kedro_cli = KedroCLI(fake_metadata.project_path)
        assert expected_masked_args == _mask_kedro_cli(
            cli=kedro_cli, command_args=input_command_args
        )
