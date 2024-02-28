import sys
from pathlib import Path

import requests
import yaml
from kedro import __version__ as kedro_version
from kedro.framework.project import pipelines
from kedro.framework.startup import ProjectMetadata
from kedro.io import DataCatalog, MemoryDataset
from kedro.pipeline import node
from kedro.pipeline.modular_pipeline import pipeline as modular_pipeline
from pytest import fixture, mark

from kedro_telemetry import __version__ as TELEMETRY_VERSION
from kedro_telemetry.plugin import (
    KNOWN_CI_ENV_VAR_KEYS,
    KedroTelemetryCLIHooks,
    KedroTelemetryProjectHooks,
    _check_for_telemetry_consent,
    _confirm_consent,
    _is_known_ci_env,
)

REPO_NAME = "dummy_project"
PACKAGE_NAME = "dummy_package"

MOCK_PYPROJECT_TOOLS = """
[build-system]
requires = [ "setuptools",]
build-backend = "setuptools.build_meta"

[project]
name = "spaceflights"
readme = "README.md"
dynamic = [ "dependencies", "version",]

[project.scripts]
new-proj = "spaceflights.__main__:main"

[tool.kedro]
package_name = "spaceflights"
project_name = "spaceflights"
kedro_init_version = "0.18.14"
tools = ["Linting", "Testing", "Custom Logging", "Documentation", "Data Structure", "PySpark"]
example_pipeline = "True"

[project.entry-points."kedro.hooks"]

[tool.setuptools.dynamic.dependencies]
file = "requirements.txt"

[tool.setuptools.dynamic.version]
attr = "spaceflights.__version__"

[tool.setuptools.packages.find]
where = [ "src",]
namespaces = false
"""


@fixture
def fake_metadata(tmp_path):
    metadata = ProjectMetadata(
        config_file=tmp_path / REPO_NAME / "pyproject.toml",
        package_name=PACKAGE_NAME,
        project_name="CLI Testing Project",
        project_path=tmp_path / REPO_NAME,
        source_dir=tmp_path / REPO_NAME / "src",
        kedro_init_version=kedro_version,
        tools=[],
        example_pipeline="No",
    )
    return metadata


@fixture
def fake_catalog():
    catalog = DataCatalog(
        {
            "dummy_1": MemoryDataset(),
            "dummy_2": MemoryDataset(),
            "dummy_3": MemoryDataset(),
            "parameters": MemoryDataset(),
            "params:dummy": MemoryDataset(),
        }
    )
    return catalog


def identity(arg):
    return arg


@fixture
def fake_context():
    class MockKedroContext:
        # A dummy stand-in for KedroContext sufficient for this test
        project_path = Path("")

    return MockKedroContext()


@fixture
def fake_default_pipeline():
    mock_default_pipeline = modular_pipeline(
        [
            node(identity, ["input"], ["intermediate"], name="node0"),
            node(identity, ["intermediate"], ["output"], name="node1"),
        ],
    )
    return mock_default_pipeline


@fixture
def fake_sub_pipeline():
    mock_sub_pipeline = modular_pipeline(
        [
            node(identity, ["input"], ["intermediate"], name="node0"),
        ],
    )
    return mock_sub_pipeline


class TestKedroTelemetryCLIHooks:
    def test_before_command_run(self, mocker, fake_metadata):
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        mocker.patch("kedro_telemetry.plugin._is_known_ci_env", return_value=True)
        mocked_anon_id = mocker.patch("kedro_telemetry.plugin._hash")
        mocked_anon_id.return_value = "digested"
        mocker.patch("kedro_telemetry.plugin.PACKAGE_NAME", "spaceflights")
        mocker.patch(
            "kedro_telemetry.plugin._get_hashed_username",
            return_value="hashed_username",
        )

        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]
        telemetry_hook.before_command_run(fake_metadata, command_args)
        expected_properties = {
            "username": "hashed_username",
            "package_name": "digested",
            "project_version": kedro_version,
            "telemetry_version": TELEMETRY_VERSION,
            "python_version": sys.version,
            "os": sys.platform,
            "command": "kedro --version",
            "is_ci_env": True,
        }
        generic_properties = {
            **expected_properties,
            "main_command": "--version",
        }

        expected_calls = [
            mocker.call(
                event_name="Command run: --version",
                identity="hashed_username",
                properties=expected_properties,
            ),
            mocker.call(
                event_name="CLI command",
                identity="hashed_username",
                properties=generic_properties,
            ),
        ]
        assert mocked_heap_call.call_args_list == expected_calls

    def test_before_command_run_with_tools(self, mocker, fake_metadata):
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        mocker.patch("kedro_telemetry.plugin._is_known_ci_env", return_value=True)
        mocked_anon_id = mocker.patch("kedro_telemetry.plugin._hash")
        mocked_anon_id.return_value = "digested"
        mocker.patch("kedro_telemetry.plugin.PACKAGE_NAME", "spaceflights")
        mocker.patch(
            "kedro_telemetry.plugin._get_hashed_username",
            return_value="hashed_username",
        )

        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        mocker.patch("builtins.open", mocker.mock_open(read_data=MOCK_PYPROJECT_TOOLS))
        mocker.patch("pathlib.Path.exists", return_value=True)
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]
        telemetry_hook.before_command_run(fake_metadata, command_args)
        expected_properties = {
            "username": "hashed_username",
            "package_name": "digested",
            "project_version": kedro_version,
            "telemetry_version": TELEMETRY_VERSION,
            "python_version": sys.version,
            "os": sys.platform,
            "is_ci_env": True,
            "command": "kedro --version",
            "tools": "Linting, Testing, Custom Logging, Documentation, Data Structure, PySpark",
            "example_pipeline": "True",
        }
        generic_properties = {
            **expected_properties,
            "main_command": "--version",
        }

        expected_calls = [
            mocker.call(
                event_name="Command run: --version",
                identity="hashed_username",
                properties=expected_properties,
            ),
            mocker.call(
                event_name="CLI command",
                identity="hashed_username",
                properties=generic_properties,
            ),
        ]
        assert mocked_heap_call.call_args_list == expected_calls

    def test_before_command_run_empty_args(self, mocker, fake_metadata):
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        mocker.patch("kedro_telemetry.plugin._is_known_ci_env", return_value=True)
        mocked_anon_id = mocker.patch("kedro_telemetry.plugin._hash")
        mocked_anon_id.return_value = "digested"
        mocker.patch("kedro_telemetry.plugin.PACKAGE_NAME", "spaceflights")

        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = []
        telemetry_hook.before_command_run(fake_metadata, command_args)
        expected_properties = {
            "username": "digested",
            "package_name": "digested",
            "project_version": kedro_version,
            "telemetry_version": TELEMETRY_VERSION,
            "python_version": sys.version,
            "os": sys.platform,
            "is_ci_env": True,
            "command": "kedro",
        }
        generic_properties = {
            "main_command": "kedro",
            **expected_properties,
        }

        expected_calls = [
            mocker.call(
                event_name="Command run: kedro",
                identity="digested",
                properties=expected_properties,
            ),
            mocker.call(
                event_name="CLI command",
                identity="digested",
                properties=generic_properties,
            ),
        ]

        assert mocked_heap_call.call_args_list == expected_calls

    def test_before_command_run_no_consent_given(self, mocker, fake_metadata):
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=False
        )

        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]
        telemetry_hook.before_command_run(fake_metadata, command_args)

        mocked_heap_call.assert_not_called()

    def test_before_command_run_connection_error(self, mocker, fake_metadata, caplog):
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]

        mocked_post_request = mocker.patch(
            "requests.post", side_effect=requests.exceptions.ConnectionError()
        )
        telemetry_hook.before_command_run(fake_metadata, command_args)
        msg = "Failed to send data to Heap. Exception of type 'ConnectionError' was raised."
        assert msg in caplog.messages[-1]
        mocked_post_request.assert_called()

    def test_before_command_run_anonymous(self, mocker, fake_metadata):
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        mocker.patch("kedro_telemetry.plugin._is_known_ci_env", return_value=True)
        mocked_anon_id = mocker.patch("kedro_telemetry.plugin._hash")
        mocked_anon_id.return_value = "digested"
        mocker.patch("kedro_telemetry.plugin.PACKAGE_NAME", "spaceflights")
        mocker.patch("getpass.getuser", side_effect=Exception)

        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]
        telemetry_hook.before_command_run(fake_metadata, command_args)
        expected_properties = {
            "username": "",
            "command": "kedro --version",
            "package_name": "digested",
            "project_version": kedro_version,
            "telemetry_version": TELEMETRY_VERSION,
            "python_version": sys.version,
            "os": sys.platform,
            "is_ci_env": True,
        }
        generic_properties = {
            "main_command": "--version",
            **expected_properties,
        }

        expected_calls = [
            mocker.call(
                event_name="Command run: --version",
                identity="",
                properties=expected_properties,
            ),
            mocker.call(
                event_name="CLI command",
                identity="",
                properties=generic_properties,
            ),
        ]
        assert mocked_heap_call.call_args_list == expected_calls

    def test_before_command_run_heap_call_error(self, mocker, fake_metadata, caplog):
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        mocked_heap_call = mocker.patch(
            "kedro_telemetry.plugin._send_heap_event", side_effect=Exception
        )
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]

        telemetry_hook.before_command_run(fake_metadata, command_args)
        msg = (
            "Something went wrong in hook implementation to send command run data to"
            " Heap. Exception:"
        )
        assert msg in caplog.messages[-1]
        mocked_heap_call.assert_called()

    def test_check_for_telemetry_consent_given(self, mocker, fake_metadata):
        Path(fake_metadata.project_path, "conf").mkdir(parents=True)
        telemetry_file_path = fake_metadata.project_path / ".telemetry"
        with open(telemetry_file_path, "w", encoding="utf-8") as telemetry_file:
            yaml.dump({"consent": True}, telemetry_file)

        mock_create_file = mocker.patch("kedro_telemetry.plugin._confirm_consent")
        mock_create_file.assert_not_called()
        assert _check_for_telemetry_consent(fake_metadata.project_path)

    def test_check_for_telemetry_consent_not_given(self, mocker, fake_metadata):
        Path(fake_metadata.project_path, "conf").mkdir(parents=True)
        telemetry_file_path = fake_metadata.project_path / ".telemetry"
        with open(telemetry_file_path, "w", encoding="utf-8") as telemetry_file:
            yaml.dump({"consent": False}, telemetry_file)

        mock_create_file = mocker.patch("kedro_telemetry.plugin._confirm_consent")
        mock_create_file.assert_not_called()
        assert not _check_for_telemetry_consent(fake_metadata.project_path)

    def test_check_for_telemetry_consent_empty_file(self, mocker, fake_metadata):
        Path(fake_metadata.project_path, "conf").mkdir(parents=True)
        telemetry_file_path = fake_metadata.project_path / ".telemetry"
        mock_create_file = mocker.patch(
            "kedro_telemetry.plugin._confirm_consent", return_value=True
        )

        assert _check_for_telemetry_consent(fake_metadata.project_path)
        mock_create_file.assert_called_once_with(telemetry_file_path)

    def test_check_for_telemetry_no_consent_empty_file(self, mocker, fake_metadata):
        Path(fake_metadata.project_path, "conf").mkdir(parents=True)
        telemetry_file_path = fake_metadata.project_path / ".telemetry"
        mock_create_file = mocker.patch(
            "kedro_telemetry.plugin._confirm_consent", return_value=False
        )

        assert not _check_for_telemetry_consent(fake_metadata.project_path)
        mock_create_file.assert_called_once_with(telemetry_file_path)

    def test_check_for_telemetry_consent_file_no_consent_field(
        self, mocker, fake_metadata
    ):
        Path(fake_metadata.project_path, "conf").mkdir(parents=True)
        telemetry_file_path = fake_metadata.project_path / ".telemetry"
        with open(telemetry_file_path, "w", encoding="utf8") as telemetry_file:
            yaml.dump({"nonsense": "bla"}, telemetry_file)

        mock_create_file = mocker.patch(
            "kedro_telemetry.plugin._confirm_consent", return_value=True
        )

        assert _check_for_telemetry_consent(fake_metadata.project_path)
        mock_create_file.assert_called_once_with(telemetry_file_path)

    def test_check_for_telemetry_consent_file_invalid_yaml(self, mocker, fake_metadata):
        Path(fake_metadata.project_path, "conf").mkdir(parents=True)
        telemetry_file_path = fake_metadata.project_path / ".telemetry"
        telemetry_file_path.write_text("invalid_ yaml")

        mock_create_file = mocker.patch(
            "kedro_telemetry.plugin._confirm_consent", return_value=True
        )

        assert _check_for_telemetry_consent(fake_metadata.project_path)
        mock_create_file.assert_called_once_with(telemetry_file_path)

    def test_confirm_consent_yaml_dump_error(self, mocker, fake_metadata, caplog):
        Path(fake_metadata.project_path, "conf").mkdir(parents=True)
        telemetry_file_path = fake_metadata.project_path / ".telemetry"
        mocker.patch("yaml.dump", side_efyfect=Exception)

        assert not _confirm_consent(telemetry_file_path)

        msg = (
            "Failed to confirm consent. No data was sent to Heap. Exception: "
            "pytest: reading from stdin while output is captured!  Consider using `-s`."
        )
        assert msg in caplog.messages[-1]

    @mark.parametrize(
        "env_vars,result",
        [
            ({"CI": "true"}, True),
            ({"CI": "false"}, False),
            ({"CI": "false", "CODEBUILD_BUILD_ID": "Testing known CI env var"}, True),
            ({"JENKINS_URL": "Testing known CI env var"}, True),
            ({"CI": "false", "TRAVIS": "Testing known CI env var"}, True),
            ({"GITLAB_CI": "Testing known CI env var"}, True),
            ({"CI": "false", "CIRCLECI": "Testing known CI env var"}, True),
            (
                {"CI": "false", "BITBUCKET_BUILD_NUMBER": "Testing known CI env var"},
                True,
            ),
        ],
    )
    def test_check_is_known_ci_env(self, monkeypatch, env_vars, result):
        for env_var, env_var_value in env_vars.items():
            monkeypatch.setenv(env_var, env_var_value)

        known_ci_vars = KNOWN_CI_ENV_VAR_KEYS
        # Because our CI runs on Github Actions, this would always return True otherwise
        known_ci_vars.discard("GITHUB_ACTION")
        assert _is_known_ci_env(known_ci_vars) == result


class TestKedroTelemetryProjectHooks:
    def test_after_context_created_without_kedro_run(  # noqa: PLR0913
        self,
        mocker,
        fake_catalog,
        fake_default_pipeline,
        fake_sub_pipeline,
        fake_context,
    ):
        mocker.patch.dict(
            pipelines, {"__default__": fake_default_pipeline, "sub": fake_sub_pipeline}
        )
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        mocker.patch("kedro_telemetry.plugin._is_known_ci_env", return_value=True)
        mocker.patch("kedro_telemetry.plugin._hash", return_value="digested")
        mocker.patch("kedro_telemetry.plugin.PACKAGE_NAME", "spaceflights")
        mocker.patch(
            "kedro_telemetry.plugin._get_hashed_username",
            return_value="hashed_username",
        )
        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        mocker.patch("kedro_telemetry.plugin.open")
        mocker.patch("kedro_telemetry.plugin.toml.load")

        # Without CLI invoked - i.e. `session.run` in Jupyter/IPython
        telemetry_hook = KedroTelemetryProjectHooks()
        telemetry_hook.after_context_created(fake_context)
        telemetry_hook.after_catalog_created(fake_catalog)

        project_properties = {
            "username": "hashed_username",
            "package_name": "digested",
            "project_version": kedro_version,
            "telemetry_version": TELEMETRY_VERSION,
            "python_version": sys.version,
            "os": sys.platform,
            "is_ci_env": True,
        }
        project_statistics = {
            "number_of_datasets": 3,
            "number_of_nodes": 2,
            "number_of_pipelines": 2,
        }
        expected_properties = {**project_properties, **project_statistics}

        expected_call = mocker.call(
            event_name="Kedro Project Statistics",
            identity="hashed_username",
            properties=expected_properties,
        )

        # The 1st call is the Project Hook without CLI
        assert mocked_heap_call.call_args_list[0] == expected_call

    def test_after_context_created_with_kedro_run(  # noqa: PLR0913
        self,
        mocker,
        fake_catalog,
        fake_metadata,
        fake_default_pipeline,
        fake_sub_pipeline,
        fake_context,
    ):
        mocker.patch.dict(
            pipelines, {"__default__": fake_default_pipeline, "sub": fake_sub_pipeline}
        )
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        mocker.patch("kedro_telemetry.plugin._is_known_ci_env", return_value=True)
        mocker.patch("kedro_telemetry.plugin._hash", return_value="digested")
        mocker.patch("kedro_telemetry.plugin.PACKAGE_NAME", "spaceflights")
        mocker.patch(
            "kedro_telemetry.plugin._get_hashed_username",
            return_value="hashed_username",
        )
        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        mocker.patch("kedro_telemetry.plugin.toml.load")
        # CLI run first
        telemetry_cli_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]
        telemetry_cli_hook.before_command_run(fake_metadata, command_args)

        # Follow by project run
        telemetry_hook = KedroTelemetryProjectHooks()
        telemetry_hook.after_context_created(fake_context)
        telemetry_hook.after_catalog_created(fake_catalog)

        project_properties = {
            "username": "hashed_username",
            "package_name": "digested",
            "project_version": kedro_version,
            "telemetry_version": TELEMETRY_VERSION,
            "python_version": sys.version,
            "os": sys.platform,
            "is_ci_env": True,
        }
        project_statistics = {
            "number_of_datasets": 3,
            "number_of_nodes": 2,
            "number_of_pipelines": 2,
        }
        expected_properties = {**project_properties, **project_statistics}

        expected_call = mocker.call(
            event_name="Kedro Project Statistics",
            identity="hashed_username",
            properties=expected_properties,
        )

        # CLI hook makes the first 2 calls, the 3rd one is the Project hook
        assert mocked_heap_call.call_args_list[2] == expected_call

    def test_after_context_created_with_kedro_run_and_tools(  # noqa: PLR0913
        self,
        mocker,
        fake_catalog,
        fake_metadata,
        fake_default_pipeline,
        fake_sub_pipeline,
        fake_context,
    ):
        mocker.patch.dict(
            pipelines, {"__default__": fake_default_pipeline, "sub": fake_sub_pipeline}
        )
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        mocker.patch("kedro_telemetry.plugin._is_known_ci_env", return_value=True)
        mocker.patch("kedro_telemetry.plugin._hash", return_value="digested")
        mocker.patch("kedro_telemetry.plugin.PACKAGE_NAME", "spaceflights")
        mocker.patch(
            "kedro_telemetry.plugin._get_hashed_username",
            return_value="hashed_username",
        )
        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        mocker.patch("builtins.open", mocker.mock_open(read_data=MOCK_PYPROJECT_TOOLS))
        mocker.patch("pathlib.Path.exists", return_value=True)

        # CLI run first
        telemetry_cli_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]
        telemetry_cli_hook.before_command_run(fake_metadata, command_args)

        # Follow by project run
        telemetry_hook = KedroTelemetryProjectHooks()
        telemetry_hook.after_context_created(fake_context)
        telemetry_hook.after_catalog_created(fake_catalog)

        project_properties = {
            "username": "hashed_username",
            "package_name": "digested",
            "project_version": kedro_version,
            "telemetry_version": TELEMETRY_VERSION,
            "python_version": sys.version,
            "os": sys.platform,
            "is_ci_env": True,
            "tools": "Linting, Testing, Custom Logging, Documentation, Data Structure, PySpark",
            "example_pipeline": "True",
        }
        project_statistics = {
            "number_of_datasets": 3,
            "number_of_nodes": 2,
            "number_of_pipelines": 2,
        }
        expected_properties = {**project_properties, **project_statistics}

        expected_call = mocker.call(
            event_name="Kedro Project Statistics",
            identity="hashed_username",
            properties=expected_properties,
        )

        # CLI hook makes the first 2 calls, the 3rd one is the Project hook
        assert mocked_heap_call.call_args_list[2] == expected_call

    def test_after_context_created_no_consent_given(self, mocker):
        fake_context = mocker.Mock()
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=False
        )

        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        telemetry_hook = KedroTelemetryProjectHooks()
        telemetry_hook.after_context_created(fake_context)

        mocked_heap_call.assert_not_called()
