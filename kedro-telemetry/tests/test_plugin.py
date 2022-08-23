import socket
import sys
from pathlib import Path

import requests
import yaml
from kedro import __version__ as kedro_version
from kedro.framework.startup import ProjectMetadata
from pytest import fixture

from kedro_telemetry import __version__ as telemetry_version
from kedro_telemetry.plugin import (
    KedroTelemetryCLIHooks,
    _check_for_telemetry_consent,
    _confirm_consent,
)

REPO_NAME = "dummy_project"
PACKAGE_NAME = "dummy_package"

# pylint: disable=too-few-public-methods


@fixture
def fake_metadata(tmp_path):
    metadata = ProjectMetadata(
        tmp_path / REPO_NAME / "pyproject.toml",
        PACKAGE_NAME,
        "CLI Testing Project",
        tmp_path / REPO_NAME,
        kedro_version,
        tmp_path / REPO_NAME / "src",
    )
    return metadata


class TestKedroTelemetryCLIHooks:
    def test_before_command_run(self, mocker, fake_metadata):
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        mocked_anon_id = mocker.patch("hashlib.sha512")
        mocked_anon_id.return_value.hexdigest.return_value = "digested"

        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]
        telemetry_hook.before_command_run(fake_metadata, command_args)
        expected_properties = {
            "username": "digested",
            "command": "kedro --version",
            "package_name": "digested",
            "project_name": "digested",
            "project_version": kedro_version,
            "telemetry_version": telemetry_version,
            "python_version": sys.version,
            "os": sys.platform,
        }
        generic_properties = {
            "main_command": "--version",
            **expected_properties,
        }

        expected_calls = [
            mocker.call(
                event_name="Command run: --version",
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

    def test_before_command_run_empty_args(self, mocker, fake_metadata):
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        mocked_anon_id = mocker.patch("hashlib.sha512")
        mocked_anon_id.return_value.hexdigest.return_value = "digested"

        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = []
        telemetry_hook.before_command_run(fake_metadata, command_args)
        expected_properties = {
            "username": "digested",
            "command": "kedro",
            "package_name": "digested",
            "project_name": "digested",
            "project_version": kedro_version,
            "telemetry_version": telemetry_version,
            "python_version": sys.version,
            "os": sys.platform,
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

    def test_before_command_run_get_username_failed(self, mocker, fake_metadata):
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]
        mocker.patch("getpass.getuser", side_effect=Exception)
        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")

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
        mocked_anon_id = mocker.patch("hashlib.sha512")
        mocked_anon_id.return_value.hexdigest.return_value = "digested"
        #mocker.patch("getpass.getuser", side_effect=Exception)
        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        telemetry_hook = KedroTelemetryCLIHooks()
        command_args = ["--version"]
        telemetry_hook.before_command_run(fake_metadata, command_args)
        expected_properties = {
            "username": "digested",
            "command": "kedro --version",
            "package_name": "digested",
            "project_name": "digested",
            "project_version": kedro_version,
            "telemetry_version": telemetry_version,
            "python_version": sys.version,
            "os": sys.platform,
        }
        generic_properties = {
            "main_command": "--version",
            **expected_properties,
        }

        expected_calls = [
            mocker.call(
                event_name="Command run: --version",
                identity="digested",
                properties=expected_properties,
            ),
            mocker.call(
                event_name="CLI command",
                identity="digested",
                properties=generic_properties,
            ),
        ]
        print(f"mocked_heap_call args list is {mocked_heap_call.call_args_list}")
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
        mocker.patch("yaml.dump", side_effect=Exception)

        assert not _confirm_consent(telemetry_file_path)

        msg = (
            "Failed to confirm consent. No data was sent to Heap. Exception: "
            "pytest: reading from stdin while output is captured!  Consider using `-s`."
        )
        assert msg in caplog.messages[-1]
