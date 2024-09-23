from pathlib import Path

from click.testing import CliRunner
from kedro.framework.cli.cli import KedroCLI
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from pytest import fixture


@fixture
def dummy_project_path():
    return Path(__file__).parent / "dummy-project"


class TestKedroTelemetryHookIntegration:
    def test_telemetry_sent_once_with_kedro_run(self, mocker, dummy_project_path):
        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        kedro_cli = KedroCLI(dummy_project_path)
        CliRunner().invoke(kedro_cli, ["run"])
        mocked_heap_call.assert_called_once()

    def test_telemetry_sent_once_with_other_kedro_command(
        self, mocker, dummy_project_path
    ):
        from kedro_telemetry.plugin import telemetry_hook

        telemetry_hook.consent = None
        telemetry_hook._sent = False
        telemetry_hook.event_properties = None
        telemetry_hook.project_path = None

        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        kedro_cli = KedroCLI(dummy_project_path)
        CliRunner().invoke(kedro_cli, ["run"])
        mocked_heap_call.assert_called_once()

    def test_telemetry_sent_once_with_session_run(self, mocker, dummy_project_path):
        from kedro_telemetry.plugin import telemetry_hook

        telemetry_hook.consent = None
        telemetry_hook._sent = False
        telemetry_hook.event_properties = None
        telemetry_hook.project_path = None

        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        mocker.patch(
            "kedro_telemetry.plugin._check_for_telemetry_consent", return_value=True
        )
        # Mock because all tests are sharing the kedro_telemetry.plugin.telemetry_hook object

        bootstrap_project(dummy_project_path)
        with KedroSession.create(project_path=dummy_project_path) as session:
            session.run()
        mocked_heap_call.assert_called_once()
