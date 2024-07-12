from pathlib import Path

from click.testing import CliRunner
from kedro.framework.cli.cli import KedroCLI
from kedro.framework.session import KedroSession
from pytest import fixture


@fixture
def dummy_project_path():
    return Path(__file__).parent / "dummy-project"

class TestKedroTelemetryHookIntegration:
    def test_telemetry_sent_once_with_kedro_run(self, mocker, dummy_project_path):
        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        kedro_cli = KedroCLI(dummy_project_path)
        result = CliRunner().invoke(kedro_cli, [])
        mocked_heap_call.assert_called_once()

    def test_telemetry_sent_once_with_session_run(self, mocker, dummy_project_path):
        mocked_heap_call = mocker.patch("kedro_telemetry.plugin._send_heap_event")
        with KedroSession.create(project_path=dummy_project_path) as session:
            session.run()
            mocked_heap_call.assert_called_once()

