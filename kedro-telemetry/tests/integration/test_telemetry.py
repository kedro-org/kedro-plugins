from pathlib import Path

from click.testing import CliRunner
from kedro.framework.cli.cli import KedroCLI
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from pytest import fixture
import shutil
import tempfile
from kedro_telemetry.plugin import KedroTelemetryHook


@fixture
def dummy_project_path():
    temp_dir = tempfile.mkdtemp()
    template_project_path = Path(__file__).parent / "dummy-project"
    shutil.copytree(template_project_path, Path(temp_dir), dirs_exist_ok=True)

    yield Path(temp_dir)

    shutil.rmtree(temp_dir)


@fixture()
def reset_telemetry(mocker):
    new_telemetry_hook = KedroTelemetryHook()
    mocker.patch("kedro_telemetry.plugin.telemetry_hook", new_telemetry_hook)
    return new_telemetry_hook


class TestKedroTelemetryHookIntegration:
    def test_telemetry_sent_once_with_kedro_run(
        self, mocker, dummy_project_path, reset_telemetry
    ):
        telemetry_hook = reset_telemetry
        mocked_heap_call = mocker.patch.object(
            telemetry_hook, "_send_telemetry_heap_event"
        )

        kedro_cli = KedroCLI(dummy_project_path)
        result = CliRunner().invoke(kedro_cli, ["run"])
        assert result.exit_code == 0
        mocked_heap_call.assert_called_once()

    def test_telemetry_sent_once_with_other_kedro_command(
        self, mocker, dummy_project_path, reset_telemetry
    ):
        telemetry_hook = reset_telemetry
        mocked_heap_call = mocker.patch.object(
            telemetry_hook, "_send_telemetry_heap_event"
        )

        kedro_cli = KedroCLI(dummy_project_path)
        result = CliRunner().invoke(kedro_cli, ["info"])
        assert result.exit_code == 0
        mocked_heap_call.assert_called_once()

    def test_telemetry_sent_once_with_session_run(
        self, mocker, dummy_project_path, reset_telemetry
    ):
        telemetry_hook = reset_telemetry
        mocked_heap_call = mocker.patch.object(
            telemetry_hook, "_send_telemetry_heap_event"
        )

        bootstrap_project(dummy_project_path)
        with KedroSession.create(project_path=dummy_project_path) as session:
            session.run()
        mocked_heap_call.assert_called_once()
