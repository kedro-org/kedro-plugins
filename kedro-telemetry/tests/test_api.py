"""Tests for the public `kedro_telemetry.api` module."""

from pathlib import Path

from kedro_telemetry.api import send_telemetry_event
from kedro_telemetry.plugin import MISSING_USER_IDENTITY


class TestSendTelemetryEvent:
    def test_sends_event_with_merged_properties(self, mocker):
        mocker.patch(
            "kedro_telemetry.api._check_for_telemetry_consent", return_value=None
        )
        mocker.patch(
            "kedro_telemetry.api._get_or_create_uuid", return_value="user-uuid"
        )
        mocker.patch(
            "kedro_telemetry.api._get_project_properties",
            return_value={"username": "user-uuid", "project_version": "1.0.0"},
        )
        mocked_heap_call = mocker.patch(
            "kedro_telemetry.api._send_heap_event", return_value=True
        )

        sent = send_telemetry_event(
            "kedro_skills_install",
            properties={
                "skill_id": "catalog-config",
                "success": True,
                "target_ides": ["claude", "cursor"],
            },
            project_path=Path("/fake/project"),
        )

        assert sent is True
        mocked_heap_call.assert_called_once_with(
            event_name="kedro_skills_install",
            identity="user-uuid",
            properties={
                "skill_id": "catalog-config",
                "success": True,
                # Non-scalar values are stringified so Heap accepts them
                "target_ides": "['claude', 'cursor']",
                "username": "user-uuid",
                "project_version": "1.0.0",
            },
        )

    def test_no_event_when_consent_denied(self, mocker):
        mocker.patch(
            "kedro_telemetry.api._check_for_telemetry_consent", return_value=False
        )
        mocked_heap_call = mocker.patch("kedro_telemetry.api._send_heap_event")

        sent = send_telemetry_event("kedro_skills_install", {"skill_id": "x"})

        assert sent is False
        mocked_heap_call.assert_not_called()

    def test_no_event_when_do_not_track_set(self, mocker, monkeypatch):
        monkeypatch.setenv("DO_NOT_TRACK", "1")
        mocked_heap_call = mocker.patch("kedro_telemetry.api._send_heap_event")

        sent = send_telemetry_event("kedro_skills_update", {"skills_updated": 1})

        assert sent is False
        mocked_heap_call.assert_not_called()

    def test_missing_identity_placeholder(self, mocker):
        mocker.patch(
            "kedro_telemetry.api._check_for_telemetry_consent", return_value=None
        )
        mocker.patch("kedro_telemetry.api._get_or_create_uuid", return_value="")
        mocker.patch(
            "kedro_telemetry.api._get_project_properties",
            return_value={"username": ""},
        )
        mocked_heap_call = mocker.patch(
            "kedro_telemetry.api._send_heap_event", return_value=True
        )

        sent = send_telemetry_event("kedro_skills_uninstall", {"skill_id": "x"})

        assert sent is True
        assert mocked_heap_call.call_args.kwargs["identity"] == MISSING_USER_IDENTITY

    def test_returns_false_when_heap_rejects(self, mocker):
        mocker.patch(
            "kedro_telemetry.api._check_for_telemetry_consent", return_value=None
        )
        mocker.patch(
            "kedro_telemetry.api._get_or_create_uuid", return_value="user-uuid"
        )
        mocker.patch(
            "kedro_telemetry.api._get_project_properties",
            return_value={"username": "user-uuid"},
        )
        mocker.patch("kedro_telemetry.api._send_heap_event", return_value=False)

        assert send_telemetry_event("kedro_skills_install", {"skill_id": "x"}) is False

    def test_base_properties_cannot_be_overridden(self, mocker):
        mocker.patch(
            "kedro_telemetry.api._check_for_telemetry_consent", return_value=None
        )
        mocker.patch(
            "kedro_telemetry.api._get_or_create_uuid", return_value="user-uuid"
        )
        mocker.patch(
            "kedro_telemetry.api._get_project_properties",
            return_value={"username": "user-uuid"},
        )
        mocked_heap_call = mocker.patch("kedro_telemetry.api._send_heap_event")

        send_telemetry_event("some_event", {"username": "spoofed"})

        assert (
            mocked_heap_call.call_args.kwargs["properties"]["username"] == "user-uuid"
        )

    def test_never_raises(self, mocker):
        mocker.patch(
            "kedro_telemetry.api._check_for_telemetry_consent", return_value=None
        )
        mocker.patch(
            "kedro_telemetry.api._get_or_create_uuid",
            side_effect=RuntimeError("boom"),
        )

        assert send_telemetry_event("kedro_skills_install", {}) is False
