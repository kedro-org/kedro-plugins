"""Public helpers for sending custom telemetry events from Kedro plugins."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from kedro_telemetry.plugin import (
    MISSING_USER_IDENTITY,
    _check_for_telemetry_consent,
    _get_or_create_uuid,
    _get_project_properties,
    _send_heap_event,
)

logger = logging.getLogger(__name__)

_SCALAR_TYPES = (str, int, float, bool)


def send_telemetry_event(
    event_name: str,
    properties: dict[str, Any] | None = None,
    project_path: Path | None = None,
) -> bool:
    """Send a custom telemetry event on behalf of a Kedro plugin.

    The standard telemetry consent flow applies: the event is dropped when the
    user has opted out via the `DO_NOT_TRACK` or `KEDRO_DISABLE_TELEMETRY`
    environment variables or a `.telemetry` file with `consent: false` in
    `project_path`. Plugins calling this helper must not introduce a separate
    opt-in of their own.

    The event is enriched with the same anonymous base properties that are
    attached to every `kedro-telemetry` event (hashed project id, Kedro,
    plugin and Python versions, OS, CI flag). Callers must not include
    personal data or project-specific paths in `properties`. Property values
    must be scalars; any other type is converted with `str()` so the payload
    is accepted by Heap.

    Args:
        event_name: Name of the event, e.g. `kedro_skills_install`.
        properties: Event-specific properties to attach.
        project_path: Root of the current Kedro project, when the event
            relates to one. Used for the consent file lookup and the hashed
            project id.

    Returns:
        True if Heap accepted the event, False if consent was withheld,
        the request failed, or Heap rejected the payload. Never raises.
    """
    try:
        consent = _check_for_telemetry_consent(project_path)
        if consent is False:
            return False

        user_uuid = _get_or_create_uuid()

        merged: dict[str, Any] = {
            key: value if isinstance(value, _SCALAR_TYPES) else str(value)
            for key, value in (properties or {}).items()
        }
        merged.update(_get_project_properties(user_uuid, project_path))

        return _send_heap_event(
            event_name=event_name,
            identity=user_uuid if user_uuid else MISSING_USER_IDENTITY,
            properties=merged,
        )
    except Exception as exc:
        logger.debug(
            "Failed to send telemetry event '%s'. Exception: %s", event_name, exc
        )
        return False
