"""Kedro Telemetry plugin for collecting Kedro usage data."""

import hashlib
import json
import logging
import os
import socket
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import click
import requests
import yaml
from kedro.framework.cli.cli import KedroCLI
from kedro.framework.cli.hooks import cli_hook_impl
from kedro.framework.startup import ProjectMetadata

from kedro_telemetry import __version__ as telemetry_version
from kedro_telemetry.masking import _get_cli_structure, _mask_kedro_cli

HEAP_APPID_PROD = "2388822444"

HEAP_ENDPOINT = "https://heapanalytics.com/api/track"
HEAP_HEADERS = {"Content-Type": "application/json"}
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

logger = logging.getLogger(__name__)


class KedroTelemetryCLIHooks:
    """Hook to send CLI command data to Heap"""

    # pylint: disable=too-few-public-methods

    @cli_hook_impl
    def before_command_run(
        self, project_metadata: ProjectMetadata, command_args: List[str]
    ):
        """Hook implementation to send command run data to Heap"""
        # pylint: disable=no-self-use

        # get KedroCLI and its structure from actual project root
        cli = KedroCLI(project_path=Path.cwd())
        cli_struct = _get_cli_structure(cli_obj=cli, get_help=False)
        masked_command_args = _mask_kedro_cli(
            cli_struct=cli_struct, command_args=command_args
        )
        main_command = masked_command_args[0] if masked_command_args else "kedro"
        if not project_metadata:  # in package mode
            return

        consent = _check_for_telemetry_consent(project_metadata.project_path)
        if not consent:
            click.secho(
                "Kedro-Telemetry is installed, but you have opted out of "
                "sharing usage analytics so none will be collected.",
                fg="green",
            )
            return

        logger.info("You have opted into product usage analytics.")

        try:
            hashed_computer_name = hashlib.sha512(
                bytes(socket.gethostname(), encoding="utf8")
            )
        except socket.timeout as exc:
            logger.warning(
                "Socket timeout when trying to get the computer name. "
                "No data was sent to Heap. Exception: %s",
                exc,
            )
            return

        properties = _format_user_cli_data(masked_command_args, project_metadata)

        _send_heap_event(
            event_name=f"Command run: {main_command}",
            identity=hashed_computer_name.hexdigest(),
            properties=properties,
        )

        # send generic event too so it's easier in data processing
        generic_properties = deepcopy(properties)
        generic_properties["main_command"] = main_command
        _send_heap_event(
            event_name="CLI command",
            identity=hashed_computer_name.hexdigest(),
            properties=generic_properties,
        )


def _format_user_cli_data(command_args: List[str], project_metadata: ProjectMetadata):
    """Hash username, format CLI command, system and project data to send to Heap."""
    hashed_username = ""
    try:
        hashed_username = hashlib.sha512(bytes(os.getlogin(), encoding="utf8"))
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning(
            "Something went wrong with getting the username to send to Heap. "
            "Exception: %s",
            exc,
        )

    hashed_package_name = hashlib.sha512(
        bytes(project_metadata.package_name, encoding="utf8")
    )
    hashed_project_name = hashlib.sha512(
        bytes(project_metadata.project_name, encoding="utf8")
    )
    project_version = project_metadata.project_version

    return {
        "username": hashed_username.hexdigest() if hashed_username else "anonymous",
        "command": f"kedro {' '.join(command_args)}" if command_args else "kedro",
        "package_name": hashed_package_name.hexdigest(),
        "project_name": hashed_project_name.hexdigest(),
        "project_version": project_version,
        "telemetry_version": telemetry_version,
        "python_version": sys.version,
        "os": sys.platform,
    }


def _get_heap_app_id() -> str:
    """
    Get the Heap App ID to send the data to.
    This will be the development ID if it's set as an
    environment variable, otherwise it will be the production ID.
    """
    return os.environ.get("HEAP_APPID_DEV", HEAP_APPID_PROD)


def _send_heap_event(
    event_name: str, identity: str, properties: Dict[str, Any] = None
) -> None:
    data = {
        "app_id": _get_heap_app_id(),
        "identity": identity,
        "event": event_name,
        "timestamp": datetime.now().strftime(TIMESTAMP_FORMAT),
        "properties": properties or {},
    }

    try:
        resp = requests.post(
            url=HEAP_ENDPOINT, headers=HEAP_HEADERS, data=json.dumps(data)
        )
        if resp.status_code != 200:
            logger.warning(
                "Failed to send data to Heap. Response code returned: %s, Response reason: %s",
                resp.status_code,
                resp.reason,
            )
    except requests.exceptions.RequestException as exc:
        logger.warning(
            "Failed to send data to Heap. Exception of type '%s' was raised.",
            type(exc).__name__,
        )


def _check_for_telemetry_consent(project_path: Path) -> bool:
    telemetry_file_path = project_path / ".telemetry"
    if not telemetry_file_path.exists():
        return _confirm_consent(telemetry_file_path)
    with open(telemetry_file_path, encoding="utf-8") as telemetry_file:
        telemetry = yaml.safe_load(telemetry_file)
        if _is_valid_syntax(telemetry):
            return telemetry["consent"]
        return _confirm_consent(telemetry_file_path)


def _is_valid_syntax(telemetry: Any) -> bool:
    return isinstance(telemetry, dict) and isinstance(
        telemetry.get("consent", None), bool
    )


def _confirm_consent(telemetry_file_path: Path) -> bool:
    try:
        with telemetry_file_path.open("w") as telemetry_file:
            confirm_msg = (
                "As an open-source project, we collect usage analytics. \n"
                "We cannot see nor store information contained in "
                "a Kedro project. \nYou can find out more by reading our "
                "privacy notice: \n"
                "https://github.com/kedro-org/kedro-plugins/tree/main/kedro-telemetry#"
                "privacy-notice \n"
                "Do you opt into usage analytics? "
            )
            if click.confirm(confirm_msg):
                yaml.dump({"consent": True}, telemetry_file)
                click.secho("You have opted into product usage analytics.", fg="green")
                return True
            yaml.dump({"consent": False}, telemetry_file)
            return False
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning(
            "Failed to confirm consent. Exception: %s",
            exc,
        )
        return False


cli_hooks = KedroTelemetryCLIHooks()
