"""Kedro Telemetry plugin for collecting Kedro usage data."""

import getpass
import hashlib
import json
import logging
import os
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import click
import requests
import yaml
from kedro import __version__ as KEDRO_VERSION
from kedro.framework.cli.cli import KedroCLI
from kedro.framework.cli.hooks import cli_hook_impl
from kedro.framework.hooks import hook_impl
from kedro.framework.project import PACKAGE_NAME, pipelines
from kedro.framework.startup import ProjectMetadata
from kedro.io.data_catalog import DataCatalog
from kedro.pipeline import Pipeline

from kedro_telemetry import __version__ as TELEMETRY_VERSION
from kedro_telemetry.masking import _get_cli_structure, _mask_kedro_cli

HEAP_APPID_PROD = "2388822444"
HEAP_ENDPOINT = "https://heapanalytics.com/api/track"
HEAP_HEADERS = {"Content-Type": "application/json"}
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

logger = logging.getLogger(__name__)


def _hash(string: str) -> str:
    return hashlib.sha512(bytes(string, encoding="utf8")).hexdigest()


def _get_hashed_username():
    try:
        username = getpass.getuser()
        return _hash(username)
    except Exception as exc:
        logger.warning(
            "Something went wrong with getting the username. Exception: %s",
            exc,
        )
        return ""


class KedroTelemetryCLIHooks:
    """Hook to send CLI command data to Heap"""

    @cli_hook_impl
    def before_command_run(
        self, project_metadata: ProjectMetadata, command_args: List[str]
    ):
        """Hook implementation to send command run data to Heap"""
        try:
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
                logger.debug(
                    "Kedro-Telemetry is installed, but you have opted out of "
                    "sharing usage analytics so none will be collected.",
                )
                return

            logger.debug("You have opted into product usage analytics.")

            hashed_username = _get_hashed_username()
            project_properties = _get_project_properties(hashed_username)
            cli_properties = _format_user_cli_data(
                project_properties, masked_command_args
            )

            _send_heap_event(
                event_name=f"Command run: {main_command}",
                identity=hashed_username,
                properties=cli_properties,
            )

            # send generic event too, so it's easier in data processing
            generic_properties = deepcopy(cli_properties)
            generic_properties["main_command"] = main_command
            _send_heap_event(
                event_name="CLI command",
                identity=hashed_username,
                properties=generic_properties,
            )
        except Exception as exc:
            logger.warning(
                "Something went wrong in hook implementation to send command run data to Heap. "
                "Exception: %s",
                exc,
            )


class KedroTelemetryProjectHooks:
    """Hook to send project statistics data to Heap"""

    @hook_impl
    def after_context_created(self, context):
        """Hook implementation to send project statistics data to Heap"""
        consent = _check_for_telemetry_consent(context.project_path)
        if not consent:
            logger.debug(
                "Kedro-Telemetry is installed, but you have opted out of "
                "sharing usage analytics so none will be collected.",
            )
            return

        logger.debug("You have opted into product usage analytics.")

        catalog = context.catalog
        default_pipeline = pipelines.get("__default__")  # __default__
        hashed_username = _get_hashed_username()

        project_properties = _get_project_properties(hashed_username)

        project_statistics_properties = _format_project_statistics_data(
            project_properties, catalog, default_pipeline, pipelines
        )
        _send_heap_event(
            event_name="Kedro Project Statistics",
            identity=hashed_username,
            properties=project_statistics_properties,
        )


def _get_project_properties(hashed_username: str) -> Dict:
    hashed_package_name = _hash(PACKAGE_NAME) if PACKAGE_NAME else "undefined"

    return {
        "username": hashed_username,
        "package_name": hashed_package_name,
        "project_version": KEDRO_VERSION,
        "telemetry_version": TELEMETRY_VERSION,
        "python_version": sys.version,
        "os": sys.platform,
    }


def _format_user_cli_data(
    properties: dict,
    command_args: List[str],
):
    """Add format CLI command data to send to Heap."""
    cli_properties = properties.copy()
    cli_properties["command"] = (
        f"kedro {' '.join(command_args)}" if command_args else "kedro"
    )
    return cli_properties


def _format_project_statistics_data(
    properties: dict,
    catalog: DataCatalog,
    default_pipeline: Pipeline,
    project_pipelines: dict,
):
    """Add project statistics to send to Heap."""
    project_statistics_properties = properties.copy()
    project_statistics_properties["number_of_datasets"] = len(
        catalog.datasets.__dict__.keys()
    )
    project_statistics_properties["number_of_nodes"] = (
        len(default_pipeline.nodes) if default_pipeline else None
    )
    project_statistics_properties["number_of_pipelines"] = len(project_pipelines.keys())
    return project_statistics_properties


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
        "event": event_name,
        "timestamp": datetime.now().strftime(TIMESTAMP_FORMAT),
        "properties": properties or {},
    }
    if identity:
        data["identity"] = identity

    try:
        resp = requests.post(
            url=HEAP_ENDPOINT, headers=HEAP_HEADERS, data=json.dumps(data), timeout=10
        )
        if resp.status_code != 200:  # noqa: PLR2004
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
            click.secho(
                "You have opted out of product usage analytics, so none will be collected.",
                fg="green",
            )
            yaml.dump({"consent": False}, telemetry_file)
            return False
    except Exception as exc:
        logger.warning(
            "Failed to confirm consent. No data was sent to Heap. Exception: %s",
            exc,
        )
        return False


cli_hooks = KedroTelemetryCLIHooks()
project_hooks = KedroTelemetryProjectHooks()
