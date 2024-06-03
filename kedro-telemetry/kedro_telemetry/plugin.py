"""Kedro Telemetry plugin for collecting Kedro usage data."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import uuid
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import click
import requests
import toml
import yaml
from appdirs import user_config_dir
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
KNOWN_CI_ENV_VAR_KEYS = {
    "GITLAB_CI",  # https://docs.gitlab.com/ee/ci/variables/predefined_variables.html
    "GITHUB_ACTION",  # https://docs.github.com/en/actions/learn-github-actions/variables#default-environment-variables
    "BITBUCKET_BUILD_NUMBER",  # https://support.atlassian.com/bitbucket-cloud/docs/variables-and-secrets/
    "JENKINS_URL",  # https://www.jenkins.io/doc/book/pipeline/jenkinsfile/#using-environment-variables
    "CODEBUILD_BUILD_ID",  # https://docs.aws.amazon.com/codebuild/latest/userguide/build-env-ref-env-vars.html
    "CIRCLECI",  # https://circleci.com/docs/variables/#built-in-environment-variables
    "TRAVIS",  # https://docs.travis-ci.com/user/environment-variables/#default-environment-variables
    "BUILDKITE",  # https://buildkite.com/docs/pipelines/environment-variables
}
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
CONFIG_FILENAME = "telemetry.toml"
PYPROJECT_CONFIG_NAME = "pyproject.toml"
UNDEFINED_PACKAGE_NAME = "undefined_package_name"

logger = logging.getLogger(__name__)


def _hash(string: str) -> str:
    return hashlib.sha512(bytes(string, encoding="utf8")).hexdigest()


def _get_or_create_uuid() -> str:
    """
    Reads a UUID from a configuration file or generates and saves a new one if not present.
    """
    config_path = user_config_dir("kedro")
    full_path = os.path.join(config_path, CONFIG_FILENAME)

    try:
        if os.path.exists(full_path):
            with open(full_path) as f:
                config = toml.load(f)

                if "telemetry" in config and "uuid" in config["telemetry"]:
                    return uuid.UUID(config["telemetry"]["uuid"]).hex

        # Generate a new UUID and save it to the config file
        new_uuid = _generate_new_uuid(full_path)

        return new_uuid

    except Exception as e:
        logging.error(f"Failed to retrieve UUID: {e}")
        return ""


def _get_or_create_project_id(pyproject_path: Path) -> str | None:
    """
    Reads a project id from a configuration file or generates and saves a new one if not present.
    Returns None if configuration file does not exist or does not relate to Kedro.
    """
    if pyproject_path.exists():
        with open(pyproject_path, "r+") as file:
            pyproject_data = toml.load(file)

            # Check if pyproject related to kedro
            try:
                _ = pyproject_data["tool"]["kedro"]
                try:
                    project_id = pyproject_data["tool"]["kedro_telemetry"]["project_id"]
                except KeyError:
                    project_id = uuid.uuid4().hex
                    toml_string = (
                        f'\n[tool.kedro_telemetry]\nproject_id = "{project_id}"\n'
                    )
                    file.write(toml_string)
                return project_id
            except KeyError:
                logging.debug(
                    f"Failed to retrieve project id or save project id: "
                    f"{str(pyproject_path)} does not contain a [tool.kedro] section"
                )
                return None

    logging.debug(
        f"Failed to retrieve project id or save project id: {str(pyproject_path)} does not exist"
    )
    return None


def _add_tool_properties(
    properties: dict[str, Any], pyproject_path: Path
) -> dict[str, Any]:
    """
    Extends project properties with tool's properties.
    """
    if pyproject_path.exists():
        with open(pyproject_path) as file:
            pyproject_data = toml.load(file)

        try:
            tool_kedro = pyproject_data["tool"]["kedro"]
            if "tools" in tool_kedro:
                properties["tools"] = ", ".join(tool_kedro["tools"])
            if "example_pipeline" in tool_kedro:
                properties["example_pipeline"] = tool_kedro["example_pipeline"]
        except KeyError:
            pass

    return properties


def _generate_new_uuid(full_path: str) -> str:
    try:
        config: dict[str, dict[str, Any]] = {"telemetry": {}}
        new_uuid = uuid.uuid4().hex
        config["telemetry"]["uuid"] = new_uuid

        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "w") as f:
            toml.dump(config, f)

        return new_uuid
    except Exception as e:
        logging.error(f"Failed to create UUID: {e}")
        return ""


class KedroTelemetryCLIHooks:
    """Hook to send CLI command data to Heap"""

    @cli_hook_impl
    def before_command_run(
        self, project_metadata: ProjectMetadata, command_args: list[str]
    ):
        """Hook implementation to send command run data to Heap"""
        try:
            if not project_metadata:  # in package mode
                return

            consent = _check_for_telemetry_consent(project_metadata.project_path)
            if not consent:
                logger.debug(
                    "Kedro-Telemetry is installed, but you have opted out of "
                    "sharing usage analytics so none will be collected.",
                )
                return

            # get KedroCLI and its structure from actual project root
            cli = KedroCLI(project_path=project_metadata.project_path)
            cli_struct = _get_cli_structure(cli_obj=cli, get_help=False)
            masked_command_args = _mask_kedro_cli(
                cli_struct=cli_struct, command_args=command_args
            )
            main_command = masked_command_args[0] if masked_command_args else "kedro"

            logger.debug("You have opted into product usage analytics.")
            user_uuid = _get_or_create_uuid()
            project_properties = _get_project_properties(
                user_uuid, project_metadata.project_path / PYPROJECT_CONFIG_NAME
            )
            cli_properties = _format_user_cli_data(
                project_properties, masked_command_args
            )

            _send_heap_event(
                event_name=f"Command run: {main_command}",
                identity=user_uuid,
                properties=cli_properties,
            )

            # send generic event too, so it's easier in data processing
            generic_properties = deepcopy(cli_properties)
            generic_properties["main_command"] = main_command
            _send_heap_event(
                event_name="CLI command",
                identity=user_uuid,
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
        self.consent = _check_for_telemetry_consent(context.project_path)
        self.project_path = context.project_path

    @hook_impl
    def after_catalog_created(self, catalog):
        if not self.consent:
            logger.debug(
                "Kedro-Telemetry is installed, but you have opted out of "
                "sharing usage analytics so none will be collected.",
            )
            return

        logger.debug("You have opted into product usage analytics.")

        default_pipeline = pipelines.get("__default__")  # __default__
        user_uuid = _get_or_create_uuid()

        project_properties = _get_project_properties(
            user_uuid, self.project_path / PYPROJECT_CONFIG_NAME
        )

        project_statistics_properties = _format_project_statistics_data(
            project_properties, catalog, default_pipeline, pipelines
        )
        _send_heap_event(
            event_name="Kedro Project Statistics",
            identity=user_uuid,
            properties=project_statistics_properties,
        )


def _is_known_ci_env(known_ci_env_var_keys: set[str]):
    # Most CI tools will set the CI environment variable to true
    if os.getenv("CI") == "true":
        return True
    # Not all CI tools follow this convention, we can check through those that don't
    return any(os.getenv(key) for key in known_ci_env_var_keys)


def _get_project_properties(user_uuid: str, pyproject_path: Path) -> dict:
    project_id = _get_or_create_project_id(pyproject_path)
    package_name = PACKAGE_NAME or UNDEFINED_PACKAGE_NAME
    hashed_project_id = (
        _hash(f"{project_id}{package_name}") if project_id is not None else None
    )

    properties = {
        "username": user_uuid,
        "project_id": hashed_project_id,
        "project_version": KEDRO_VERSION,
        "telemetry_version": TELEMETRY_VERSION,
        "python_version": sys.version,
        "os": sys.platform,
        "is_ci_env": _is_known_ci_env(KNOWN_CI_ENV_VAR_KEYS),
    }

    properties = _add_tool_properties(properties, pyproject_path)

    return properties


def _format_user_cli_data(
    properties: dict,
    command_args: list[str],
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
    project_statistics_properties["number_of_datasets"] = sum(
        1
        for c in catalog.list()
        if not c.startswith("parameters") and not c.startswith("params:")
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
    event_name: str, identity: str, properties: dict[str, Any] | None = None
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
