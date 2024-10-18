"""Kedro Telemetry plugin for collecting Kedro usage data."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

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
from kedro_telemetry.masking import _mask_kedro_cli

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
_SKIP_TELEMETRY_ENV_VAR_KEYS = (
    "DO_NOT_TRACK",
    "KEDRO_DISABLE_TELEMETRY",
)
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
CONFIG_FILENAME = "telemetry.toml"
PYPROJECT_CONFIG_NAME = "pyproject.toml"
UNDEFINED_PACKAGE_NAME = "undefined_package_name"
MISSING_USER_IDENTITY = "missing_user_identity"

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
        logging.debug(f"Failed to retrieve UUID: {e}")
        return ""


def _get_or_create_project_id(pyproject_path: Path) -> str | None:
    """
    Reads a project id from a configuration file or generates and saves a new one if not present.
    Returns None if configuration file does not exist or does not relate to Kedro.
    """
    try:
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
    except OSError as exc:
        logging.debug(f"Failed to read the file: {str(pyproject_path)}.\n{str(exc)}")
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
        logging.debug(f"Failed to create UUID: {e}")
        return ""


class KedroTelemetryHook:
    """Hook to send CLI command data to Heap"""

    def __init__(self):
        self._consent = None
        self._sent = False
        self._event_properties = None
        self._project_path = None
        self._user_uuid = None

    @cli_hook_impl
    def before_command_run(
        self, project_metadata: ProjectMetadata, command_args: list[str]
    ):
        """Hook implementation to send command run data to Heap"""

        project_path = project_metadata.project_path if project_metadata else None

        self._consent = _check_for_telemetry_consent(project_path)
        if not self._consent:
            return

        # get KedroCLI and its structure from actual project root
        cli = KedroCLI(project_path=project_path if project_path else Path.cwd())
        masked_command_args = _mask_kedro_cli(cli, command_args=command_args)

        self._user_uuid = _get_or_create_uuid()

        event_properties = _get_project_properties(self._user_uuid, project_path)
        event_properties["command"] = (
            f"kedro {' '.join(masked_command_args)}" if masked_command_args else "kedro"
        )
        event_properties["main_command"] = (
            masked_command_args[0] if masked_command_args else "kedro"
        )

        self._event_properties = event_properties

    @cli_hook_impl
    def after_command_run(self):
        if self._consent and not self._sent:
            self._send_telemetry_heap_event("CLI command")

    @hook_impl
    def after_context_created(self, context):
        """Hook implementation to read metadata"""

        self._consent = _check_for_telemetry_consent(context.project_path)
        self._project_path = context.project_path

    @hook_impl
    def after_catalog_created(self, catalog):
        """Hook implementation to send project statistics data to Heap"""

        if self._consent is False:
            return

        default_pipeline = pipelines.get("__default__")  # __default__

        if not self._user_uuid:
            self._user_uuid = _get_or_create_uuid()

        if not self._event_properties:
            self._event_properties = _get_project_properties(
                self._user_uuid, self._project_path
            )

        project_properties = _format_project_statistics_data(
            catalog, default_pipeline, pipelines
        )
        self._event_properties.update(project_properties)

        self._send_telemetry_heap_event("Kedro Project Statistics")

    def _send_telemetry_heap_event(self, event_name: str):
        """Hook implementation to send command run data to Heap"""

        logger.info(
            "Kedro is sending anonymous usage data with the sole purpose of improving the product. "
            "No personal data or IP addresses are stored on our side. "
            "If you want to opt out, set the `KEDRO_DISABLE_TELEMETRY` or `DO_NOT_TRACK` environment variables, "
            "or create a `.telemetry` file in the current working directory with the contents `consent: false`. "
            "Read more at https://docs.kedro.org/en/stable/configuration/telemetry.html"
        )

        try:
            _send_heap_event(
                event_name=event_name,
                identity=self._user_uuid if self._user_uuid else MISSING_USER_IDENTITY,
                properties=self._event_properties,
            )
            self._sent = True
        except Exception as exc:
            logger.debug(
                "Something went wrong in hook implementation to send command run data to Heap. "
                "Exception: %s",
                exc,
            )


def _is_known_ci_env(known_ci_env_var_keys: set[str]):
    # Most CI tools will set the CI environment variable to true
    if os.getenv("CI") == "true":
        return True
    # Not all CI tools follow this convention, we can check through those that don't
    return any(os.getenv(key) for key in known_ci_env_var_keys)


def _get_project_properties(user_uuid: str, project_path: Path | None) -> dict:
    if project_path:
        pyproject_path = project_path / PYPROJECT_CONFIG_NAME
        project_id = _get_or_create_project_id(pyproject_path)
        package_name = PACKAGE_NAME or UNDEFINED_PACKAGE_NAME
        hashed_project_id = (
            _hash(f"{project_id}{package_name}") if project_id is not None else None
        )
    else:
        hashed_project_id = None

    properties = {
        "username": user_uuid,
        "project_id": hashed_project_id,
        "project_version": KEDRO_VERSION,
        "telemetry_version": TELEMETRY_VERSION,
        "python_version": sys.version,
        "os": sys.platform,
        "is_ci_env": _is_known_ci_env(KNOWN_CI_ENV_VAR_KEYS),
    }

    if project_path:
        properties = _add_tool_properties(properties, pyproject_path)

    return properties


def _format_project_statistics_data(
    catalog: DataCatalog,
    default_pipeline: Pipeline,
    project_pipelines: dict,
):
    """Add project statistics to send to Heap."""
    project_statistics_properties = {}
    project_statistics_properties["number_of_datasets"] = sum(
        1
        for c in catalog.list()
        if not c.startswith("parameters") and not c.startswith("params:")
    )
    project_statistics_properties["number_of_nodes"] = (
        len(default_pipeline.nodes) if default_pipeline else None  # type: ignore
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
        "identity": identity,
    }

    try:
        resp = requests.post(
            url=HEAP_ENDPOINT, headers=HEAP_HEADERS, data=json.dumps(data), timeout=10
        )
        if resp.status_code != 200:  # noqa: PLR2004
            logger.debug(
                "Failed to send data to Heap. Response code returned: %s, Response reason: %s",
                resp.status_code,
                resp.reason,
            )
    except requests.exceptions.RequestException as exc:
        logger.debug(
            "Failed to send data to Heap. Exception of type '%s' was raised.",
            type(exc).__name__,
        )


def _check_for_telemetry_consent(project_path: Path | None) -> bool:
    """
    Use telemetry consent from ".telemetry" file if it exists and has a valid format.
    Telemetry is considered as opt-in otherwise.
    """

    for env_var in _SKIP_TELEMETRY_ENV_VAR_KEYS:
        if os.environ.get(env_var):
            return False

    if project_path:
        telemetry_file_path = project_path / ".telemetry"
        if telemetry_file_path.exists():
            with open(telemetry_file_path, encoding="utf-8") as telemetry_file:
                telemetry = yaml.safe_load(telemetry_file)
                if _is_valid_syntax(telemetry):
                    return telemetry["consent"]
    return True


def _is_valid_syntax(telemetry: Any) -> bool:
    return isinstance(telemetry, dict) and isinstance(
        telemetry.get("consent", None), bool
    )


telemetry_hook = KedroTelemetryHook()
