#!/usr/bin/env python3

import os

import requests
from requests.structures import CaseInsensitiveDict

from utils.check_no_version_pypi import check_no_version_pypi
from utils.package_version import get_package_version


PACKAGE_PATH = (
    "kedro-datasets/kedro_datasets",
    "kedro-telemetry/kedro_telemetry",
    "kedro-airflow/kedro_airflow",
    "kedro-docker/kedro_docker",
)
PROJECT_SLUG = "github/kedro-org/kedro-plugins"
# CIRCLE_BRANCH = "feat/cicd-auto-release"
CIRCLE_BRANCH = os.environ.get("CIRCLE_BRANCH")

def circleci_release(project_slug, payload, circle_endpoint, circle_release_token):
    """Trigging the CircleCI Release Pipeline"""
    # See https://circleci.com/docs/2.0/api-developers-guide
    print("Starting the CircleCI Release Pipeline")
    CIRCLE_ENDPOINT = f"https://circleci.com/api/v2/project/{project_slug}/pipeline"

    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Circle-Token"] = circle_release_token

    resp = requests.post(circle_endpoint, headers=headers, json=payload)
    if resp.status_code == 201:
        print("Creating CircleCI Pipeline successfully")
    else:
        print("Failed to create CircleCI Pipeline")
    return resp


if __name__ == "__main__":
    """Trigger the CircleCI Release Process"""

    CIRCLE_RELEASE_TOKEN = os.environ.get("CIRCLE_RELEASE_TOKEN")
    if not CIRCLE_RELEASE_TOKEN:
        raise ValueError("CIRCLE_RELEASE_TOKEN is not defined as envionrmnet variable.")



    base_path = "/Users/Nok_Lam_Chan/GitHub/kedro_plugins_release"
    package_paths = PACKAGE_PATH
    # Loop for all 4 repositories
    for package_path in package_paths:
        PACKAGE_NAME, _ = package_path.split("/")
        PACKAGE_VERSION = get_package_version(base_path, package_path)
        PYPI_ENDPOINT = f"https://pypi.org/pypi/{PACKAGE_NAME}/{PACKAGE_VERSION}/json/"
        CIRCLE_ENDPOINT = f"https://circleci.com/api/v2/project/{PROJECT_SLUG}/pipeline"
        PAYLOAD = {
            "branch": CIRCLE_BRANCH,
            "parameters": {
                "release_package": PACKAGE_NAME,
                "release_version": PACKAGE_VERSION,
            },
        }

        print(PACKAGE_NAME, PACKAGE_VERSION)
        if check_no_version_pypi(PYPI_ENDPOINT, PACKAGE_NAME, PACKAGE_VERSION):
            circleci_release(
                PROJECT_SLUG, PAYLOAD, CIRCLE_ENDPOINT, CIRCLE_RELEASE_TOKEN
            )
