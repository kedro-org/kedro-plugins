#!/usr/bin/env python3
"""
CircleCI pipeline to check if it needs to trigger a release
"""

import os
import sys

import requests
from requests.structures import CaseInsensitiveDict
from utils.check_no_version_pypi import check_no_version_pypi
from utils.package_version import get_package_version

PACKAGE_PATHS = (
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

    resp = requests.post(circle_endpoint, headers=headers, json=payload, timeout=10)
    return resp


if __name__ == "__main__":
    """Trigger the CircleCI Release Process"""
    from pathlib import Path

    # Personal API Tokens - https://circleci.com/docs/managing-api-tokens
    CIRCLE_RELEASE_TOKEN = os.environ.get("CIRCLE_RELEASE_TOKEN")
    if not CIRCLE_RELEASE_TOKEN:
        raise ValueError("CIRCLE_RELEASE_TOKEN is not defined as envionrmnet variable.")

    base_path = Path()
    # Loop for all 4 repositories
    for package_path in PACKAGE_PATHS:
        package_name, _ = package_path.split("/")
        package_version = get_package_version(base_path, package_path)
        pypi_endpoint = f"https://pypi.org/pypi/{package_name}/{package_version}/json/"
        circleci_endpoint = (
            f"https://circleci.com/api/v2/project/{PROJECT_SLUG}/pipeline"
        )
        payload = {
            "branch": CIRCLE_BRANCH,
            "parameters": {
                "release_package": package_name,
                "release_version": package_version,
            },
        }

        print(package_name, package_version)
        if check_no_version_pypi(pypi_endpoint, package_name, package_version):
            res = circleci_release(
                PROJECT_SLUG, payload, circleci_endpoint, CIRCLE_RELEASE_TOKEN
            )
            print(f"Status Code: {resp.status_code}")
            if resp.status_code == 201:
                print("Creating CircleCI Pipeline successfully")
            else:
                print("Failed to create CircleCI Pipeline")
            print(resp.content)
            if resp.status_code != 201:
                sys.exit(1)
