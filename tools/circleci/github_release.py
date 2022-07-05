#!/usr/bin/env python3
import os

import requests
from requests.structures import CaseInsensitiveDict

GITHUB_USER = os.environ.get("GITHUB_USER")
GITHUB_REPO = os.environ.get("GITHUB_REPO")
GITHUB_TAGGING_TOKEN = os.environ.get("GITHUB_TAGGING_TOKEN")


def github_release(
    package_name,
    version,
    github_user=GITHUB_USER,
    github_repo=GITHUB_REPO,
    github_tagging_token=GITHUB_TAGGING_TOKEN,
):
    """Trigger the GitHub Release to create artifacts and tags"""
    print("Starting GitHub Release")

    GITHUB_ENDPOINT = (
        f"https://api.github.com/repos/{github_user}/{github_repo}/releases"
    )
    PAYLOAD = {
        "tag_name": f"{package_name}-{version}",  # kedro-datasets 0.0.1
        "target_commitish": "main",
        "name": f"{version}",
        "body": f"Release {version}",
        "draft": False,
        "prerelease": False,
    }

    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Authorization"] = f"token {github_tagging_token}"
    resp = requests.post(GITHUB_ENDPOINT, headers=headers, json=PAYLOAD)
    print(resp.status_code)
    print(resp.content)
    return resp


if __name__ == "__main__":
    import sys

    package_name = sys.argv[0]
    package_version = sys.argv[1]
    res = github_release(package_name, package_version)
