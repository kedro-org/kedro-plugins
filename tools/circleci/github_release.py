#!/usr/bin/env python3
import os
import sys

import requests
from requests.structures import CaseInsensitiveDict

GITHUB_USER = "kedro-org"
GITHUB_REPO = "kedro-plugins"
# On GitHub select "Settings" > "Developer Setting" -> "Personal access Token""
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

    github_endpoint = (
        f"https://api.github.com/repos/{github_user}/{github_repo}/releases"
    )
    payload = {
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
    resp = requests.post(github_endpoint, headers=headers, json=payload, timeout=10)
    if resp.status_code == 200:
        print("Create GitHub release successfully")
        print(resp.content)
    else:
        print("Failed to create Github release")
        print(resp.content)
    return resp


if __name__ == "__main__":
    package_name = sys.argv[1]
    package_version = sys.argv[2]
    res = github_release(package_name, package_version)
