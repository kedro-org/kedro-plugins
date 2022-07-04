import requests
from requests.structures import CaseInsensitiveDict

import os

GITHUB_USER = os.envron.get("GITHUB_USER")
GITHUB_REPO = os.envron.get("GITHUB_REPO")
GITHUB_TAGGING_TOKEN = os.envron.get("GITHUB_TAGGING_TOKEN")


def release(package_name, version, github_user, github_repo, github_tagging_token):
    """ Trigger the GitHub Release"""
    print("release")

    """ The release of GitHub artifacts and tagging"""


    GITHUB_ENDPOINT = f"https://api.github.com/repos/{github_user}/{github_repo}/releases"
    PAYLOAD = {
    "tag_name": f"{package_name}-{version}",  # kedro-datasets 0.0.1
    "target_commitish": "main",
    "name": f"{version}",
    "body": f"Release {version}",
    "draft": False,
    "prerelease": False
}

    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Authorization"] = f"token {github_tagging_token}"
    return
    resp = requests.post(GITHUB_ENDPOINT, headers=headers, json=PAYLOAD)
    print(resp.status_code)
    return resp

if __name__ == "__main__":
    res = release(PACKAGE_NAME, VERSION, GITHUB_USER, GITHUB_REPO, GITHUB_TAGGING_TOKEN)