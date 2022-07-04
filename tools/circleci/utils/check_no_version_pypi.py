import requests
import sys


def check_no_version_pypi(pypi_endpoint, package_name, package_version):
    print("Check if package is on pypi")
    response = requests.get(pypi_endpoint)
    if response.status_code == 404:
        # Not exist on Pypi - do release
        print(f"Starting the release of {package_name} {package_version}")
        return True

    else:
        print(f"Skipped: {package_name} {package_version} already exists on PyPI")
        # sys.exit(0)  # This will terminate the CI job
        return False

