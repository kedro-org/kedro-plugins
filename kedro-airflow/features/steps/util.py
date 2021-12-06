"""Common functions for e2e testing.
"""

import tempfile
import urllib.request
import venv
from pathlib import Path


def download_url(url: str) -> str:
    """
    Download and return decoded contents of url

    Args:
        url: Url that is to be read.

    Returns:
        Decoded data fetched from url.

    """
    with urllib.request.urlopen(url) as http_response_obj:
        return http_response_obj.read().decode()


def create_new_venv() -> Path:
    """Create a new venv.

    Returns:
        path to created venv
    """
    # Create venv
    venv_dir = Path(tempfile.mkdtemp())
    venv.main([str(venv_dir)])
    return venv_dir
