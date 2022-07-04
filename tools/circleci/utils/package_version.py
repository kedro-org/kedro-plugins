#!/usr/bin/env python3
"""
Get version of Kedro
"""

import re
from pathlib import Path

VERSION_MATCHSTR = r'\s*__version__\s*=\s*"(\d+\.\d+\.\d+)"'


def get_package_version(base_path, package_path):
    init_file_path = Path(base_path) / package_path / "__init__.py"
    match_obj = re.search(VERSION_MATCHSTR, Path(init_file_path).read_text())
    return match_obj.group(1)


if __name__ == "__main__":
    from pathlib import Path

    base_path = Path()
    package_path = "kedro-datasets/kedro_datasets"
    print(get_package_version(base_path, package_path))
