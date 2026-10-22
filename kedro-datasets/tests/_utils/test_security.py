"""Tests for utility functions in security.py."""

import pytest
from kedro.io.core import DatasetError

from kedro_datasets._utils.security import validate_sub_path

BASE = "/data/base"


class TestValidateSubPath:
    @pytest.mark.parametrize(
        "sub_path",
        [
            "file",
            "a/b",
            "a\\b",
            "a/../b",
            "...data",
            "file:name",
            "dir/C:foo",
            "a:1.csv",
            "C:secrets",
        ],
    )
    def test_valid_paths(self, sub_path):
        validate_sub_path(sub_path, BASE)

    @pytest.mark.parametrize(
        "sub_path", ["/etc/passwd", "\\secrets", "\\\\server\\share\\secrets"]
    )
    def test_absolute_paths_rejected(self, sub_path):
        with pytest.raises(
            DatasetError,
            match=r"^Path '.*' must be a relative path inside the dataset "
            r"directory '/data/base'\.$",
        ):
            validate_sub_path(sub_path, BASE)

    @pytest.mark.parametrize(
        "sub_path", ["..", "../secrets", "a/../../b", "a\\..\\..\\b"]
    )
    def test_traversal_rejected(self, sub_path):
        with pytest.raises(
            DatasetError,
            match=r"^Path '.*' resolves to '.*' which is outside the dataset "
            r"directory '/data/base'\.$",
        ):
            validate_sub_path(sub_path, BASE)
