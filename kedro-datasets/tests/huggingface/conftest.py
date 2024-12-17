"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""
import sys
import pytest

if sys.version_info >= (3, 13):
    pytest.skip("Transformers is not available in Python 3.13 yet")
