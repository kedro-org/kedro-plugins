"""Source for setup.cfg configuration files via ini config."""
from __future__ import annotations

from pathlib import Path

from .ini import IniConfig


class SetupConfig(IniConfig):

    """Source for setup.cfg configuration files via ini config."""

    name: str = "setup.cfg"
    description = """
    Setuptools allows using configuration files (usually setup.cfg) to define a
    package`s metadata and other options that are normally supplied to the setup()
    function (declarative config)."""

    def __init__(self, working_directory: Path, base_table: str) -> None:
        """Initialize the setup.cfg file as a special INI file.

        Args:
            working_directory: working directory to find the file recursively.
            base_table: base table to read from.
        """
        super().__init__(working_directory, "setup.cfg", base_table)
