"""A PEP 517 interface port from setuptools
This is not a formal definition! Just a "taste" of the module.
"""

import io
import os
import sys
import tokenize
import shutil
import contextlib
import tempfile
import warnings
from pathlib import Path
from typing import Optional

import setuptools
import distutils

from pkg_resources import parse_requirements

__all__ = [
    "get_requires_for_build_sdist",
    "get_requires_for_build_wheel",
    "prepare_metadata_for_build_wheel",
    "build_wheel",
    "build_sdist",
    "build_editable",
    "SetupRequirementsError",
]


class SetupRequirementsError(BaseException):
    def __init__(self, specifiers):
        self.specifiers = specifiers


class Distribution(setuptools.dist.Distribution):
    def fetch_build_eggs(self, specifiers):
        specifier_list = list(map(str, parse_requirements(specifiers)))

        raise SetupRequirementsError(specifier_list)

    @classmethod
    @contextlib.contextmanager
    def patch(cls):
        """
        Replace
        distutils.dist.Distribution with this class
        for the duration of this context.
        """
        orig = distutils.core.Distribution
        distutils.core.Distribution = cls
        try:
            yield
        finally:
            distutils.core.Distribution = orig


@contextlib.contextmanager
def no_install_setup_requires():
    """Temporarily disable installing setup_requires

    Under PEP 517, the backend reports build dependencies to the frontend,
    and the frontend is responsible for ensuring they're installed.
    So pyproject-toml (acting as a backend) should not try to install them.
    """
    orig = setuptools._install_setup_requires
    setuptools._install_setup_requires = lambda attrs: None
    try:
        yield
    finally:
        setuptools._install_setup_requires = orig


@contextlib.contextmanager
def suppress_known_deprecation():
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', 'setup.py install is deprecated')
        yield


def _get_immediate_subdirectories(a_dir):
    return [
        name for name in os.listdir(a_dir) if os.path.isdir(os.path.join(a_dir, name))
    ]


def _file_with_extension(directory, extension):
    matching = (f for f in os.listdir(directory) if f.endswith(extension))
    (file,) = matching
    return file


def _open_setup_script(setup_script):
    if not os.path.exists(setup_script):
        # Supply a default setup.py
        return io.StringIO(u"from pyproject_toml import setup; setup()")

    return getattr(tokenize, "open", open)(setup_script)


def _fix_config(config_settings):
    config_settings = config_settings or {}
    config_settings.setdefault("--global-option", [])
    return config_settings


class _BuildBackend:
    def _get_build_requires(self, config_settings, requirements):
        config_settings = _fix_config(config_settings)

        sys.argv = sys.argv[:1] + ["egg_info"] + config_settings["--global-option"]
        try:
            with Distribution.patch():
                self.run_setup()
        except SetupRequirementsError as e:
            requirements += e.specifiers

        return requirements

    def run_setup(self, setup_script="setup.py"):
        # Note that we can reuse our build directory between calls
        # Correctness comes first, then optimization later
        __file__ = setup_script
        __name__ = "__main__"

        with _open_setup_script(__file__) as f:
            code = f.read().replace(r"\r\n", r"\n")

        exec(compile(code, __file__, "exec"), locals())

    def get_requires_for_build_wheel(self, config_settings=None):
        config_settings = _fix_config(config_settings)
        return self._get_build_requires(config_settings, requirements=["wheel"])

    def get_requires_for_build_sdist(self, config_settings=None):
        config_settings = _fix_config(config_settings)
        return self._get_build_requires(config_settings, requirements=[])

    def prepare_metadata_for_build_wheel(
        self, metadata_directory, config_settings=None
    ):
        sys.argv = sys.argv[:1] + ["dist_info", "--egg-base", metadata_directory]
        with no_install_setup_requires():
            self.run_setup()

        dist_info_directory = metadata_directory
        while True:
            dist_infos = [
                f for f in os.listdir(dist_info_directory) if f.endswith(".dist-info")
            ]

            if (
                len(dist_infos) == 0
                and len(_get_immediate_subdirectories(dist_info_directory)) == 1
            ):

                dist_info_directory = os.path.join(
                    dist_info_directory, os.listdir(dist_info_directory)[0]
                )
                continue

            assert len(dist_infos) == 1
            break

        # PEP 517 requires that the .dist-info directory be placed in the
        # metadata_directory. To comply, we MUST copy the directory to the root
        if dist_info_directory != metadata_directory:
            shutil.move(
                os.path.join(dist_info_directory, dist_infos[0]), metadata_directory
            )
            shutil.rmtree(dist_info_directory, ignore_errors=True)

        return dist_infos[0]

    def _build_with_temp_dir(
        self, setup_command, result_extension, result_directory, config_settings
    ):
        config_settings = _fix_config(config_settings)
        result_directory = os.path.abspath(result_directory)

        # Build in a temporary directory, then copy to the target.
        os.makedirs(result_directory, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=result_directory) as tmp_dist_dir:
            sys.argv = (
                sys.argv[:1]
                + setup_command
                + ["--dist-dir", tmp_dist_dir]
                + config_settings["--global-option"]
            )
            with no_install_setup_requires():
                self.run_setup()

            result_basename = _file_with_extension(tmp_dist_dir, result_extension)
            result_path = os.path.join(result_directory, result_basename)
            if os.path.exists(result_path):
                # os.rename will fail overwriting on non-Unix.
                os.remove(result_path)
            os.rename(os.path.join(tmp_dist_dir, result_basename), result_path)

        return result_basename

    def build_wheel(
        self, wheel_directory, config_settings=None, metadata_directory=None
    ):
        return self._build_with_temp_dir(
            ["bdist_wheel"], ".whl", wheel_directory, config_settings
        )

    def build_sdist(self, sdist_directory, config_settings=None):
        return self._build_with_temp_dir(
            ["sdist", "--formats", "gztar"], ".tar.gz", sdist_directory, config_settings
        )

    def _get_dist_info_dir(self, metadata_directory: Optional[str]) -> Optional[str]:
        if not metadata_directory:
            return None
        dist_info_candidates = list(Path(metadata_directory).glob("*.dist-info"))
        assert len(dist_info_candidates) <= 1
        return str(dist_info_candidates[0]) if dist_info_candidates else None

    def build_editable(
        self, wheel_directory, config_settings=None, metadata_directory=None
    ):
        # XXX can or should we hide our editable_wheel command normally?
        info_dir = self._get_dist_info_dir(metadata_directory)
        opts = ["--dist-info-dir", info_dir] if info_dir else []
        cmd = ["editable_wheel", *opts]
        with suppress_known_deprecation():
            return self._build_with_temp_dir(
                cmd, ".whl", wheel_directory, config_settings
            )

    def get_requires_for_build_editable(self, config_settings=None):
        return self.get_requires_for_build_wheel(config_settings)

    def prepare_metadata_for_build_editable(self, metadata_directory,
                                            config_settings=None):
        return self.prepare_metadata_for_build_wheel(
            metadata_directory, config_settings
        )

# The primary backend
_BACKEND = _BuildBackend()

get_requires_for_build_wheel = _BACKEND.get_requires_for_build_wheel
get_requires_for_build_sdist = _BACKEND.get_requires_for_build_sdist
prepare_metadata_for_build_wheel = _BACKEND.prepare_metadata_for_build_wheel
build_wheel = _BACKEND.build_wheel
build_sdist = _BACKEND.build_sdist
build_editable = _BACKEND.build_editable
