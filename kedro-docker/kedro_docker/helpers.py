""" Utilities for use with click docker commands """

from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import sys
from collections.abc import Mapping, Sequence
from importlib import import_module
from itertools import chain
from pathlib import Path, PurePosixPath
from subprocess import DEVNULL, PIPE

from click import secho
from kedro.framework.cli.utils import KedroCliError

PYTHON_VERSION_FILE = ".python-version"
PIPELINES_ARG = "KEDRO_PIPELINES"

# CPython versions as written by uv or pyenv, e.g. 3.12, 3.12.4, cpython@3.12 or
# cpython-3.12.4-linux-x86_64-gnu
_CPYTHON_VERSION = re.compile(
    r"(?:cpython[@-]?|cp)?3\.(?P<minor>\d{1,2})(?:\.\d+)?(?:-\w+-\w+-\w+)?",
    re.IGNORECASE,
)


def check_docker_image_exists(image: str):
    """
    Check that the specified Docker image exists locally.

    Args:
        image: Docker image name.

    Raises:
        KedroCliError: If specified Docker image was not found.

    """
    command = ["docker", "images", "-q", image]
    res = subprocess.run(command, stdout=PIPE, stderr=DEVNULL, check=False)
    if not res.stdout:
        cmd = f"kedro docker build --image {image}"
        raise KedroCliError(
            f"Unable to find image `{image}` locally. Please build it first "
            f"by running:\n{cmd}"
        )


def _list_docker_volumes(host_root: str, container_root: str, volumes: Sequence[str]):
    """
    Generate volume mount options as ``docker run`` command expects it.

    Args:
        host_root: Path project root on the host.
        container_root: Path to project root in the container (e.g., `/home/kedro`).
        volumes: List of volumes to mount.

    Yields:
        Tuple[str]: Tuple of the form: ('-v', 'host_path:container_path')

    """
    host_root_path = Path(host_root).resolve()
    container_root_path = PurePosixPath(container_root)
    for volume in volumes:
        hpath = host_root_path / volume  # host path
        cpath = PurePosixPath(container_root_path) / volume  # container path
        yield "-v", str(hpath) + ":" + str(cpath)


def compose_docker_run_args(  # noqa: PLR0913
    host_root: str | None = None,
    container_root: str | None = None,
    mount_volumes: Sequence[str] | None = None,
    required_args: Sequence[tuple[str, str | None]] | None = None,
    optional_args: Sequence[tuple[str, str | None]] | None = None,
    user_args: Sequence[str] | None = None,
) -> list[str]:
    """
    Make a list of arguments for the docker command.

    Args:
        host_root: Path project root on the host. It must be provided
            if `mount_volumes` are specified, optional otherwise.
        container_root: Path to project root in the container
            (e.g., `/home/kedro/<repo_name>`). It must be
            provided if `mount_volumes` are specified, optional otherwise.
        mount_volumes: List of volumes to be mounted.
        required_args: List of required arguments.
        optional_args: List of optional arguments, these will be added if only
            not present in `user_args` list.
        user_args: List of arguments already specified by the user.
    Raises:
        KedroCliError: If `mount_volumes` are provided but either `host_root`
            or `container_root` are missing.

    Returns:
        List of arguments for the docker command.
    """

    mount_volumes = mount_volumes or []
    required_args = required_args or []
    optional_args = optional_args or []
    user_args = user_args or []
    split_user_args = {ua.split("=", 1)[0] for ua in user_args}

    def _add_args(
        name_: str, value_: str | None = None, force_: bool = False
    ) -> list[str]:
        """
        Add extra args to existing list of CLI args.
        Args:
            name_: Arg name to add.
            value_: Arg value to add, skipped if None.
            force_: Add the argument even if it's present in the current list of args.

        Returns:
            List containing the new args and (optionally) its value or an empty list
                if no values to be added.
        """
        if not force_ and name_ in split_user_args:
            return []
        return [name_] if value_ is None else [name_, value_]

    if mount_volumes:
        if not (host_root and container_root):
            raise KedroCliError(
                "Both `host_root` and `container_root` must "
                "be specified in `compose_docker_run_args` "
                "call if `mount_volumes` are provided."
            )
        vol_gen = _list_docker_volumes(host_root, container_root, mount_volumes)
        combined_args = list(chain.from_iterable(vol_gen))
    else:
        combined_args = []
    for arg_name, arg_value in required_args:
        combined_args += _add_args(arg_name, arg_value, True)
    for arg_name, arg_value in optional_args:
        combined_args += _add_args(arg_name, arg_value)
    return combined_args + list(user_args)


def make_container_name(image: str, suffix: str = "") -> str:
    """
    Make default container name for the given Docker image.

    Args:
        image: Docker image tag.
        suffix: Suffix to append to the container name.

    Returns:
        Docker container name.
    """
    name = re.sub(r"[^a-zA-Z0-9_.-]+", "-", image)
    if suffix:
        name += "-" + str(suffix)
    return name


def copy_template_files(
    project_path: Path,
    template_path: Path,
    template_files: Sequence[str],
    verbose: bool = False,
    arg_defaults: Mapping[str, str] | None = None,
):
    """
    If necessary copy files from a template directory into a project directory.

    Args:
        project_path: Destination path.
        template_path: Source path.
        template_files: Files to copy.
        verbose: Echo the names of any created files.
        arg_defaults: New default values for ``ARG NAME=value`` lines in the
            Dockerfile. Other files are copied as they are.

    """
    for file_ in template_files:
        dest_file = "Dockerfile" if file_.startswith("Dockerfile") else file_
        dest = project_path / dest_file
        if not dest.exists():
            src = template_path / file_
            if arg_defaults and dest_file == "Dockerfile":
                content = src.read_bytes().decode("utf-8")
                content = set_dockerfile_arg_defaults(content, arg_defaults)
                dest.write_bytes(content.encode("utf-8"))
            else:
                shutil.copyfile(str(src), str(dest))
            if verbose:
                secho(f"Creating `{dest}`")
        else:
            msg = f"{dest_file} already exists and won't be overwritten."
            secho(msg, fg="yellow")


def set_dockerfile_arg_defaults(content: str, arg_defaults: Mapping[str, str]) -> str:
    """
    Set the default values of ``ARG NAME=value`` lines in a Dockerfile.
    Names that the Dockerfile does not declare are ignored.

    Args:
        content: Dockerfile text.
        arg_defaults: Mapping of argument names to their new default values.

    Returns:
        Dockerfile text with the new default values.
    """
    for name, value in arg_defaults.items():
        pattern = rf"^(ARG[ \t]+{re.escape(name)}=)\S*"
        content = re.sub(
            pattern, lambda m: m.group(1) + value, content, flags=re.MULTILINE
        )
    return content


def dockerfile_declares_arg(dockerfile: Path, name: str) -> bool:
    """
    Check whether a Dockerfile has an ``ARG`` instruction for a build argument.

    Args:
        dockerfile: Path to the Dockerfile.
        name: Build argument name.

    Returns:
        True if the Dockerfile exists and declares the build argument.
    """
    if not dockerfile.is_file():
        return False
    content = dockerfile.read_bytes().decode("utf-8", errors="replace")
    pattern = rf"^[ \t]*(?i:ARG)[ \t]+(?:\S+[ \t]+)*{re.escape(name)}(?:=|\s|$)"
    return re.search(pattern, content, flags=re.MULTILINE) is not None


def uses_custom_dockerfile(docker_args: Sequence[str]) -> bool:
    """
    Check whether ``docker build`` arguments choose a Dockerfile with ``-f/--file``.

    Args:
        docker_args: Arguments for ``docker build``.

    Returns:
        True if another Dockerfile is used.
    """
    return any(
        arg == "--file" or arg.startswith("--file=") or arg.startswith("-f")
        for arg in docker_args
    )


def parse_python_version(text: str) -> str | None:
    """
    Get the ``3.Y`` Python version from the content of a ``.python-version`` file.
    As in uv and pyenv, only the first word of the first line that is
    neither blank nor a ``#`` comment is used.

    Args:
        text: Content of a ``.python-version`` file.

    Returns:
        ``3.Y`` for a CPython version, otherwise None.
    """
    for line in text.splitlines():
        words = line.split()
        if not words or words[0].startswith("#"):
            continue
        match = _CPYTHON_VERSION.fullmatch(words[0])
        return f"3.{match.group('minor')}" if match else None
    return None


def get_python_version(
    project_path: Path, current: Sequence[int] | None = None
) -> tuple[str, str]:
    """
    Get the ``X.Y`` Python version for the default base image: from the
    project's ``.python-version`` file if it names a CPython version,
    otherwise from the current environment.

    Args:
        project_path: Project root directory.
        current: Current Python version, defaults to ``sys.version_info``.

    Returns:
        ``X.Y`` and where it comes from, e.g. "from `.python-version`".
    """
    version_file = project_path / PYTHON_VERSION_FILE
    if version_file.is_file():
        text = version_file.read_bytes().decode("utf-8-sig", errors="replace")
        version = parse_python_version(text)
        if version:
            return version, f"from `{PYTHON_VERSION_FILE}`"
        secho(
            f"Ignoring `{version_file}` because it does not name a CPython version "
            f"such as 3.12. Using the Python version of the current environment.",
            fg="yellow",
        )
    major, minor = (current or sys.version_info)[:2]
    return f"{major}.{minor}", "of the current environment"


def get_base_image(python_version: str) -> str:
    """
    Get the default base image for a Python version. Patch versions are not used,
    because images tagged with them stop receiving updates.

    Args:
        python_version: ``X.Y`` Python version.

    Returns:
        Base image name.
    """
    return f"python:{python_version}-slim"


def make_pipeline_image_name(image: str, pipelines: str) -> str:
    """
    Make the default name of an image that runs specific pipelines.

    Args:
        image: Image name for the whole project.
        pipelines: Comma-separated pipeline names.

    Raises:
        KedroCliError: If no valid name can be made from the pipeline names.

    Returns:
        ``<image>-<pipelines>``, lowercased, with characters that are not
        allowed in an image name replaced by ``-``.
    """
    suffix = re.sub(
        r"[^a-z0-9]+",
        lambda m: "_" if m.group() == "_" else "-",
        pipelines.lower(),
    ).strip("-_")
    if not suffix:
        raise KedroCliError(
            f"Cannot make an image name from the pipelines `{pipelines}`. "
            f"Use --image to name the image."
        )
    return f"{image}-{suffix}"


def warn_if_uv_lock_outdated(project_path: Path):
    """
    Warn if the project's ``uv.lock`` does not match its ``pyproject.toml``,
    because the Dockerfile installs the locked versions as they are. Nothing is
    checked if there is no ``uv.lock`` or uv is not installed.

    Args:
        project_path: Project root directory.
    """
    uv = shutil.which("uv")
    if not uv or not (project_path / "uv.lock").is_file():
        return
    env = dict(os.environ, UV_PYTHON_DOWNLOADS="never")
    try:
        res = subprocess.run(
            [uv, "lock", "--check"],
            cwd=project_path,
            env=env,
            stdout=DEVNULL,
            stderr=DEVNULL,
            check=False,
            timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired):
        return
    # uv exits with 1 when the lock file needs updating, and 2 for other errors
    if res.returncode == 1:
        secho(
            "`uv.lock` is out of date, so the image will have the versions in it "
            "rather than the ones `pyproject.toml` asks for. Run `uv lock` to "
            "update it.",
            fg="yellow",
        )


def get_image_env(image: str) -> dict[str, str]:
    """
    Get the environment variables set in a Docker image.

    Args:
        image: Docker image name.

    Returns:
        Environment variables of the image, or an empty dict if they
        cannot be read.
    """
    command = ["docker", "image", "inspect", "--format", "{{json .Config.Env}}", image]
    res = subprocess.run(command, stdout=PIPE, stderr=DEVNULL, check=False)
    try:
        env = json.loads(res.stdout) if res.returncode == 0 else None
    except ValueError:
        env = None
    if not isinstance(env, list):
        return {}
    return dict(item.split("=", 1) for item in env if "=" in item)


def selects_pipelines(run_args: Sequence[str]) -> bool:
    """
    Check whether ``kedro run`` arguments choose the pipelines to run.

    Args:
        run_args: Arguments for ``kedro run``.

    Returns:
        True if ``-p``, ``--pipeline`` or ``--pipelines`` is used.
    """
    return any(
        arg.split("=", 1)[0] in ("--pipeline", "--pipelines")
        or (arg.startswith("-p") and not arg.startswith("--"))
        for arg in run_args
    )


def compose_kedro_run_command(
    run_args: Sequence[str], image_env: Mapping[str, str]
) -> list[str]:
    """
    Make the ``kedro run`` command for ``kedro docker run``. If the image was
    built for specific pipelines (``KEDRO_PIPELINES``), they are run unless
    the arguments choose other pipelines.

    Args:
        run_args: Arguments for ``kedro run``.
        image_env: Environment variables of the image.

    Returns:
        Command to run in the container.
    """
    pipelines = image_env.get(PIPELINES_ARG, "")
    if pipelines and not selects_pipelines(run_args):
        return ["kedro", "run", "--pipelines", pipelines, *run_args]
    return ["kedro", "run", *run_args]


def get_uid_gid(uid: int | None = None, gid: int | None = None) -> tuple[int, int]:
    """
    Get UID and GID to be passed into the Docker container.
    Defaults to the current user's UID and GID on Unix and (999, 0) on Windows.

    Args:
        uid: Input UID.
        gid: Input GID.

    Returns:
        (UID, GID).
    """

    # Default uid 999 is chosen as the one having potentially the lowest chance
    # of clashing with some existing user in the Docker container.
    _default_uid = 999

    # Default gid 0 corresponds to the root group.
    _default_gid = 0

    if uid is None:
        uid = os.getuid() if os.name == "posix" else _default_uid

    if gid is None:
        gid = (
            import_module("pwd").getpwuid(uid).pw_gid
            if os.name == "posix"
            else _default_gid
        )

    return uid, gid


def add_jupyter_args(run_args: list[str]) -> list[str]:
    """
    Adds `--ip 0.0.0.0` and `--no-browser` options to run args if those are not
    there yet.

    Args:
        run_args: Existing list of run arguments.

    Returns:
        Modified list of run arguments.
    """
    run_args = run_args.copy()
    if not any(arg.split("=", 1)[0] == "--ip" for arg in run_args):
        run_args += ["--ip", "0.0.0.0"]  # nosec
    if "--no-browser" not in run_args:
        run_args += ["--no-browser"]
    return run_args


def is_port_in_use(port: int) -> bool:
    """Check if the specified TCP port is already in use.

    Args:
        port: Port number.

    Returns:
        True if port is already in use, False otherwise.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _s:
        return _s.connect_ex(("0.0.0.0", port)) == 0  # nosec
