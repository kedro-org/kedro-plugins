""" Utilities for use with click docker commands """

import os
import re
import shutil
import socket
import subprocess
from importlib import import_module
from itertools import chain
from pathlib import Path, PurePosixPath
from subprocess import DEVNULL, PIPE
from typing import List, Sequence, Tuple, Union

from click import secho
from kedro.framework.cli.utils import KedroCliError


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
    host_root = Path(host_root).resolve()
    container_root = PurePosixPath(container_root)
    for volume in volumes:
        hpath = host_root / volume  # host path
        cpath = PurePosixPath(container_root) / volume  # container path
        yield "-v", str(hpath) + ":" + str(cpath)


def compose_docker_run_args(  # noqa: PLR0913
    host_root: str = None,
    container_root: str = None,
    mount_volumes: Sequence[str] = None,
    required_args: Sequence[Tuple[str, Union[str, None]]] = None,
    optional_args: Sequence[Tuple[str, Union[str, None]]] = None,
    user_args: Sequence[str] = None,
) -> List[str]:
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

    def _add_args(name_: str, value_: str = None, force_: bool = False) -> List[str]:
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
    return combined_args + user_args


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
):
    """
    If necessary copy files from a template directory into a project directory.

    Args:
        project_path: Destination path.
        template_path: Source path.
        template_files: Files to copy.
        verbose: Echo the names of any created files.

    """
    for file_ in template_files:
        dest_file = "Dockerfile" if file_.startswith("Dockerfile") else file_
        dest = project_path / dest_file
        if not dest.exists():
            src = template_path / file_
            shutil.copyfile(str(src), str(dest))
            if verbose:
                secho(f"Creating `{dest}`")
        else:
            msg = f"{dest_file} already exists and won't be overwritten."
            secho(msg, fg="yellow")


def get_uid_gid(uid: int = None, gid: int = None) -> Tuple[int, int]:
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


def add_jupyter_args(run_args: List[str]) -> List[str]:
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
