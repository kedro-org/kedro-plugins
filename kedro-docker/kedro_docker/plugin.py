""" Kedro plugin for packaging a project with Docker """
import shlex
import subprocess
from pathlib import Path
from sys import version_info
from typing import Dict, Tuple, Union

import click
from kedro import __version__ as kedro_version
from kedro.framework.cli.utils import KedroCliError, call, forward_command
from semver import VersionInfo

from .helpers import (
    add_jupyter_args,
    check_docker_image_exists,
    compose_docker_run_args,
    copy_template_files,
    get_uid_gid,
    is_port_in_use,
    make_container_name,
)

KEDRO_VERSION = VersionInfo.parse(kedro_version)

NO_DOCKER_MESSAGE = """
Cannot connect to the Docker daemon. Is the Docker daemon running?
"""

DOCKER_DEFAULT_VOLUMES = (
    "conf/local",
    "data",
    "logs",
    "notebooks",
    "references",
    "results",
)

DEFAULT_BASE_IMAGE = f"python:{version_info.major}.{version_info.minor}-slim"
DIVE_IMAGE = "wagoodman/dive:latest"


def _image_callback(ctx, param, value):
    image = value or Path.cwd().name
    check_docker_image_exists(image)
    return image


def _port_callback(ctx, param, value):
    if is_port_in_use(value):
        raise KedroCliError(
            f"Port {value} is already in use on the host. "
            f"Please specify an alternative port number."
        )
    return value


def _make_port_option(**kwargs):
    defaults = {
        "type": int,
        "default": 8888,
        "help": "Host port to publish to",
        "callback": _port_callback,
    }
    kwargs = dict(defaults, **kwargs)
    return click.option("--port", **kwargs)


def _make_image_option(**kwargs):
    defaults = {
        "type": str,
        "default": None,
        "help": "Docker image tag. Default is the project directory name",
    }
    kwargs = dict(defaults, **kwargs)
    return click.option("--image", **kwargs)


def _make_docker_args_option(**kwargs):
    defaults = {
        "type": str,
        "default": "",
        "callback": lambda ctx, param, value: shlex.split(value),
        "help": "Optional arguments to be passed to `docker run` command",
    }
    kwargs = dict(defaults, **kwargs)
    return click.option("--docker-args", **kwargs)


@click.group(name="Kedro-Docker")
def commands():
    pass


@commands.group(name="docker", context_settings={"help_option_names": ["-h", "--help"]})
def docker_group():
    """Dockerize your Kedro project."""
    # check that docker is running
    try:
        res = subprocess.run(
            ["docker", "version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
    except FileNotFoundError as err:
        raise KedroCliError(NO_DOCKER_MESSAGE) from err

    if res:
        raise KedroCliError(NO_DOCKER_MESSAGE)


@docker_group.command(name="init")
@click.option(
    "--with-spark",
    "spark",
    is_flag=True,
    help="Initialize a Dockerfile with Spark and Hadoop support.",
)
def docker_init(spark):
    """Initialize a Dockerfile for the project."""
    project_path = Path.cwd()
    template_path = Path(__file__).parent / "template"

    if KEDRO_VERSION.match(">=0.17.0"):
        verbose = KedroCliError.VERBOSE_ERROR
    else:
        from kedro.framework.cli.cli import (
            _VERBOSE as verbose,
        )

    docker_file_version = "spark" if spark else "simple"
    docker_file = f"Dockerfile.{docker_file_version}"
    copy_template_files(
        project_path,
        template_path,
        [docker_file, ".dockerignore", ".dive-ci"],
        verbose,
    )


@docker_group.command(name="build")
@click.option(
    "--uid",
    type=int,
    default=None,
    help="User ID for kedro user inside the container. "
    "Default is the current user's UID",
)
@click.option(
    "--gid",
    type=int,
    default=None,
    help="Group ID for kedro user inside the container. "
    "Default is the current user's GID",
)
@click.option(
    "--with-spark", "spark", is_flag=True, help="Build an image with Spark and Hadoop."
)
@click.option(
    "--base-image",
    type=str,
    default=DEFAULT_BASE_IMAGE,
    show_default=True,
    help="Base image for Dockerfile.",
)
@_make_image_option()
@_make_docker_args_option(
    help="Optional arguments to be passed to `docker build` command"
)
@click.pass_context
def docker_build(ctx, uid, gid, spark, base_image, image, docker_args):  # noqa: PLR0913
    """Build a Docker image for the project."""
    uid, gid = get_uid_gid(uid, gid)
    project_path = Path.cwd()
    image = image or project_path.name

    ctx.invoke(docker_init, spark=spark)

    combined_args = compose_docker_run_args(
        required_args=[
            ("--build-arg", f"KEDRO_UID={uid}"),
            ("--build-arg", f"KEDRO_GID={gid}"),
            ("--build-arg", f"BASE_IMAGE={base_image}"),
        ],
        # add image tag if only it is not already supplied by the user
        optional_args=[("-t", image)],
        user_args=docker_args,
    )
    command = ["docker", "build"] + combined_args + [str(project_path)]
    call(command)


def _mount_info() -> Dict[str, Union[str, Tuple]]:
    res = {
        "host_root": str(Path.cwd()),
        "container_root": "/home/kedro_docker",
        "mount_volumes": DOCKER_DEFAULT_VOLUMES,
    }
    return res


@forward_command(docker_group, "run")
@_make_image_option(callback=_image_callback)
@_make_docker_args_option()
def docker_run(image, docker_args, args, **kwargs):
    """Run the pipeline in the Docker container.
    Any extra arguments unspecified in this help
    are passed to `docker run` as is.

    **kwargs is needed to make the global `verbose` argument work and pass it through.
    """

    container_name = make_container_name(image, "run")
    _docker_run_args = compose_docker_run_args(
        optional_args=[("--rm", None), ("--name", container_name)],
        user_args=docker_args,
        **_mount_info(),
    )

    command = (
        ["docker", "run"] + _docker_run_args + [image, "kedro", "run"] + list(args)
    )
    call(command)


@forward_command(docker_group, "ipython")
@_make_image_option(callback=_image_callback)
@_make_docker_args_option()
def docker_ipython(image, docker_args, args, **kwargs):
    """Run ipython in the Docker container.
    Any extra arguments unspecified in this help are passed to
    `kedro ipython` command inside the container as is.

    **kwargs is needed to make the global `verbose` argument work and pass it through.
    """

    container_name = make_container_name(image, "ipython")
    _docker_run_args = compose_docker_run_args(
        optional_args=[("--rm", None), ("-it", None), ("--name", container_name)],
        user_args=docker_args,
        **_mount_info(),
    )

    command = (
        ["docker", "run"] + _docker_run_args + [image, "kedro", "ipython"] + list(args)
    )
    call(command)


@docker_group.group(name="jupyter")
def docker_jupyter():
    """Run jupyter notebook / lab in Docker container."""


@forward_command(docker_jupyter, "notebook")
@_make_image_option(callback=_image_callback)
@_make_port_option()
@_make_docker_args_option()
def docker_jupyter_notebook(docker_args, port, image, args, **kwargs):
    """Run jupyter notebook in the Docker container.
    Any extra arguments unspecified in this help are passed to
    `kedro jupyter notebook` command inside the container as is.

    **kwargs is needed to make the global `verbose` argument work and pass it through.
    """

    container_name = make_container_name(image, "jupyter-notebook")
    _docker_run_args = compose_docker_run_args(
        required_args=[("-p", f"{port}:8888")],
        optional_args=[("--rm", None), ("-it", None), ("--name", container_name)],
        user_args=docker_args,
        **_mount_info(),
    )

    args = add_jupyter_args(list(args))
    command = (
        ["docker", "run"]
        + _docker_run_args
        + [image, "kedro", "jupyter", "notebook"]
        + args
    )
    call(command)


@forward_command(docker_jupyter, "lab")
@_make_image_option(callback=_image_callback)
@_make_port_option()
@_make_docker_args_option()
def docker_jupyter_lab(docker_args, port, image, args, **kwargs):
    """Run jupyter lab in the Docker container.
    Any extra arguments unspecified in this help are passed to
    `kedro jupyter lab` command inside the container as is.

    **kwargs is needed to make the global `verbose` argument work and pass it through.
    """

    container_name = make_container_name(image, "jupyter-lab")
    _docker_run_args = compose_docker_run_args(
        required_args=[("-p", f"{port}:8888")],
        optional_args=[("--rm", None), ("-it", None), ("--name", container_name)],
        user_args=docker_args,
        **_mount_info(),
    )

    args = add_jupyter_args(list(args))
    command = (
        ["docker", "run"] + _docker_run_args + [image, "kedro", "jupyter", "lab"] + args
    )
    call(command)


@forward_command(docker_group, "cmd")
@_make_image_option(callback=_image_callback)
@_make_docker_args_option()
def docker_cmd(args, docker_args, image, **kwargs):
    """Run arbitrary command from ARGS in the Docker container.
    If ARGS are not specified, this will invoke `kedro run` inside the container.

    **kwargs is needed to make the global `verbose` argument work and pass it through.
    """

    container_name = make_container_name(image, "cmd")
    _docker_run_args = compose_docker_run_args(
        optional_args=[("--rm", None), ("--name", container_name)],
        user_args=docker_args,
        **_mount_info(),
    )

    command = ["docker", "run"] + _docker_run_args + [image] + list(args)
    call(command)


@docker_group.command(name="dive")
@click.option(
    "--ci/--no-ci",
    "ci_flag",
    default=True,
    show_default=True,
    help="Run Dive in non-interactive mode",
)
@click.option(
    "--ci-config-path",
    "-c",
    "dive_ci",
    default=".dive-ci",
    show_default=True,
    type=click.Path(exists=False, dir_okay=False, resolve_path=True),
    help="Path to `.dive-ci` config file",
)
@_make_image_option(callback=_image_callback)
@_make_docker_args_option()
def docker_dive(ci_flag, dive_ci, docker_args, image):
    """Run Dive analyzer of Docker image efficiency."""
    container_name = make_container_name(image, "dive")

    required_args = [("-v", "/var/run/docker.sock:/var/run/docker.sock")]
    optional_args = [("--rm", None), ("--name", container_name)]

    if ci_flag:
        dive_ci = Path(dive_ci).absolute()
        if dive_ci.is_file():
            required_args.append(("-v", f"{dive_ci}:/.dive-ci"))
        else:
            msg = f"`{dive_ci}` file not found, using default CI config"
            click.secho(msg, fg="yellow")
        required_args.append(("-e", f"CI={str(ci_flag).lower()}"))
    else:
        optional_args.append(("-it", None))

    _docker_run_args = compose_docker_run_args(
        required_args=required_args, optional_args=optional_args, user_args=docker_args
    )

    command = ["docker", "run"] + _docker_run_args + [DIVE_IMAGE] + [image]
    call(command)
