"""Behave environment setup commands"""

import os
import shutil
import stat
import tempfile
from pathlib import Path

from features.steps.sh_run import run
from features.steps.util import create_new_venv, docker_prune, kill_docker_containers


def call(cmd, env, verbose=False):
    res = run(cmd, env=env)
    if res.returncode or verbose:
        print(">", " ".join(cmd))
        print(res.stdout)
        print(res.stderr)
    assert res.returncode == 0


def before_all(context):
    """Environment preparation before other cli tests are run.
    Installs kedro by running pip in the top level directory.
    """

    # make a venv
    if "E2E_VENV" in os.environ:
        context.venv_dir = Path(os.environ["E2E_VENV"])
    else:
        context.venv_dir = create_new_venv()

    context = _setup_context_with_venv(context, context.venv_dir)

    call(
        [
            context.python,
            "-m",
            "pip",
            "install",
            "-U",
            # Temporarily pin pip to fix https://github.com/jazzband/pip-tools/issues/1503
            # This can be removed when Kedro 0.17.6 is released, because pip-tools is upgraded
            # for that version.
            "pip>=21.2,<23.2",
            "setuptools>=38.0",
            "wheel",
            ".",
        ],
        env=context.env,
    )

    # install the plugin
    call([context.python, "-m", "pip", "install", "."], env=context.env)


def _setup_context_with_venv(context, venv_dir):
    context.venv_dir = venv_dir
    # note the locations of some useful stuff
    # this is because exe resolution in subprocess doesn't respect a passed env
    if os.name == "posix":
        bin_dir = context.venv_dir / "bin"
        path_sep = ":"
    else:
        bin_dir = context.venv_dir / "Scripts"
        path_sep = ";"
    context.pip = str(bin_dir / "pip")
    context.python = str(bin_dir / "python")
    context.kedro = str(bin_dir / "kedro")

    # clone the environment, remove any condas and venvs and insert our venv
    context.env = os.environ.copy()
    path = context.env["PATH"].split(path_sep)
    path = [p for p in path if not (Path(p).parent / "pyvenv.cfg").is_file()]
    path = [p for p in path if not (Path(p).parent / "conda-meta").is_dir()]
    path = [str(bin_dir)] + path
    context.env["PATH"] = path_sep.join(path)

    # Create an empty pip.conf file and point pip to it
    pip_conf_path = context.venv_dir / "pip.conf"
    pip_conf_path.touch()
    context.env["PIP_CONFIG_FILE"] = str(pip_conf_path)

    return context


def after_all(context):
    if "E2E_VENV" not in os.environ:
        rmtree(context.venv_dir)
    docker_prune()


def before_scenario(context, feature):
    context.temp_dir = Path(tempfile.mkdtemp())


def after_scenario(context, feature):
    if "docker" in feature.tags:
        kill_docker_containers(context.project_name)
    docker_prune()
    rmtree(context.temp_dir)


def rmtree(top: Path):
    if os.name != "posix":
        for root, _, files in os.walk(str(top), topdown=False):
            for name in files:
                os.chmod(os.path.join(root, name), stat.S_IWUSR)
    shutil.rmtree(str(top))
