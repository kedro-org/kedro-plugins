"""Behave environment setup commands"""

import os
import shutil
import stat
import tempfile
from pathlib import Path

from features.steps.sh_run import run
from features.steps.util import create_new_venv


def before_scenario(context, scenario):
    """Environment preparation before other cli tests are run.
    Installs kedro by running pip in the top level directory.
    """

    def call(cmd, print_output=False):
        res = run(cmd, env=context.env)
        if res.returncode or print_output:
            print(">", " ".join(cmd))
            print(res.stdout)
            print(res.stderr)
        assert res.returncode == 0

    # make a venv
    context.venv_dir = create_new_venv()

    # note the locations of some useful stuff
    # this is because exe resolution in supbrocess doens't respect a passed env
    if os.name == "posix":
        bin_dir = context.venv_dir / "bin"
        path_sep = ":"
    else:
        bin_dir = context.venv_dir / "Scripts"
        path_sep = ";"
    context.pip = str(bin_dir / "pip")
    context.python = str(bin_dir / "python")
    context.kedro = str(bin_dir / "kedro")
    context.airflow = str(bin_dir / "airflow")

    # clone the environment, remove any condas and venvs and insert our venv
    context.env = os.environ.copy()
    path = context.env["PATH"].split(path_sep)
    path = [p for p in path if not (Path(p).parent / "pyvenv.cfg").is_file()]
    path = [p for p in path if not (Path(p).parent / "conda-meta").is_dir()]
    path = [str(bin_dir)] + path
    context.env["PATH"] = path_sep.join(path)

    # pip install us
    call([context.python, "-m", "pip", "install", "-U", "pip", "pip-tools"])
    call([context.pip, "install", ".[test]"])

    context.temp_dir = Path(tempfile.mkdtemp())


def after_scenario(context, scenario):
    rmtree(str(context.temp_dir))
    rmtree(str(context.venv_dir))


def rmtree(top):
    if os.name != "posix":
        for root, _, files in os.walk(top, topdown=False):
            for name in files:
                os.chmod(os.path.join(root, name), stat.S_IWUSR)
    shutil.rmtree(top)
