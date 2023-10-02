import shlex
import subprocess
from typing import Dict


def run(
    cmd: str, split: bool = True, print_output: bool = False, **kwargs: Dict
) -> int:
    """
    Args:
        cmd: A command string, or a command followed by program
            arguments that will be submitted to POpen to run.

        split: Flag that splits command to provide as multiple *args
            to Popen. Default is True.

        print_output: If True will print previously captured stdout.
            Default is False.

        kwargs: Extra options to pass to subprocess

    Example:
    ::
        "ls"
        "ls -la"
        "chmod 754 local/file"

    Returns:
        Result with attributes args, returncode, stdout and
        stderr. By default, stdout and stderr are not captured, and those
        attributes will be None. Pass stdout=PIPE and/or stderr=PIPE in order
        to capture them.

    """
    if isinstance(cmd, str) and split:
        cmd = shlex.split(cmd)
    result = subprocess.run(cmd, input="", capture_output=True, **kwargs)
    result.stdout = result.stdout.decode("utf-8")
    result.stderr = result.stderr.decode("utf-8")
    if print_output:
        print(result.stdout)
    return result
