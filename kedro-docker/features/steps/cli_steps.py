"""Behave step definitions for the cli_scenarios feature."""
import re
import sys
import textwrap
from pathlib import Path
from time import sleep

import behave
import yaml
from behave import given, then, when

from features.steps.sh_run import ChildTerminatingPopen, run
from features.steps.util import (
    TimeoutException,
    download_url,
    get_docker_images,
    kill_docker_containers,
    timeout,
    wait_for,
)

OK_EXIT_CODE = 0


def _read_lines_with_timeout(process_handler, max_lines=100):
    """
    We want to read from multiple streams, merge outputs together,
    limiting the number of lines we want.
    Also don't try for longer than ``timeout`` seconds.

    NOTE: a nice and easy solution might be implemented
    with ``fcntl`` and non-blocking read, but it's unix-only.
    """
    lines = []

    def _read_stdout():
        # In some cases, if the command dies at start, it will be a string here.
        stream = process_handler.stdout

        if isinstance(stream, str):
            lines.extend(stream.split("\n"))
            return

        while len(lines) < max_lines:
            new_line = stream.readline().decode().strip()

            if new_line:
                lines.append(new_line)

            sleep(0.1)

    def _read_stderr():
        while True:
            new_line = process_handler.stderr.readline().decode().strip()

            if new_line:
                lines.append(new_line)

    try:
        timeout(_read_stdout, duration=30)
    except TimeoutException:
        pass

    # Read the remaining error logs if any
    try:
        timeout(_read_stderr, duration=3)
    except TimeoutException:
        pass

    return "\n".join(lines)


def _get_docker_ipython_output(context):
    """Get first 16 lines of ipython output if not already retrieved"""
    if hasattr(context, "ipython_stdout"):
        return context.ipython_stdout

    try:
        context.ipython_stdout = _read_lines_with_timeout(context.result, max_lines=128)
    finally:
        kill_docker_containers(context.project_name)

    return context.ipython_stdout


def _check_service_up(context: behave.runner.Context, url: str, string: str):
    """
    Check that a service is running and responding appropriately

    Args:
        context: Test context.
        url: Url that is to be read.
        string: The string to be checked.
    """
    data = download_url(url)

    try:
        assert context.result.poll() is None
        assert string in data
    finally:
        if "docker" in context.result.args:
            kill_docker_containers(context.project_name)
        else:
            context.result.terminate()


@given("I have prepared a config file")
def create_configuration_file(context):
    """Behave step to create a temporary config file
    (given the existing temp directory)
    and store it in the context.
    """
    context.config_file = context.temp_dir / "config"
    context.project_name = "project-dummy"

    root_project_dir = context.temp_dir / context.project_name
    context.root_project_dir = root_project_dir
    config = {
        "project_name": context.project_name,
        "repo_name": context.project_name,
        "output_dir": str(context.temp_dir),
        "python_package": context.project_name.replace("-", "_"),
    }
    with context.config_file.open("w") as config_file:
        yaml.dump(config, config_file, default_flow_style=False)


@given("I run a non-interactive kedro new using {starter_name} starter")
def create_project_from_config_file(context, starter_name):
    """Behave step to run kedro new
    given the config I previously created.
    """
    res = run(
        [
            context.kedro,
            "new",
            "-c",
            str(context.config_file),
            "--starter",
            starter_name,
        ],
        env=context.env,
        cwd=str(context.temp_dir),
    )

    # add a consent file to prevent telemetry from prompting for input during e2e test
    telemetry_file = context.root_project_dir / ".telemetry"
    telemetry_file.write_text("consent: false", encoding="utf-8")

    # override base logging configuration to simplify assertions
    logging_conf = context.root_project_dir / "conf" / "base" / "logging.yml"
    logging_conf.write_text(
        textwrap.dedent(
            """
        version: 1

        disable_existing_loggers: False

        formatters:
          simple:
            format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        handlers:
          console:
            class: logging.StreamHandler
            level: INFO
            formatter: simple
            stream: ext://sys.stdout

        loggers:
          kedro:
            level: INFO

        root:
          handlers: [console]
        """
        )
    )

    assert res.returncode == 0


@given('I have executed the kedro command "{command}"')
def exec_kedro_command(context, command):
    """Execute Kedro command and check the status."""
    make_cmd = [context.kedro] + command.split()

    res = run(make_cmd, env=context.env, cwd=str(context.root_project_dir))

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@given("I have installed the project dependencies")
def pip_install_dependencies(context):
    """Install project dependencies using pip."""
    reqs_path = Path("requirements.txt")
    res = run(
        [context.pip, "install", "-r", str(reqs_path)],
        env=context.env,
        cwd=str(context.root_project_dir),
    )

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@given("I have removed old docker image of test project")
def remove_old_docker_images(context):
    """Remove old docker images of project"""
    run(["docker", "rmi", context.project_name])


@when('I execute the kedro command "{command}"')
def exec_kedro_target(context, command):
    """Execute Kedro target"""
    split_command = command.split()
    make_cmd = [context.kedro] + split_command

    if split_command[0] == "docker" and split_command[1] in ("ipython", "jupyter"):
        context.result = ChildTerminatingPopen(
            make_cmd, env=context.env, cwd=str(context.root_project_dir)
        )
    else:
        context.result = run(
            make_cmd, env=context.env, cwd=str(context.root_project_dir)
        )


@given("I have executed kedro docker build with custom base image")
@when("I execute kedro docker build with custom base image")
def exec_docker_build_target(context):
    """Execute Kedro Docker build with custom base image"""
    base_image = f"python:3.{sys.version_info[1]}-buster"
    cmd = [context.kedro, "docker", "build", "--base-image", base_image]
    context.result = run(cmd, env=context.env, cwd=str(context.root_project_dir))


@when('I occupy port "{port}"')
def occupy_port(context, port):
    """Execute  target"""
    ChildTerminatingPopen(
        ["nc", "-l", "0.0.0.0", port],
        env=context.env,
        cwd=str(context.root_project_dir),
    )


@then('I should get a message including "{msg}"')
def read_docker_stdout(context, msg):
    """Read stdout and raise AssertionError if the given message is not there."""

    if hasattr(context.result.stdout, "read"):
        context.result.stdout = context.result.stdout.read().decode("utf-8")
    if msg == "Python":
        msg = f"Python 3.{sys.version_info[1]}"

    try:
        if msg not in context.result.stdout:
            print(context.result.stdout)
            assert False, f"Message '{msg}' not found in stdout"
    finally:
        kill_docker_containers(context.project_name)


@then('Standard output should contain a message including "{msg}"')
def read_docker_stdout_error(context, msg):
    """Read stdout and raise AssertionError if the given message is not there."""

    if hasattr(context.result.stdout, "read"):
        context.result.stdout = context.result.stdout.read().decode("utf-8")

    try:
        if msg not in context.result.stdout:
            print(context.result.stdout)
            assert False, f"Message '{msg}' not found in stdout"
    finally:
        kill_docker_containers(context.project_name)


@then('Standard error should contain a message including "{msg}"')
def read_docker_stderr(context, msg):
    """Read stderr and raise AssertionError if the given message is not there."""

    if hasattr(context.result.stderr, "read"):
        context.result.stderr = context.result.stderr.read().decode("utf-8")

    try:
        if msg not in context.result.stderr:
            print(context.result.stderr)
            assert False, f"Message '{msg}' not found in stderr"
    finally:
        kill_docker_containers(context.project_name)


@then("I should get a successful exit code")
def check_status_code(context):
    if context.result.returncode != OK_EXIT_CODE:
        print(context.result.stdout)
        print(context.result.stderr)
        assert (
            False
        ), f"Expected exit code {OK_EXIT_CODE} but got {context.result.returncode}"


@then("I should get an error exit code")
def check_failed_status_code(context):
    if context.result.returncode == OK_EXIT_CODE:
        print(context.result.stdout)
        print(context.result.stderr)
        assert (
            False
        ), f"Expected exit code {OK_EXIT_CODE} but got {context.result.returncode}"


@then('I should see messages from docker ipython startup including "{msg}"')
def check_docker_ipython_msg(context, msg):
    stdout = _get_docker_ipython_output(context)

    assert msg in stdout, (
        "Expected the following message segment to be printed on stdout: "
        f"{msg},\nbut got {stdout}"
    )


@then("Jupyter {command} should run on port {port}")
def check_jupyter_nb_proc_on_port(
    context: behave.runner.Context, command: str, port: int
):
    """
    Check that jupyter notebook service is running on specified port

    Args:
        context: Test context
        command: Jupyter command message to check
        port: Port to check
    """
    url = f"http://localhost:{int(port)}"
    wait_for(
        func=_check_service_up,
        expected_result=None,
        print_error=False,
        context=context,
        url=url,
        string=f"Jupyter {command}",
        timeout_=15,
    )


@then("A new docker image for test project should be created")
def check_docker_project_created(context):
    """Check that docker image for test project has been created"""

    def _check_image():
        while True:
            if get_docker_images(context.project_name):
                return True
            sleep(0.5)

    assert timeout(_check_image, duration=30)


@then("A {filepath} file should exist")
def check_if_file_exists(context: behave.runner.Context, filepath: str):
    """Checks if file is present and has content.

    Args:
        context: Behave context.
        filepath: A path to a file to check for existence.
    """
    filepath: Path = context.root_project_dir / filepath
    assert (
        filepath.exists()
    ), f"Expected {filepath} to exists but .exists() returns {filepath.exists()}"
    assert (
        filepath.stat().st_size > 0
    ), f"Expected {filepath} to have size > 0 but has {filepath.stat().st_size}"


@then("A {filepath} file should contain {text} string")
def grep_file(context: behave.runner.Context, filepath: str, text: str):
    """Checks if given file contains passed string.

    Args:
        context: Behave context.
        filepath: A path to a file to grep.
        text: Text (or regex) to search for.
    """
    filepath: Path = context.root_project_dir / filepath
    with filepath.open("r") as file:
        found = any(line and re.search(text, line) for line in file)
    assert found, f"String {text} not found in {filepath}"
