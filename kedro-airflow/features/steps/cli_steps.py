"""Behave step definitions for the cli_scenarios feature."""

import yaml
from behave import given, then, when

from features.steps.sh_run import run

OK_EXIT_CODE = 0


@given('I have initialized Airflow with home dir "{home_dir}"')
def init_airflow(context, home_dir):
    context.airflow_dir = context.temp_dir / home_dir
    context.env["AIRFLOW_HOME"] = str(context.airflow_dir)
    res = run(["airflow", "db", "migrate"], env=context.env)
    assert res.returncode == 0


@given('I have installed kedro version "{version}"')
def install_kedro(context, version):
    """Execute Kedro command and check the status."""
    if version == "latest":
        cmd = [context.pip, "install", "-U", "kedro-datasets[PANDAS]"]
    else:
        cmd = [context.pip, "install", f"kedro-datasets[PANDAS]=={version}"]
    res = run(cmd, env=context.env)

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@given("I have installed the kedro project package")
def install_project_package(context):
    """Install the packaged project."""
    cmd = [context.pip, "install", "-e", "."]
    res = run(cmd, env=context.env, cwd=str(context.root_project_dir))

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@when('I execute the airflow command "{command}"')
def airflow_command(context, command):
    split_command = command.split()
    cmd = [context.airflow] + split_command
    context.result = run(cmd, env=context.env, cwd=str(context.root_project_dir))


@then('I should get a message including "{msg}"')
def check_message_printed(context, msg):
    """Check that specified message is printed to stdout (can be a segment)."""
    stdout = context.result.stdout
    assert msg in stdout, (
        "Expected the following message segment to be printed on stdout: "
        f"{msg},\nbut got {stdout}"
    )


@given("I have prepared a config file")
def create_configuration_file(context):
    """Behave step to create a temporary config file
    (given the existing temp directory)
    and store it in the context.
    """
    context.config_file = context.temp_dir / "config.yml"
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


@given("I have run a non-interactive kedro new")
def create_project_from_config_file(context):
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
            "astro-airflow-iris",
        ],
        env=context.env,
        cwd=str(context.temp_dir),
    )
    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@given('I have executed the kedro command "{command}"')
def exec_make_target_checked(context, command):
    """Execute Makefile target"""
    make_cmd = [context.kedro] + command.split()

    res = run(make_cmd, env=context.env, cwd=str(context.root_project_dir))

    if res.returncode != OK_EXIT_CODE:
        print(res.stdout)
        print(res.stderr)
        assert False


@then("I should get a successful exit code")
def check_status_code(context):
    if context.result.returncode != OK_EXIT_CODE:
        print(context.result.stdout)
        print(context.result.stderr)
        raise AssertionError(
            f"Expected exit code {OK_EXIT_CODE} but got {context.result.returncode}"
        )
