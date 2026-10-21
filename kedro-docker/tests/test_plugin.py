from pathlib import Path

import pytest
from click.testing import CliRunner

from kedro_docker.plugin import commands


@pytest.fixture
def project(tmp_path, monkeypatch):
    project_path = tmp_path / "my-project"
    project_path.mkdir()
    monkeypatch.chdir(project_path)
    return project_path


@pytest.fixture
def docker(mocker):
    """Mock the `docker version` check, the commands the plugin calls and the
    stale uv.lock check, and return the mock that records the called commands."""
    mocker.patch("kedro_docker.plugin.subprocess.run").return_value.returncode = 0
    mocker.patch("kedro_docker.plugin.check_docker_image_exists")
    mocker.patch("kedro_docker.plugin.warn_if_uv_lock_outdated")
    return mocker.patch("kedro_docker.plugin.call")


def _invoke(args):
    return CliRunner().invoke(commands, ["docker", *args])


class TestDockerInit:
    def test_base_image_from_python_version_file(self, project, docker):
        (project / ".python-version").write_text("3.11\n", encoding="utf-8")
        result = _invoke(["init"])
        assert result.exit_code == 0, result.output
        dockerfile = (project / "Dockerfile").read_text(encoding="utf-8")
        assert "ARG BASE_IMAGE=python:3.11-slim\n" in dockerfile
        assert (project / ".dockerignore").is_file()
        assert (project / ".dive-ci").is_file()

    def test_with_spark(self, project, docker):
        result = _invoke(["init", "--with-spark"])
        assert result.exit_code == 0, result.output
        assert "default-jre-headless" in (project / "Dockerfile").read_text(
            encoding="utf-8"
        )

    def test_existing_dockerfile(self, project, docker, mocker):
        (project / "Dockerfile").write_text("FROM img\n", encoding="utf-8")
        get_python_version = mocker.patch("kedro_docker.plugin.get_python_version")
        result = _invoke(["init"])
        assert result.exit_code == 0, result.output
        get_python_version.assert_not_called()
        assert (project / "Dockerfile").read_text(encoding="utf-8") == "FROM img\n"
        assert "Dockerfile already exists" in result.output


class TestDockerBuild:
    def test_default(self, project, docker, mocker):
        mocker.patch(
            "kedro_docker.plugin.get_python_version",
            return_value=("3.13", "of the current environment"),
        )
        result = _invoke(["build", "--uid", "1", "--gid", "2"])
        assert result.exit_code == 0, result.output
        assert (
            "Using base image `python:3.13-slim` "
            "(Python version of the current environment)." in result.output
        )
        docker.assert_called_once_with(
            [
                "docker",
                "build",
                "--build-arg",
                "KEDRO_UID=1",
                "--build-arg",
                "KEDRO_GID=2",
                "--build-arg",
                "BASE_IMAGE=python:3.13-slim",
                "-t",
                "my-project",
                str(project),
            ]
        )
        dockerfile = (project / "Dockerfile").read_text(encoding="utf-8")
        assert "ARG BASE_IMAGE=python:3.13-slim\n" in dockerfile

    def test_custom_base_image(self, project, docker):
        result = _invoke(["build", "--uid", "1", "--gid", "2", "--base-image", "img"])
        assert result.exit_code == 0, result.output
        assert "Using base image" not in result.output
        assert "BASE_IMAGE=img" in docker.call_args.args[0]
        dockerfile = (project / "Dockerfile").read_text(encoding="utf-8")
        assert "ARG BASE_IMAGE=img\n" in dockerfile

    def test_pipelines(self, project, docker):
        result = _invoke(
            ["build", "--uid", "1", "--gid", "2", "--pipelines", "data_processing"]
        )
        assert result.exit_code == 0, result.output
        command = docker.call_args.args[0]
        assert command[-5:] == [
            "--build-arg",
            "KEDRO_PIPELINES=data_processing",
            "-t",
            "my-project-data_processing",
            str(project),
        ]

    def test_pipelines_with_image(self, project, docker):
        result = _invoke(
            ["build", "--uid", "1", "--gid", "2", "--pipelines", "a", "--image", "x"]
        )
        assert result.exit_code == 0, result.output
        assert docker.call_args.args[0][-3:] == ["-t", "x", str(project)]

    def test_empty_pipelines(self, project, docker):
        result = _invoke(["build", "--uid", "1", "--gid", "2", "--pipelines", ""])
        assert result.exit_code == 0, result.output
        command = docker.call_args.args[0]
        assert not any(arg.startswith("KEDRO_PIPELINES") for arg in command)
        assert command[-3:] == ["-t", "my-project", str(project)]

    def test_pipelines_with_old_dockerfile(self, project, docker):
        (project / "Dockerfile").write_text(
            "FROM python:3.12-slim\nCMD kedro run\n", encoding="utf-8"
        )
        result = _invoke(["build", "--uid", "1", "--gid", "2", "--pipelines", "a"])
        assert result.exit_code != 0
        assert "does not declare `ARG KEDRO_PIPELINES`" in result.output
        assert "run `kedro docker init` to" in result.output
        docker.assert_not_called()

    def test_pipelines_with_old_spark_dockerfile(self, project, docker):
        (project / "Dockerfile").write_text("FROM img\n", encoding="utf-8")
        result = _invoke(
            ["build", "--uid", "1", "--gid", "2", "--with-spark", "--pipelines", "a"]
        )
        assert result.exit_code != 0
        assert "run `kedro docker init --with-spark` to" in result.output

    @pytest.mark.usefixtures("project", "docker")
    @pytest.mark.parametrize(
        "dockerfile, docker_args, checks",
        [
            (None, [], True),
            ("FROM img\nCOPY requirements.txt .\n", [], False),
            (None, ["--docker-args", "-f other.Dockerfile"], False),
        ],
    )
    def test_uv_lock_check(self, mocker, dockerfile, docker_args, checks):
        if dockerfile:
            (Path.cwd() / "Dockerfile").write_text(dockerfile, encoding="utf-8")
        warn = mocker.patch("kedro_docker.plugin.warn_if_uv_lock_outdated")
        args = ["build", "--uid", "1", "--gid", "2", "--base-image", "img"]
        result = _invoke(args + docker_args)
        assert result.exit_code == 0, result.output
        assert warn.called is checks

    def test_pipelines_with_custom_dockerfile(self, project, docker):
        (project / "Dockerfile").write_text("FROM python:3.12-slim\n", encoding="utf-8")
        result = _invoke(
            [
                "build",
                "--uid",
                "1",
                "--gid",
                "2",
                "--pipelines",
                "a",
                "--docker-args",
                "-f other.Dockerfile",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "KEDRO_PIPELINES=a" in docker.call_args.args[0]


class TestDockerRun:
    @pytest.mark.parametrize(
        "image_env, args, expected",
        [
            ({}, [], ["kedro", "run"]),
            (
                {"KEDRO_PIPELINES": "data_science"},
                ["--env=prod"],
                ["kedro", "run", "--pipelines", "data_science", "--env=prod"],
            ),
            (
                {"KEDRO_PIPELINES": "data_science"},
                ["--pipelines=reporting"],
                ["kedro", "run", "--pipelines=reporting"],
            ),
        ],
    )
    @pytest.mark.usefixtures("project")
    def test_run(self, docker, mocker, image_env, args, expected):
        get_image_env = mocker.patch(
            "kedro_docker.plugin.get_image_env", return_value=image_env
        )
        result = _invoke(["run", "--image", "img", *args])
        assert result.exit_code == 0, result.output
        get_image_env.assert_called_once_with("img")
        command = docker.call_args.args[0]
        assert command[command.index("img") + 1 :] == expected
