import re
import subprocess
import sys
from pathlib import Path

import pytest
from click import ClickException

from kedro_docker.helpers import (
    add_jupyter_args,
    check_docker_image_exists,
    compose_docker_run_args,
    compose_kedro_run_command,
    copy_template_files,
    dockerfile_declares_arg,
    get_base_image,
    get_image_env,
    get_python_version,
    get_uid_gid,
    is_port_in_use,
    make_container_name,
    make_pipeline_image_name,
    parse_python_version,
    selects_pipelines,
    set_dockerfile_arg_defaults,
    uses_custom_dockerfile,
    warn_if_uv_lock_outdated,
)

TEMPLATE_PATH = Path(__file__).parents[1] / "kedro_docker" / "template"


def test_missing_docker_image(mocker):
    """Check the error raised when docker image is missing"""
    patched_subproc = mocker.patch("subprocess.run")
    patched_subproc.return_value.stdout = b""
    image_name = "image-name"
    pattern = f"Unable to find image `{image_name}` locally"
    with pytest.raises(ClickException, match=pattern):
        check_docker_image_exists(image_name)
    assert patched_subproc.call_count == 1


@pytest.mark.parametrize(
    "args",
    [
        ["image-name-with-suffix"],
        ["image name with  suffix"],
        ["image!name", "with-suffix"],
        ["image!&+=*name", "with-suffix"],
    ],
)
def test_make_container_name(args):
    """Test docker container name normalization"""
    assert make_container_name(*args) == "image-name-with-suffix"


class TestComposeDockerRunArgs:
    def test_args(self, tmp_path):
        """Test composing the arguments for `docker run` command"""
        kwargs = {
            "host_root": str(tmp_path),
            "container_root": "/home/kedro/projectname",
            "optional_args": [("-arg1", "projectname"), ("--arg4", "x4")],
            "required_args": [("-arg2", None), ("-arg3", "x2")],
            "user_args": ["-arg1", "-arg2=y2", "-arg3", "y3"],
        }
        expected = ["-arg2", "-arg3", "x2", "--arg4", "x4"] + kwargs["user_args"]
        assert compose_docker_run_args(**kwargs) == expected

    def test_mount(self, tmp_path):
        """Test composing the arguments with volumes to mount"""
        host_root = tmp_path.resolve()
        kwargs = {
            "host_root": str(host_root),
            "container_root": "/home/kedro/projectname",
            "mount_volumes": ("conf/local", "data", "logs"),
            "user_args": ["-v", "y1"],
        }
        expected = []
        for _vol in kwargs["mount_volumes"]:
            _mount_vol = f"{host_root / _vol}:{kwargs['container_root']}/{_vol}"
            expected.extend(["-v", _mount_vol])
        expected += kwargs["user_args"]
        assert compose_docker_run_args(**kwargs) == expected

    @pytest.mark.parametrize("host_root", ["host_root", None])
    @pytest.mark.parametrize("container_root", ["container_root", None])
    def test_bad_mount(self, host_root, container_root):
        """Check the error raised when host and/or container roots are
        not defined, but mount volumes are provided"""
        mount_volumes = ("conf/local", "data", "logs")
        pattern = (
            "Both `host_root` and `container_root` must be specified "
            "in `compose_docker_run_args` call if `mount_volumes` "
            "are provided."
        )
        if not (host_root and container_root):
            with pytest.raises(ClickException, match=pattern):
                compose_docker_run_args(
                    host_root=host_root,
                    container_root=container_root,
                    mount_volumes=mount_volumes,
                )


class TestCopyTemplateFiles:
    def test_copy(self, tmp_path, capsys):
        """Test copying template files"""
        this_file = Path(__file__)
        dest_file = tmp_path / this_file.name
        assert not dest_file.exists()
        copy_template_files(tmp_path, this_file.parent, [this_file.name], True)
        captured = capsys.readouterr()
        assert "Creating" in captured.out
        assert "already exists" not in captured.out
        assert dest_file.exists()

    def test_skip(self, tmp_path, capsys):
        """Test copying is skipped if destination path already exists"""
        this_file = Path(__file__)
        dest_file = tmp_path / this_file.name
        with dest_file.open("w") as f:
            f.write("hello world")
        copy_template_files(tmp_path, this_file.parent, [this_file.name])
        with dest_file.open("r") as f:
            assert f.read().strip() == "hello world"
        captured = capsys.readouterr()
        expected_out = f"{this_file.name} already exists and won't be overwritten.\n"
        assert "Creating" not in captured.out
        assert captured.out == expected_out

    def test_copy_dockerfile(self, tmp_path):
        """Test copying template Dockerfile"""
        dockerfile = (
            Path(__file__).parents[1]
            / "kedro_docker"
            / "template"
            / "Dockerfile.simple"
        )
        dest_file = tmp_path / "Dockerfile"
        assert not dest_file.exists()
        copy_template_files(tmp_path, dockerfile.parent, [dockerfile.name])
        assert dest_file.exists()

    def test_copy_dockerfile_with_arg_defaults(self, tmp_path):
        """Test setting build argument defaults while copying a Dockerfile"""
        src = tmp_path / "src"
        src.mkdir()
        (src / "Dockerfile.simple").write_text(
            "ARG BASE_IMAGE=python:3.12-slim\nFROM ${BASE_IMAGE}\n", encoding="utf-8"
        )
        (src / ".dockerignore").write_text("ARG BASE_IMAGE=keep\n", encoding="utf-8")

        copy_template_files(
            tmp_path,
            src,
            ["Dockerfile.simple", ".dockerignore"],
            arg_defaults={"BASE_IMAGE": "python:3.11-slim"},
        )

        assert (tmp_path / "Dockerfile").read_text(encoding="utf-8") == (
            "ARG BASE_IMAGE=python:3.11-slim\nFROM ${BASE_IMAGE}\n"
        )
        assert (tmp_path / ".dockerignore").read_text(
            encoding="utf-8"
        ) == "ARG BASE_IMAGE=keep\n"


class TestTemplates:
    @pytest.mark.parametrize("template", ["Dockerfile.simple", "Dockerfile.spark"])
    def test_dockerfile_templates(self, template, tmp_path):
        """Test that both Dockerfile templates get the base image and declare
        the build arguments that the plugin passes"""
        copy_template_files(
            tmp_path, TEMPLATE_PATH, [template], arg_defaults={"BASE_IMAGE": "img"}
        )
        dockerfile = tmp_path / "Dockerfile"
        content = dockerfile.read_text(encoding="utf-8")

        assert content.count("ARG BASE_IMAGE=") == 1
        assert "ARG BASE_IMAGE=img\n" in content
        for name in ("KEDRO_UID", "KEDRO_GID", "KEDRO_PIPELINES", "UV_VERSION"):
            assert dockerfile_declares_arg(dockerfile, name)
        assert not re.search(r"^FROM .* as ", content, flags=re.MULTILINE)


@pytest.mark.parametrize(
    "content, arg_defaults, expected",
    [
        ("ARG A=1\nARG B=2\n", {"A": "x"}, "ARG A=x\nARG B=2\n"),
        ("ARG A=1\n", {"A": "x", "C": "y"}, "ARG A=x\n"),
        ('ARG A=""\n', {"A": "x"}, "ARG A=x\n"),
        ("ARG AB=1\nARG A\n", {"A": "x"}, "ARG AB=1\nARG A\n"),
        ("# ARG A=1\n", {"A": "x"}, "# ARG A=1\n"),
        ("ARG A=1\n", {"A": r"\1"}, "ARG A=\\1\n"),
    ],
)
def test_set_dockerfile_arg_defaults(content, arg_defaults, expected):
    """Test setting the default values of ARG lines"""
    assert set_dockerfile_arg_defaults(content, arg_defaults) == expected


@pytest.mark.parametrize(
    "content, expected",
    [
        ("ARG KEDRO_PIPELINES\n", True),
        ('ARG KEDRO_PIPELINES=""\n', True),
        ("  arg KEDRO_PIPELINES=a,b\n", True),
        ("ARG KEDRO_UID=1 KEDRO_PIPELINES\n", True),
        ("ENV KEDRO_PIPELINES=a\n", False),
        ("ARG KEDRO_PIPELINES_X=a\n", False),
        ("ARG kedro_pipelines\n", False),
        ("# ARG KEDRO_PIPELINES\n", False),
    ],
)
def test_dockerfile_declares_arg(tmp_path, content, expected):
    """Test checking whether a Dockerfile declares a build argument"""
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(content, encoding="utf-8")
    assert dockerfile_declares_arg(dockerfile, "KEDRO_PIPELINES") is expected


def test_dockerfile_declares_arg_missing_file(tmp_path):
    assert not dockerfile_declares_arg(tmp_path / "Dockerfile", "KEDRO_PIPELINES")


@pytest.mark.parametrize(
    "docker_args, expected",
    [
        ([], False),
        (["--no-cache", "--force-rm"], False),
        (["-f", "Dockerfile.prod"], True),
        (["-fDockerfile.prod"], True),
        (["--file", "Dockerfile.prod"], True),
        (["--file=Dockerfile.prod"], True),
    ],
)
def test_uses_custom_dockerfile(docker_args, expected):
    assert uses_custom_dockerfile(docker_args) is expected


@pytest.mark.parametrize(
    "text, expected",
    [
        ("3.12\n", "3.12"),
        ("3.12.4", "3.12"),
        ("  3.11  \n", "3.11"),
        ("# pinned\n\n3.13\n3.12\n", "3.13"),
        ("cpython@3.12", "3.12"),
        ("CPython-3.10", "3.10"),
        ("cp312", None),
        ("cpython-3.12.4-linux-x86_64-gnu", "3.12"),
        ("3.12 3.11", "3.12"),
        ("pypy@3.10", None),
        ("3.13t", None),
        ("system\n3.12", None),
        ("2.7", None),
        ("", None),
        ("# only a comment\n", None),
    ],
)
def test_parse_python_version(text, expected):
    """Test reading the version from a .python-version file"""
    assert parse_python_version(text) == expected


class TestGetPythonVersion:
    def test_from_file(self, tmp_path):
        (tmp_path / ".python-version").write_bytes(b"\xef\xbb\xbf3.11.9\n")
        assert get_python_version(tmp_path, (3, 12)) == (
            "3.11",
            "from `.python-version`",
        )

    def test_without_file(self, tmp_path):
        assert get_python_version(tmp_path, (3, 13, 1)) == (
            "3.13",
            "of the current environment",
        )

    def test_current_python(self, tmp_path):
        expected = f"{sys.version_info.major}.{sys.version_info.minor}"
        assert get_python_version(tmp_path)[0] == expected

    def test_invalid_file(self, tmp_path, capsys):
        (tmp_path / ".python-version").write_text("pypy@3.10\n", encoding="utf-8")
        assert get_python_version(tmp_path, (3, 12)) == (
            "3.12",
            "of the current environment",
        )
        assert "does not name a CPython version" in capsys.readouterr().out


def test_get_base_image():
    assert get_base_image("3.12") == "python:3.12-slim"


@pytest.mark.parametrize(
    "pipelines, expected",
    [
        ("data_processing", "proj-data_processing"),
        ("data_processing,data_science", "proj-data_processing-data_science"),
        ("Data Science", "proj-data-science"),
        ("__default__", "proj-default"),
        ("a__b..c", "proj-a-b-c"),
    ],
)
def test_make_pipeline_image_name(pipelines, expected):
    assert make_pipeline_image_name("proj", pipelines) == expected


@pytest.mark.parametrize("pipelines", ["", ",", "__"])
def test_make_pipeline_image_name_error(pipelines):
    with pytest.raises(ClickException, match="Use --image to name the image"):
        make_pipeline_image_name("proj", pipelines)


class TestWarnIfUvLockOutdated:
    @pytest.fixture
    def project(self, tmp_path):
        (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
        return tmp_path

    @pytest.fixture
    def uv(self, mocker):
        return mocker.patch("shutil.which", return_value="/bin/uv")

    @pytest.mark.usefixtures("uv")
    @pytest.mark.parametrize("returncode, warns", [(0, False), (1, True), (2, False)])
    def test_check(self, project, mocker, capsys, returncode, warns):
        run = mocker.patch("subprocess.run")
        run.return_value.returncode = returncode
        warn_if_uv_lock_outdated(project)
        assert run.call_args.args[0] == ["/bin/uv", "lock", "--check"]
        assert run.call_args.kwargs["cwd"] == project
        assert run.call_args.kwargs["env"]["UV_PYTHON_DOWNLOADS"] == "never"
        assert ("`uv.lock` is out of date" in capsys.readouterr().out) is warns

    @pytest.mark.parametrize(
        "error", [OSError("boom"), subprocess.TimeoutExpired("uv", 120)]
    )
    @pytest.mark.usefixtures("uv")
    def test_check_fails(self, project, mocker, capsys, error):
        mocker.patch("subprocess.run", side_effect=error)
        warn_if_uv_lock_outdated(project)
        assert capsys.readouterr().out == ""

    @pytest.mark.usefixtures("uv")
    def test_no_lock(self, tmp_path, mocker):
        run = mocker.patch("subprocess.run")
        warn_if_uv_lock_outdated(tmp_path)
        run.assert_not_called()

    def test_no_uv(self, project, mocker):
        mocker.patch("shutil.which", return_value=None)
        run = mocker.patch("subprocess.run")
        warn_if_uv_lock_outdated(project)
        run.assert_not_called()


@pytest.mark.parametrize(
    "returncode, stdout, expected",
    [
        (
            0,
            b'["PATH=/opt/venv/bin:/usr/bin","KEDRO_PIPELINES=a,b","A=x=y","B"]\n',
            {"PATH": "/opt/venv/bin:/usr/bin", "KEDRO_PIPELINES": "a,b", "A": "x=y"},
        ),
        (0, b"null\n", {}),
        (0, b"not json", {}),
        (1, b"", {}),
    ],
)
def test_get_image_env(mocker, returncode, stdout, expected):
    run = mocker.patch("subprocess.run")
    run.return_value.returncode = returncode
    run.return_value.stdout = stdout
    assert get_image_env("my-image") == expected
    assert run.call_args.args[0][:3] == ["docker", "image", "inspect"]
    assert run.call_args.args[0][-1] == "my-image"


@pytest.mark.parametrize(
    "run_args, expected",
    [
        ([], False),
        (["--runner=ParallelRunner", "--params", "a=1"], False),
        (["--pipelines", "a"], True),
        (["--pipelines=a"], True),
        (["--pipeline=a"], True),
        (["--pipeline", "a"], True),
        (["-p", "a"], True),
        (["-pa"], True),
        (["--pipeline-x"], False),
    ],
)
def test_selects_pipelines(run_args, expected):
    assert selects_pipelines(run_args) is expected


@pytest.mark.parametrize(
    "run_args, image_env, expected",
    [
        ([], {}, ["kedro", "run"]),
        (["--env=prod"], {"KEDRO_PIPELINES": ""}, ["kedro", "run", "--env=prod"]),
        (
            ["--env=prod"],
            {"KEDRO_PIPELINES": "a,b"},
            ["kedro", "run", "--pipelines", "a,b", "--env=prod"],
        ),
        (
            ["--pipeline=c"],
            {"KEDRO_PIPELINES": "a,b"},
            ["kedro", "run", "--pipeline=c"],
        ),
    ],
)
def test_compose_kedro_run_command(run_args, image_env, expected):
    assert compose_kedro_run_command(run_args, image_env) == expected


class TestGetUidGid:
    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="Cannot test POSIX on Windows"
    )
    @pytest.mark.parametrize(
        "uid, gid, expected",
        [
            [999, 0, (999, 0)],
            [None, 2, (123, 2)],
            [3, None, (3, 456)],
            [None, None, (123, 456)],
        ],
    )
    def test_posix(self, uid, gid, expected, mocker):
        """Test getting user and group id when host system is Posix"""
        mocker.patch("os.name", new="posix")
        mocker.patch("os.getuid", return_value=123)
        pw_gid = mocker.Mock()
        pw_gid.pw_gid = 456
        mocker.patch("pwd.getpwuid", return_value=pw_gid)
        assert get_uid_gid(uid, gid) == expected

    @pytest.mark.parametrize(
        "uid, gid, expected",
        [
            [234, 567, (234, 567)],
            [None, 2, (999, 2)],
            [3, None, (3, 0)],
            [None, None, (999, 0)],
        ],
    )
    def test_windows(self, uid, gid, expected, mocker):
        """Test getting user and group id when host system is Windows"""
        mocker.patch("os.name", new="windows")
        assert get_uid_gid(uid, gid) == expected


@pytest.mark.parametrize(
    "run_args, expected",
    [
        ([], ["--ip", "0.0.0.0", "--no-browser"]),
        (["--no-browser"], ["--no-browser", "--ip", "0.0.0.0"]),
        (["--foo", "ip"], ["--foo", "ip", "--ip", "0.0.0.0", "--no-browser"]),
        (["--foo", "ip", "--ip"], ["--foo", "ip", "--ip", "--no-browser"]),
        (["--foo", "--no-browser", "--ip=baz"], ["--foo", "--no-browser", "--ip=baz"]),
        (
            ["--foo", "--no-browser=bar", "--ip=baz"],
            ["--foo", "--no-browser=bar", "--ip=baz", "--no-browser"],
        ),
    ],
)
def test_add_jupyter_args(run_args, expected):
    """Test adding jupyter args to the existing list of CLI args"""
    assert add_jupyter_args(run_args) == expected


@pytest.mark.parametrize("port", [8888, 98765, 80, 8080])
@pytest.mark.parametrize("mock_return_value, expected", [(0, True), (61, False)])
def test_is_port_in_use(mocker, port, mock_return_value, expected):
    _mock = mocker.patch("socket.socket.connect_ex", return_value=mock_return_value)
    assert is_port_in_use(port) is expected
    _mock.assert_called_once_with(("0.0.0.0", port))
