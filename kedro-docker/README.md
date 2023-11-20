# Kedro-Docker

[![Python Version](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue.svg)](https://pypi.org/project/kedro-docker/)
[![PyPI version](https://badge.fury.io/py/kedro-docker.svg)](https://pypi.org/project/kedro-docker/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/ambv/black)

Docker is a tool that makes it easier to create, deploy and run applications. It uses containers to package an application along with its dependencies and then runs the application in an isolated virtualised environment.

Configuring a Docker container environment may become complex and tedious. Kedro-Docker significantly simplifies this process and reduces it to 2 steps:

1. Build a Docker image
2. Run your [Kedro](https://github.com/kedro-org/kedro/) project in a Docker environment

> *Note:* Kedro-Docker also makes it easy for you to run IPython and Jupyter Notebooks in a Docker container.

## How do I install Kedro-Docker?

Kedro-Docker is a Python plugin. To install it:

```bash
pip install kedro-docker
```

## How do I use Kedro-Docker?

### Prerequisites

The following conditions must be true for Kedro-Docker to package your project:

* Make sure you have [installed](https://docs.docker.com/install/) Docker
* Kedro-Docker assumes that [Docker daemon](https://docs.docker.com/engine/docker-overview/) is up and running in your system

### Generating a Dockerfile

In order to generate a `Dockerfile` for your project, navigate to the project's root directory and then run the following from the command line:

```bash
kedro docker init
```

This command will generate `Dockerfile`, `.dockerignore` and `.dive-ci` files for your project.

Options:

* `--with-spark` - optional flag to create a `Dockerimage` file with Spark and Hadoop support
* `-h, --help` - show command help and exit.

### Build a Docker image

In order to build a Docker image for your project, navigate to the project's root directory and then run the following from the command line:

```bash
kedro docker build
```

Behind the scenes Kedro does the following:

1. Creates a template `Dockerfile` and `.dockerignore` in the project root directory if those files don't already exist
2. Builds the project image using the `Dockerfile` from the project root directory

> *Note:* When calling `kedro docker build` you can also pass any specific options for `docker build` by specifying `--docker-args` option. For example, `kedro docker build --docker-args="--no-cache"` instructs Docker not to use cache when building the image. You can learn more about available options [here](https://docs.docker.com/engine/reference/commandline/build/).

> *Note:* By default, `kedro docker build` creates an image without Spark and Hadoop.

> *Note:* By default, when calling `kedro docker build` image is built with `python:VERSION-buster` image, where VERSION is Python (major + minor) version from the current environment. By specifying `--base-image` option, different base image can be used. For example `kedro docker build --base-image="python:3.8-buster"`.

> *Note:* You can generate the `Dockerfile`, `.dockerignore` or `.dive-ci` files without building the image by running `kedro docker init`. This might be of use in case you would like to modify these files before the first build.

The project Docker image will automatically be tagged as `<project-root-dir>:latest`, where `<project-root-dir>` is the name of the project root directory. To change the tag, you can add the `--image` command line option, for example: `kedro docker build --image my-project-tag`.

When building the image Kedro copies the contents of the current project into the image, however it ignores the locations specified in `.dockerignore` file in order to prevent the propagation of potentially sensitive data into the image. We recommend mounting those volumes at runtime.

Options:
* `--uid` - optional integer User ID for kedro user inside the container. Defaults to the current user's UID
* `--gid` - optional integer Group ID for kedro user inside the container. Defaults to the current user's GID
* `--image` - optional Docker image tag. Defaults to the project directory name
* `--docker-args` - optional string containing extra options for `docker build` command
* `--with-spark` - optional flag to create an image additionally with Spark and Hadoop
* `--base-image` - optional base Docker image. Default is Debian buster with the current environment Python version, e.g. `python:3.8-buster`
* `-h, --help` - show command help and exit.

### Run your project in a Docker environment

Once the project image has been built, you can run the project using a Docker environment:

```bash
kedro docker run
```

The command above will:
1. Locate the image built in the previous section
2. Copy the whole project directory into the `/home/kedro` container path
3. Execute `kedro run` command in a new container

> *Note:* The `kedro docker run` command adds `--rm` flag to the underlying `docker run` call, therefore the container will be automatically removed when it exits. Please make sure that you persist all necessary data outside the container at runtime to avoid data loss.

By default `kedro docker run` will use an image tagged as `<project-root-dir>:latest` to create a container. If you renamed your image in the previous step, please also provide an `--image` option with the corresponding image tag, for example: `kedro docker run --image "my-custom-image:latest"`.

When calling `kedro docker run` you can also pass any specific options for `docker run` by providing `--docker-args` option. Since `--docker-args` may contain multiple arguments, it's a good idea to add quotation marks. For example, `kedro docker run --docker-args="--env KEY=MYVALUE"` instructs Docker to set environment variable `KEY` to `MYVALUE` in the container. You can learn more about available options [here](https://docs.docker.com/engine/reference/commandline/run/).

All other CLI options will be appended to `kedro run` command inside the container. For example, `kedro docker run --parallel` will run `kedro run --parallel` inside the container.

Options:
* `--image` - Docker image name to be used, defaults to project root directory name
* `--docker-args` - optional string containing extra options for `docker run` command
* `-h, --help` - show command help and exit
* Any other options will be treated as `kedro run` command options.

### Interactive development with Docker

In addition to `kedro docker run` Kedro also supports the following commands:

* `kedro docker ipython` - Run IPython session inside the container
* `kedro docker jupyter notebook` - Start a Jupyter Notebook inside the container
* `kedro docker jupyter lab` - Start a Jupyter Lab inside the container

Options:
* `--image` - Docker image name to be used, defaults to project root directory name
* `--docker-args` - optional string containing extra options for `docker run` command
* `--port` - host port that a container's port will be mapped to, defaults to 8888. This option applies to `kedro docker jupyter` commands only
* `-h, --help` - show command help and exit
* Any other options will be treated as corresponding `kedro` command CLI options. For example, `kedro docker jupyter lab --NotebookApp.token='' --NotebookApp.password=''` will run Jupyter Lab server without the password and token.

> *Important:* Please note that source code directory of a project (`src` folder) is *not* mounted to the Docker container by default. This means that if you change any code in `src` directory inside the container, those changes will *not* be saved to the host machine and will be completely lost when the container is terminated. In order to mount the whole project when running a Jupyter Lab, for example, run the following command:

```bash
kedro docker jupyter lab --docker-args "-v ${PWD}:/home/kedro"
```

### Image analysis with Dive

Kedro-Docker allows to analyze the size efficiency of your project image by leveraging [Dive](https://github.com/wagoodman/dive):

```bash
kedro docker dive
```

> *Note:* By default Kedro-Docker calls Dive in CI mode. If you want to explore your image in the UI, run `kedro docker dive --no-ci`.

Options:
* `--ci / --no-ci` - flag for running Dive in non-interactive mode. Defaults to true
* `--ci-config-path` - path to Dive CI config file. Defaults to `.dive-ci` in the project root directory
* `--image` - Docker image name to be used, defaults to project root directory name
* `--docker-args` - optional string containing extra options for `docker run` command
* `-h, --help` - show command help and exit.

### Running custom commands with Docker

You can also run an arbitrary command inside Docker container by executing `kedro docker cmd <CMD>`, where `<CMD>` corresponds to the command that you want to execute. If `<CMD>` is not specified, this will execute `kedro run` inside the container.

> *Note:* If you are running `kedro` command, unlike in the previous sections, you should specify the whole command including `kedro` keyword. This is to allow the execution of non Kedro commands as well.

For example:

1. `kedro docker cmd kedro test` will run `kedro test` inside the container
2. `kedro docker cmd` will run `kedro run` inside the container
3. `kedro docker cmd --docker-args="-it" /bin/bash` will create an interactive `bash` shell in the container (and allocate a pseudo-TTY connected to the container’s stdin).

Options:
* `--image` - Docker image name to be used, defaults to project root directory name
* `--docker-args` - optional string containing extra options for `docker run` command
* `-h, --help` - show command help and exit.

## Running Kedro-Docker with [Kedro-Viz](https://github.com/kedro-org/kedro-viz/)

These instructions allow you to access [Kedro-Viz](https://github.com/kedro-org/kedro-viz/), Kedro's data pipeline visualisation tool, via Docker. In your terminal, run the following commands:

```
pip download -d data --no-deps kedro-viz
kedro docker build
kedro docker cmd bash --docker-args="-it -u=0 -p=4141:4141"
pip install data/*.whl
kedro viz --host=0.0.0.0 --no-browser
```

And then open `127.0.0.1:4141` in your preferred browser. Incidentally, if `kedro-viz` is already installed in the Docker container (via requirements) then you can run:

```
kedro docker cmd --docker-args="-p=4141:4141" kedro viz --host=0.0.0.0
```

## Can I contribute?

Yes! Want to help build Kedro-Docker? Check out our guide to [contributing](https://github.com/kedro-org/kedro-plugins/blob/main/kedro-docker/CONTRIBUTING.md).

## What licence do you use?

Kedro-Docker is licensed under the [Apache 2.0](https://github.com/kedro-org/kedro-plugins/blob/main/LICENSE.md) License.

## Python version support policy
* The [Kedro-Docker](https://github.com/kedro-org/kedro-plugins/tree/main/kedro-docker) supports all Python versions that are actively maintained by the CPython core team. When a [Python version reaches end of life](https://devguide.python.org/versions/#versions), support for that version is dropped from `kedro-docker`. This is not considered a breaking change.
