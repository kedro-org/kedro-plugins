# Introduction

Thank you for considering contributing to Kedro-Airflow! We welcome contributions in the form of pull requests, issues or code reviews. You can add to code, or simply send us spelling and grammar fixes or extra tests. Contribute anything that you think improves the community for us all!

The following sections describe our vision and the contribution process.

## Code of conduct

The Kedro team pledges to foster and maintain a welcoming and friendly community in all of our spaces. All members of our community are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md) and we will do our best to enforce those principles and build a happy environment where everyone is treated with respect and dignity.

# Get started

We use [GitHub Issues](https://github.com/kedro-org/kedro-plugins/issues) to keep track of known bugs. We keep a close eye on them and try to make it clear when we have an internal fix in progress. Before reporting a new issue, please do your best to ensure your problem hasn't already been reported. If so, it's often better to just leave a comment on an existing issue, rather than create a new one. Old issues also can often include helpful tips and solutions to common problems.

If you are looking for help with your code, please consider posting a question on [Stack Overflow](https://stackoverflow.com/questions/tagged/kedro-airflow). If you tag it `kedro-airflow`, `kedro` and `python`, more people will see it and may be able to help. We are unable to provide individual support via email. In the interest of community engagement we also believe that help is much more valuable if it's shared publicly, so that more people can benefit from it.

If you're over on Stack Overflow and want to boost your points, take a look at the `kedro-airflow` tag and see if you can help others out by sharing your knowledge. It's another great way to contribute.

If you have already checked the [existing issues](https://github.com/kedro-org/kedro-plugins/issues) on GitHub and are still convinced that you have found odd or erroneous behaviour then please file a [new issue](https://github.com/kedro-org/kedro-plugins/issues/new/choose). We have a template that helps you provide the necessary information we'll need in order to address your query.

## Feature requests

### Suggest a new feature

If you have new ideas for Kedro-Airflow functionality then please open a [GitHub issue](https://github.com/kedro-org/kedro-plugins/issues) with the label `Type: Enhancement`. Please describe in your own words the feature you would like to see, why you need it, and how it should work.

### Contribute a new feature

If you're unsure where to begin contributing to Kedro-Airflow, please start by looking through the `good first issues` and `Request: Help Wanted` on [GitHub](https://github.com/kedro-org/kedro-plugins/issues).

Typically, small contributions to Kedro-Airflow are more preferable due to an easier review process, but we accept any new features if they prove to be essential for the functioning of the plugin or if we believe that they are used by most projects.

## Your first contribution

Working on your first pull request? You can learn how from these resources:
* [First timers only](https://www.firsttimersonly.com/)
* [How to contribute to an open source project on GitHub](https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github)

### Guidelines

 - Aim for cross-platform compatibility on Windows, macOS and Linux
 - We use [Anaconda](https://www.anaconda.com/distribution/) as a preferred virtual environment
 - We use [SemVer](https://semver.org/) for versioning

Our code is designed to be compatible with Python 3.6 onwards and our style guidelines are (in cascading order):

* [PEP 8 conventions](https://www.python.org/dev/peps/pep-0008/) for all Python code
* [Google docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) for code comments
* [PEP 484 type hints](https://www.python.org/dev/peps/pep-0484/) for all user-facing functions / class methods e.g.

```
def count_truthy(elements: List[Any]) -> int:
    return sum(1 for elem in elements if elem)
```

> *Note:* We only accept contributions under the [Apache 2.0](https://opensource.org/licenses/Apache-2.0) license and you should have permission to share the submitted code.

### Branching conventions

We use a branching model that helps us keep track of branches in a logical, consistent way. All branches should have the hyphen-separated convention of: `<type-of-change>/<short-description-of-change>` e.g. `feature/awesome-new-feature`

| Types of changes | Description                                                                 |
| ---------------- | --------------------------------------------------------------------------- |
| `docs`           | Changes to the documentation of the plugin                                  |
| `feature`        | Non-breaking change which adds functionality                                |
| `fix`            | Non-breaking change which fixes an issue                                    |
| `tests`          | Changes to project unit (`tests/`) and / or integration (`features/`) tests |

## Plugin contribution process

 1. Fork the project
 2. Develop your contribution in a new branch and open a PR against the `master` branch
 3. Make sure the CI builds are green (have a look at the section [Running checks locally](#running-checks-locally) below)
 4. Update the PR according to the reviewer's comments

## CI / CD and running checks locally
To run E2E tests you need to install the test requirements which includes `behave`, do this using the following command:

```bash
pip install ".[test]"
```

### Running checks locally

All checks run by our CI / CD pipeline can be run locally on your computer.

#### Linting (`ruff` and `black`)

```bash
make plugin=kedro-airflow lint
```

#### Unit tests, 100% coverage (`pytest`, `pytest-cov`)

```bash
make plugin=kedro-airflow test
```

#### End-to-end tests (`behave`)

```bash
make plugin=kedro-airflow  e2e-tests
```
