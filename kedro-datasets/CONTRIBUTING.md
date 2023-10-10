# Introduction


Thank you for considering contributing to Kedro-Datasets! Kedro-Datasets is a collection of [Kedro's](https://github.com/kedro-org/kedro) data connectors. We welcome contributions in the form of pull requests, issues or code reviews. You can contribute new datasets, fix bugs in existing datasets, or simply send us spelling and grammar fixes or extra tests. Contribute anything that you think improves the community for us all!

The following sections describe our vision and the contribution process.

## Code of conduct

The Kedro team pledges to foster and maintain a welcoming and friendly community in all of our spaces. All members of our community are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md), and we will do our best to enforce those principles and build a happy environment where everyone is treated with respect and dignity.

# Get started

We use [GitHub Issues](https://github.com/kedro-org/kedro-plugins/issues) to keep track of known bugs. We keep a close eye on them and try to make it clear when we have an internal fix in progress. Before reporting a new issue, please do your best to ensure your problem hasn't already been reported. If so, it's often better to just leave a comment on an existing issue, rather than create a new one. Old issues also can often include helpful tips and solutions to common problems.

If you are looking for help with your code, please consider posting a question on [our Slack organisation](https://slack.kedro.org/). You can post your questions to the `#questions` channel. Past questions and discussions from our Slack organisation are accessible on [Linen](https://linen-slack.kedro.org/). In the interest of community engagement we also believe that help is much more valuable if it's shared publicly, so that more people can benefit from it.

If you have already checked the [existing issues](https://github.com/kedro-org/kedro-plugins/issues) on GitHub and are still convinced that you have found odd or erroneous behaviour then please file a [new issue](https://github.com/kedro-org/kedro-plugins/issues/new/choose). We have a template that helps you provide the necessary information we'll need in order to address your query.

## Feature requests

### Suggest a new feature

If you have new ideas for Kedro-Datasets then please open a [GitHub issue](https://github.com/kedro-org/kedro-plugins/issues) with the label `enhancement`. Please describe in your own words the feature you would like to see, why you need it, and how it should work.

### Contribute a new dataset

If you're unsure where to begin contributing to Kedro-Datasets, please start by looking through the `good first issue` and `help wanted` on [GitHub](https://github.com/kedro-org/kedro-plugins/issues).
If you want to contribute a new dataset, read the [tutorial to create and contribute a custom dataset](https://docs.kedro.org/en/stable/data/how_to_create_a_custom_dataset.html) in the Kedro documentation.
Make sure to add the new dataset to `kedro_datasets.rst` so that it shows up in the API documentation and to `static/jsonschema/kedro-catalog-X.json` for IDE validation.


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

> *Note:* We only accept contributions under the [Apache 2.0](https://opensource.org/licenses/Apache-2.0) license, and you should have permission to share the submitted code.

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
 2. Develop your contribution in a new branch.
 3. Make sure all your commits are signed off by using `-s` flag with `git commit`.
 4. Open a PR against the `main` branch and sure that the PR title follows the [Conventional Commits specs](https://www.conventionalcommits.org/en/v1.0.0/) with the scope `(datasets)`.
 5. Make sure the CI builds are green (have a look at the section [Running checks locally](#running-checks-locally) below)
 6. Update the PR according to the reviewer's comments

## CI / CD and running checks locally
To run tests you need to install the test requirements, do this using the following command:

```bash
make plugin=kedro-datasets install-test-requirements
make install-pre-commit
```


### Running checks locally

All checks run by our CI / CD pipeline can be run locally on your computer.

#### Linting (`ruff` and `black`)

```bash
make plugin=kedro-datasets lint
```

#### Unit tests, 100% coverage (`pytest`, `pytest-cov`)

```bash
make plugin=kedro-datasets test
```

If the tests in `kedro-datasets/kedro_datasets/spark` are failing, and you are not planning to work on Spark related features, then you can run the reduced test suite that excludes them with this command:
```bash
make test-no-spark
```
