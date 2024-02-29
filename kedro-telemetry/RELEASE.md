# Upcoming release 0.3.2
* Updated plugin to share if a project is being run in a ci environment.

# Release 0.3.1
* Fixed double execution of `after_catalog_created` hook by moving the logic of determining and sending of project statistics from `after_context_created` to the `after_catalog_created` hook.
* Updated the plugin to also share the tools selected during project creation with Heap.

# Release 0.3.0
* Added support for Python 3.11
* Removed support for Python 3.7

# Release 0.2.5
* Migrate all project metadata to static `pyproject.toml`.

# Release 0.2.4
* Added consent checking for collecting project statistics.

The `kedro-telemetry` plugin asks for consent to collect command run data. However, when we introduced functionality to understand the size of projects by counting the number of datasets, pipelines and nodes, the plugin did not verify consent, leading to the plugin collecting data without user consent. This bug affects users with `kedro-telemetry >= 0.2.2` installed, 22.5% of our user base.

<img width="730" alt="kedro-telemetry version piechart" src="https://user-images.githubusercontent.com/43755008/229840293-fea91403-b02d-4221-9167-71d3a6196cc5.png">

Here is an example of what was collected.

| Description               | What we received                        |
|---------------------------|-----------------------------------------|
| _(Hashed)_ Package name   | c7cd944c28cd888904f3efc2345198507...    |
| _(Hashed)_ Project name   | a6392d359362dc9827cf8688c9d634520e...   |
| `kedro` project version   | 0.18.4                                  |
| `kedro-telemetry` version | 0.2.3                                   |
| Python version            | 3.8.10 (default, Jun  2 2021, 10:49:15) |
| Operating system used     | darwin                                  |
| Number of datasets        | 7                                       |
| Number of pipelines       | 2                                       |
| Number of nodes           | 12                                      |

A fix must go beyond just releasing a bugfix for `kedro-telemetry`. Additionally, we will:

- Delete user data collected from `kedro-telemetry >= 0.2.2, <0.2.4`.
- Host a team retrospective and will specify post-mortem actions

> If you think of anything else we should prioritise then [let us know on Slack](https://slack.kedro.org/). And we also need to thank the contributor who flagged this: [@melvinkokxw](https://github.com/melvinkokxw) [helped identify this bug](https://github.com/kedro-org/kedro/issues/2492) and we're grateful to him because he helped give enough context for us to address this.


# Release 0.2.3

## Bug fixes and other changes
* Modified the process for reading a Kedro project's package name and version to avoid a failed run when no `pyproject.toml` can be read.
* Report the version of Kedro used to run a project, no longer report the project's name.

# Release 0.2.2

## Bug fixes and other changes
* Changed the value associated with the `identity` key in the call to heap's /track endpoint from hashed computer name to hashed username.
* Expanding to track numbers of datasets, nodes, pipelines to better understand the size of kedro project.

# Release 0.2.1

## Bug fixes and other changes
* Removed explicit `PyYAML` requirement since it is already a dependency of `Kedro`

## Bug fixes and other changes
* `kedro telemetry` raising errors will no longer stop `kedro` running pipelines.
* Lowered some of the log level to `DEBUG`, so they will not be shown by default.

# Release 0.2.0

## Bug fixes and other changes
* Add compatibility with `kedro` 0.18.0
* Add compatibility with Python 3.9 and 3.10
* Remove compatibility with Python 3.6

# Release 0.1.4

## Bug fixes and other changes
* If consent is provided but there's no internet connection, `kedro-telemetry` will log a warning instead of raising an error (addresses https://github.com/kedro-org/kedro/issues/1289 and https://github.com/kedro-org/kedro/issues/1249).
* Removed `click` requirement, defaulting instead to whatever `click` version is used by `kedro`.

# Release 0.1.3

## Bug fixes and other changes
* CLI command arguments on `kedro-telemetry` are now masked client-side, this allows for a more maintainable vocabulary.

# Release 0.1.2

## Bug fixes and other changes
* The plugin now sends an additional event to Heap, under the same type for any CLI command run, to make data processing easier.

# Release 0.1.1

## Bug fixes and other changes
* `kedro-telemetry` no longer raises an error when the user doesn't call any specific commands (i.e. only running `kedro` in the CLI).

# Release 0.1.0

The initial release of Kedro-Telemetry.
