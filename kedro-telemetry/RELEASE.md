# Upcoming release 0.2.1

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
