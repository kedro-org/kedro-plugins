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
