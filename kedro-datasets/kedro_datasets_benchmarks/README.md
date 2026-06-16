# Kedro-Datasets Benchmarks

This directory contains performance benchmarks for `kedro-datasets` using [airspeed velocity (asv)](https://asv.readthedocs.io/).

## Running Benchmarks in the Monorepo

To run these benchmarks locally, ensure your terminal is located inside the `kedro-datasets` directory (e.g., `kedro-plugins/kedro-datasets`), and run:

1. Install airspeed velocity:
   ```bash
   pip install asv
   ```

2. Run the benchmarks:
   ```bash
   asv run
   ```
