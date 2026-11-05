# Kedro-Datasets Benchmarks

This directory contains performance benchmarks for `kedro-datasets` using [airspeed velocity (asv)](https://asv.readthedocs.io/en/latest/).

## Running Benchmarks in the Monorepo

To run these benchmarks locally, ensure your terminal is located inside the `kedro-datasets` directory (e.g., `kedro-plugins/kedro-datasets`), and run:

1. Install airspeed velocity and dependencies:
   ```bash
   pip install asv
   pip install -e ".[pandas]"
   ```

2. Check benchmark validity:
   ```bash
   asv check -E existing
   ```
   Or from the root of the repository:
   ```bash
   make benchmark-datasets
   ```

3. Run the benchmarks:
   ```bash
   asv machine --yes
   asv run -E existing
   ```

## Continuous Integration

Benchmarks are continuously validated in GitHub Actions:
- On every pull request touching `kedro-datasets`, `asv check -E existing` runs as part of the `check-benchmarks` job in `.github/workflows/kedro-datasets.yml` to verify that the benchmark suite remains valid without regressions.
- The `ASV Benchmark for kedro-datasets` workflow in `.github/workflows/benchmark-performance.yml` provides automated and on-demand benchmark execution.
