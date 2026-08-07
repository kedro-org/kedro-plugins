# MLRun Integration

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![Kedro](https://img.shields.io/badge/kedro-compatible-green)](https://kedro.org/)
[![MLRun](https://img.shields.io/badge/mlrun-integration-orange)](https://www.mlrun.org/)

Kedro datasets for seamless integration with [MLRun](https://www.mlrun.org/) - an open-source MLOps orchestration framework for experiment tracking, model versioning, and artifact management.

## Key concepts

- **`key`**: The MLRun artifact or result identifier—the name under which the artifact is stored and retrieved in MLRun. In the catalog, if you omit `key`, the dataset name from the catalog is used (e.g. a dataset named `user_data` will use `user_data` as the MLRun key). You can set `key` explicitly, e.g. `key: generate-data-main_user_data.csv`, to control the exact name in MLRun.
- **`framework`** (models only): The ML framework name (e.g. `sklearn`, `xgboost`, `lightgbm`). MLRun uses this to tag and handle the model correctly (metadata, deployment, etc.).
- **`load_args` / `save_args`**: Each dataset forwards these to the underlying MLRun API. You can pass any arguments that the corresponding MLRun function supports in your MLRun version—except `key`, which is set from the dataset `key` parameter. **Depending on your MLRun version, supported arguments may vary; check the [MLRun API docs](https://docs.mlrun.org/en/latest/?badge=latest) for your version.**

## Datasets

| Dataset                 | Description                                           | MLRun API                                                                                                                                                                                                                                              |
|-------------------------|-------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `MLRunAbstractDataset`  | Base class; use for generic artifacts                 | [`log_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_artifact), [`get_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.get_artifact) |
| `MLRunModel`            | Save/load ML models with framework metadata           | [`log_model`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_model), [`get_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.get_artifact)       |
| `MLRunDataframeDataset` | Save/load pandas DataFrames as artifacts              | [`log_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_artifact), [`get_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.get_artifact) |
| `MLRunResult`           | Log scalar results/metrics (optional dict flattening) | [`log_result`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_result), `context.results`                                                                                                              |

## Installation

```bash
pip install "kedro-datasets[mlrun]"
```

### Requirements
- Python 3.10+
- Kedro
- MLRun SDK
- joblib

## Quick Start

```python
from kedro_datasets_experimental.mlrun import MLRunModel, MLRunDataframeDataset, MLRunResult

# Save and load ML models
model_dataset = MLRunModel(
    key="trained_model",
    framework="sklearn",
    model_format="pkl",
)
model_dataset.save(trained_model)
loaded_model = model_dataset.load()

# Save and load DataFrames
df_dataset = MLRunDataframeDataset(key="processed_data")
df_dataset.save(df)
loaded_df = df_dataset.load()

# Log metrics and results
result_dataset = MLRunResult(key="metrics", flatten=True)
result_dataset.save({"accuracy": 0.95, "loss": 0.05})
```

## Dataset examples

Each dataset supports the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/) (catalog) and the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/). The `load_args` and `save_args` you pass are forwarded to the underlying MLRun function; see the linked MLRun API for your version.

### MLRunModel

Uses MLRun's [`log_model`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_model) and [`get_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.get_artifact).

**YAML (catalog):**

```yaml
trained_model:
  type: kedro_datasets_experimental.mlrun.MLRunModel
  key: my_sklearn_model
  framework: sklearn
  model_format: pkl
  # save_args/load_args: any args supported by log_model/get_artifact for your MLRun version
```

**Python API:**

```python
from kedro_datasets_experimental.mlrun import MLRunModel

dataset = MLRunModel(
    key="my_sklearn_model",
    framework="sklearn",
    model_format="pkl",
)
dataset.save(trained_model)
loaded_model = dataset.load()
```

| Parameter      | Type | Default     | Description                                                                                                                     |
|----------------|------|-------------|---------------------------------------------------------------------------------------------------------------------------------|
| `key`          | str  | None        | Artifact key for MLRun; defaults to catalog dataset name.                                                                       |
| `framework`    | str  | `"sklearn"` | ML framework (e.g. `sklearn`, `xgboost`, `lightgbm`).                                                                           |
| `model_format` | str  | `"pkl"`     | File extension for the model file.                                                                                              |
| `load_args`    | dict | None        | Passed to MLRun when loading; see MLRun docs for your version.                                                                  |
| `save_args`    | dict | None        | Passed to [`log_model`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_model). |

### MLRunDataframeDataset

Uses MLRun's [`log_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_artifact) and [`get_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.get_artifact).

**YAML (catalog):**

```yaml
user_data:
  type: kedro_datasets_experimental.mlrun.MLRunDataframeDataset
  key: generate-data-main_user_data
  # load_args/save_args: any args supported by get_artifact/log_artifact for your MLRun version
```

**Python API:**

```python
from kedro_datasets_experimental.mlrun import MLRunDataframeDataset
import pandas as pd

dataset = MLRunDataframeDataset(key="processed_df")
dataset.save(pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
loaded_df = dataset.load()
```

| Parameter   | Type | Default | Description                                                                                                                           |
|-------------|------|---------|---------------------------------------------------------------------------------------------------------------------------------------|
| `key`       | str  | None    | Artifact key for MLRun; defaults to catalog dataset name.                                                                             |
| `load_args` | dict | None    | Passed to MLRun when loading; see MLRun docs for your version.                                                                        |
| `save_args` | dict | None    | Passed to [`log_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_artifact). |

### MLRunResult

Uses MLRun's [`log_result`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_result); results are read from `context.results`.

**YAML (catalog):**

```yaml
training_metrics:
  type: kedro_datasets_experimental.mlrun.MLRunResult
  key: metrics
  flatten: true
```

**Python API:**

```python
from kedro_datasets_experimental.mlrun import MLRunResult

dataset = MLRunResult(key="metrics", flatten=True)
dataset.save({"accuracy": 0.95, "loss": 0.05})
# or nested: {"train": {"accuracy": 0.9}, "val": {"accuracy": 0.85}} -> train.accuracy, val.accuracy
loaded = dataset.load()
```

| Parameter   | Type | Default | Description                                                                                                                                                                        |
|-------------|------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `key`       | str  | None    | Result key for MLRun; defaults to catalog dataset name.                                                                                                                            |
| `flatten`   | bool | `False` | If `True`, flatten nested dicts to dot-notation keys. When `flatten=True`, each key is stored as a separate MLRun result; load per key (e.g. from `context.results` for each key). |
| `load_args` | dict | None    | For loading; see MLRun docs for your version.                                                                                                                                      |
| `save_args` | dict | None    | Passed to [`log_result`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_result).                                                  |

### MLRunAbstractDataset

Base class for generic artifacts. Uses [`log_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_artifact) and [`get_artifact`](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.get_artifact).

**YAML (catalog):**

```yaml
generic_artifact:
  type: kedro_datasets_experimental.mlrun.MLRunAbstractDataset
  key: my_artifact
```

**Python API:**

```python
from kedro_datasets_experimental.mlrun import MLRunAbstractDataset

dataset = MLRunAbstractDataset(key="config_data")
dataset.save({"param1": "value1", "param2": 42})
loaded = dataset.load()
```

## Architecture

### MLRunContextManager

All datasets share a singleton `MLRunContextManager` that manages the MLRun project and context lifecycle:

```python
from kedro_datasets_experimental.mlrun import MLRunContextManager

ctx_manager = MLRunContextManager()
project = ctx_manager.project  # Access current MLRun project
context = ctx_manager.context  # Access MLRun execution context
```

### Running locally vs. in an MLRun job

- **Context name (`MLRUN_CONTEXT_NAME`)**: When running **locally** (not as an MLRun job), MLRun sets the context name automatically if you do not define `MLRUN_CONTEXT_NAME`; it is typically the MLRun function name. You can override it with the `MLRUN_CONTEXT_NAME` environment variable (when unset in an MLRun job, this integration uses the default `"kedro-mlrun-ctx"`). See the [MLRun execution and context docs](https://docs.mlrun.org/en/latest/?badge=latest) for details.
- **Project (local runs)**: When running **locally**, set the active project so MLRun knows where to store artifacts and results, for example:
  ```bash
  export MLRUN_ACTIVE_PROJECT=my-project
  ```
  See [MLRun project and environment configuration](https://docs.mlrun.org/en/latest/?badge=latest) for your MLRun version.

## Usage Examples

### Training Pipeline

```python
# nodes.py
def train_model(X_train, y_train):
    from sklearn.ensemble import RandomForestClassifier
    model = RandomForestClassifier()
    model.fit(X_train, y_train)
    return model

def evaluate_model(model, X_test, y_test):
    accuracy = model.score(X_test, y_test)
    return {"accuracy": accuracy}
```

```yaml
# catalog.yml
trained_model:
  type: kedro_datasets_experimental.mlrun.MLRunModel
  key: rf_classifier
  framework: sklearn

evaluation_metrics:
  type: kedro_datasets_experimental.mlrun.MLRunResult
  key: eval_metrics
  flatten: true
```

### Custom Framework

```yaml
xgboost_model:
  type: kedro_datasets_experimental.mlrun.MLRunModel
  key: xgb_regressor
  framework: xgboost
  model_format: pkl
  save_args:
    labels:
      - production
      - v1.0
```

## Troubleshooting

### MLRun Project Not Found

```
mlrun.errors.MLRunNotFoundError: Project not found
```

**Solution**: Ensure you have an active MLRun project. When running locally, set:
```bash
export MLRUN_ACTIVE_PROJECT=my-project
```
You can also set it in code: `import mlrun; mlrun.set_environment(project="my-project")`. See [MLRun project and environment configuration](https://docs.mlrun.org/en/latest/?badge=latest) for details.

### Context Initialization Error

```
RuntimeError: MLRun context could not be created
```

**Solution**: Verify MLRun is properly configured:
- Check `MLRUN_DBPATH` environment variable
- Ensure MLRun API is accessible

### Model Loading Fails

```
FileNotFoundError: Model artifact not found
```

**Solution**:
- Verify the artifact key exists in the MLRun project
- Check that the model was saved with the same key

## Related Resources

- [MLRun Documentation](https://docs.mlrun.org/en/latest/?badge=latest) — check your MLRun version for supported `load_args`/`save_args`
- [MLRun execution API (MLClientCtx)](https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html) — `log_artifact`, `log_model`, `log_result`, `get_artifact`
- [Kedro Documentation](https://docs.kedro.org/)
- [MLRun GitHub](https://github.com/mlrun/mlrun)
