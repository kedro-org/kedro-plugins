# Kedro Validator

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://pypi.org/project/kedro-validator/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Kedro Version](https://img.shields.io/badge/kedro-1.0.0%2B-orange.svg)](https://kedro.org)

A Kedro plugin that provides early parameter validation using Pydantic models and dataclasses for type-safe, fail-fast pipeline execution. Validates parameters during context creation and automatically converts validated configurations into strongly-typed instances for your pipeline nodes.

## Overview

Kedro Validator introduces optional native Pydantic-based parameter validation to Kedro projects, enabling early validation of parameters before pipeline execution. This plugin helps catch configuration errors early, provides better type safety, and improves developer experience with clear validation error messages.

### Key Benefits

- **Fail-fast validation**: Catch parameter errors before pipeline execution starts
- **Type safety**: Leverage Python's type system with runtime validation
- **Automatic type conversion**: Parameters are automatically converted to strongly-typed instances
- **Clear error messages**: Get detailed feedback on validation failures
- **Nested parameter support**: Full support for nested parameter structures
- **Backward compatible**: Optional dependency that doesn't affect existing projects
- **Future-ready**: Foundation for JSON Schema export and UI auto-generation

## Features

- **Early Parameter Validation**: Validates parameters during Kedro context creation
- **Runtime Type Conversion**: Automatically converts validated parameters to typed instances
- **Pydantic Support**: Full support for Pydantic BaseModel with Field constraints
- **Dataclass Support**: Native support for Python dataclasses
- **Nested Parameters**: Support for complex nested parameter structures with dot notation
- **Comprehensive Error Reporting**: Detailed error messages with node and pipeline context
- **Optional Dependencies**: Pydantic is an optional dependency for backward compatibility
- **Performance Optimized**: Minimal impact on session creation time
- **Hook-based Architecture**: Integrates seamlessly with Kedro's plugin system

## Installation

### Basic Installation
```bash
uv pip install kedro-validator
```

### With Pydantic Support
```bash
uv pip install kedro-validator[pydantic]
```

### Development Installation
```bash
git clone https://github.com/kedro-org/kedro-plugins.git
cd kedro-plugins/kedro-validator
uv pip install -e ".[pydantic]"
```

**Requirements:**
- python >= 3.9
- kedro >= 1.0.0
- pydantic >=2.0,<3.0 (optional)

## Quick Start - Testing with Spaceflights-Pandas Starter Project

### 1. Create the Spaceflights Project
   ```bash
   kedro new --starter=spaceflights-pandas
   cd spaceflights-pandas
   ```

### 2. Install Kedro Validator
   ```bash
   uv pip install kedro-validator[pydantic]
   ```

### 3. Define Parameter Models

**Using Pydantic (Recommended):**
```python
# src/your_project/models.py
from pydantic import BaseModel, Field
from typing import List


class TrainingConfig(BaseModel):
    test_size: float = Field(
        ..., description="Proportion of test split (e.g. 0.2 for 20%)"
    )
    random_state: int = Field(..., description="Random seed for reproducibility")
    features: List[str] = Field(
        ..., description="List of feature names used for training"
    )
```

**Using Dataclasses:**
```python
# src/your_project/models.py
from dataclasses import dataclass, field
from typing import List


@dataclass
class TrainingConfigDC:
    test_size: float
    random_state: int
    features: List[str] = field(default_factory=list)

    def __post_init__(self):
        if not (0 < self.test_size < 1):
            raise ValueError(f"test_size must be between 0 and 1, got {self.test_size}")
        if not isinstance(self.random_state, int):
            raise TypeError(
                f"random_state must be int, got {type(self.random_state).__name__}"
            )
        if not self.features:
            raise ValueError("features list cannot be empty")
        if not all(isinstance(f, str) for f in self.features):
            raise TypeError("features must be a list of strings")
```

### 4. Annotate Node Functions

```python
# src/your_project/pipelines/data_science/nodes.py
import pandas as pd


def split_data(data: pd.DataFrame, config: TrainingConfig) -> tuple:
    """Splits data into features and targets training and test sets.

    Args:
        data: Data containing features and target.
        config: Validated training configuration instance.
    Returns:
        Split data.
    """
    # Note: config is now a TrainingConfig instance, not a dictionary
    X = data[config.features]  # Use attribute access
    y = data["price"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=config.test_size, random_state=config.random_state
    )
    return X_train, X_test, y_train, y_test
```

### 5. Configure Parameters

```yaml
# conf/base/parameters_data_science.yml
training_config:
  test_size: 0.2
  random_state: 3
  features:
    - engines
    - passenger_capacity
    - crew
    - d_check_complete
    - moon_clearance_complete
    - iata_approved
    - company_rating
    - review_scores_rating
```

### 6. Update Pipeline

```python
# src/your_project/pipelines/data_science/pipeline.py
from kedro.pipeline import Pipeline, node, pipeline

from .nodes import evaluate_model, split_data, train_model


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=split_data,
                inputs=["model_input_table", "params:training_config"],
                outputs=["X_train", "X_test", "y_train", "y_test"],
                name="split_data_node",
            ),
            node(
                func=train_model,
                inputs=["X_train", "y_train"],
                outputs="regressor",
                name="train_model_node",
            ),
            node(
                func=evaluate_model,
                inputs=["regressor", "X_test", "y_test"],
                outputs=None,
                name="evaluate_model_node",
            ),
        ]
    )
```

### 7. Run with Validation

```bash
kedro run
```

If validation fails (replace a string value in param `random_state: some`), you'll get clear error messages:

```
RuntimeError: Parameter validation failed:
- pipeline=__default__ node=split_data_node param=training_config: pydantic validation failed: 1 validation error for TrainingConfig
random_state
  Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value='some', input_type=str]
    For further information visit https://errors.pydantic.dev/2.11/v/int_parsing
- pipeline=data_science node=split_data_node param=training_config: pydantic validation failed: 1 validation error for TrainingConfig
random_state
  Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value='some', input_type=str]
    For further information visit https://errors.pydantic.dev/2.11/v/int_parsing
```

## How It Works

The Kedro Validator plugin operates in two phases:

### 1. Validation Phase (Context Creation)
- **When**: During `kedro run` startup, after context creation
- **What**: Validates all `params:*` inputs against function type annotations
- **Result**: Fails fast with clear error messages if validation fails

### 2. Type Conversion Phase (Node Execution)
- **When**: Before each node runs
- **What**: Automatically converts raw parameter dictionaries to validated typed instances
- **Result**: Your functions receive strongly-typed objects instead of dictionaries

```python
# Before: Dictionary access (error-prone)
def old_way(data: pd.DataFrame, parameters: dict) -> tuple:
    test_size = parameters["test_size"]  # Could fail at runtime
    features = parameters["features"]  # No type safety


# After: Attribute access (type-safe)
def new_way(data: pd.DataFrame, config: TrainingConfig) -> tuple:
    test_size = config.test_size  # Type-safe, validated
    features = config.features  # IDE autocomplete support
```

## Advanced Usage

### Nested Parameter Models

Kedro Validator supports complex nested parameter structures:

```python
class DatabaseConfig(BaseModel):
    host: str
    port: int = Field(ge=1, le=65535)
    database: str


class ProcessingConfig(BaseModel):
    algorithm: str = Field(pattern="^(linear|tree|neural)$")
    hyperparams: dict


class ProjectParams(BaseModel):
    database: DatabaseConfig
    processing: ProcessingConfig


# Your node function receives a fully validated nested structure
def process_data(data: pd.DataFrame, config: ProjectParams) -> dict:
    # All nested attributes are type-safe and validated
    connection_string = f"postgresql://{config.database.host}:{config.database.port}/{config.database.database}"
    algorithm = config.processing.algorithm
    return {"connection": connection_string, "algorithm": algorithm}
```

```yaml
# conf/base/parameters.yml
project:
  database:
    host: "localhost"
    port: 5432
    database: "kedro_db"
  processing:
    algorithm: "linear"
    hyperparams:
      learning_rate: 0.01
```

```python
# Pipeline usage
node(
    func=process_data,
    inputs=["raw_data", "params:project"],  # Uses entire project config
    outputs="processed_data",
    name="process_data_node",
)

# Or access nested parameters directly
node(
    func=connect_to_db,
    inputs=["params:project.database"],  # Uses only database config
    outputs="connection",
    name="connect_node",
)
```

### Custom Validators

```python
from pydantic import BaseModel, Field, validator


class CustomParams(BaseModel):
    email: str = Field(regex=r"^[^@]+@[^@]+\.[^@]+$")

    @validator("email")
    def validate_email_domain(cls, v):
        if not v.endswith("@company.com"):
            raise ValueError("Email must be from company.com domain")
        return v
```

## Contributing

### Development Setup

```bash
git clone https://github.com/kedro-org/kedro-plugins.git
cd kedro-plugins/kedro-validator
pip install -e .[pydantic,test,lint]
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=kedro_validator

# Run specific test
pytest tests/test_plugin.py::test_validation
```

### Code Style

This project uses:
- `black` for code formatting
- `ruff` for linting
- `mypy` for type checking

```bash
# Format code
black kedro_validator

# Lint code
ruff check kedro_validator

# Type check
mypy kedro_validator
```

## API Reference

### Hook Implementation

The plugin implements two hooks for complete parameter validation and type conversion:

#### 1. Validation Hook
```python
@hook_impl
def after_context_created(self, context) -> None:
    """Validate parameters against function annotations during context creation.

    This hook:
    - Inspects all pipeline nodes for params:* inputs
    - Validates raw parameter values against function type annotations
    - Stores validated model instances for later use
    - Fails fast with detailed error messages if validation fails
    """
```

#### 2. Type Conversion Hook
```python
@hook_impl
def before_node_run(self, node, catalog, inputs, is_async, run_id):
    """Replace parameter dictionaries with validated model instances before node execution.

    This hook:
    - Intercepts node execution
    - Replaces params:* inputs with validated typed instances
    - Enables attribute-based access instead of dictionary access
    - Maintains full type safety throughout pipeline execution
    """
```

### Supported Annotations

- **Pydantic BaseModel**: Full validation with Field constraints
- **Dataclasses**: Basic type and structure validation. You can add custom validators

### Error Types

- **Missing Parameter**: Required parameter not found in config
- **Validation Error**: Pydantic/dataclass validation failed
- **Type Error**: Unexpected type or structure


## Roadmap

### Planned Features

- **JSON Schema Export**: Generate schemas for UI tools
- **Kedro-Viz Integration**: Display parameter schemas in visualization

### Community Feedback

We welcome feedback and contributions! Please:
- Report issues on [GitHub](https://github.com/kedro-org/kedro-plugins/issues)
- Join discussions in the Kedro community
- Contribute improvements via pull requests

## License

This project is licensed under the Apache Software License (Apache 2.0) - see the [LICENSE](LICENSE.md) file for details.

## Links

- [Kedro Documentation](https://docs.kedro.org/)
- [Plugin Development Guide](https://github.com/kedro-org/kedro-plugins/blob/main/CONTRIBUTING.md)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [Issue Tracker](https://github.com/kedro-org/kedro-plugins/issues)
