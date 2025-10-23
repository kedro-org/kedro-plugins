# Kedro Validator

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://pypi.org/project/kedro-validator/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Kedro Version](https://img.shields.io/badge/kedro-1.0.0%2B-orange.svg)](https://kedro.org)

A Kedro plugin that provides early parameter validation using Pydantic models and dataclasses for type-safe, fail-fast pipeline execution.

## Overview

Kedro Validator introduces optional native Pydantic-based parameter validation to Kedro projects, enabling early validation of parameters before pipeline execution. This plugin helps catch configuration errors early, provides better type safety, and improves developer experience with clear validation error messages.

### Key Benefits

- **Fail-fast validation**: Catch parameter errors before pipeline execution starts
- **Type safety**: Leverage Python's type system with runtime validation
- **Clear error messages**: Get detailed feedback on validation failures
- **Backward compatible**: Optional dependency that doesn't affect existing projects
- **Future-ready**: Foundation for JSON Schema export and UI auto-generation

## Features

- **Early Parameter Validation**: Validates parameters during Kedro context creation
- **Pydantic Support**: Full support for Pydantic BaseModel with Field constraints
- **Dataclass Support**: Native support for Python dataclasses
- **Comprehensive Error Reporting**: Detailed error messages with node and pipeline context
- **Optional Dependencies**: Pydantic is an optional dependency for backward compatibility
- **Performance Optimized**: Minimal impact on session creation time
- **Hook-based Architecture**: Integrates seamlessly with Kedro's plugin system

## Installation

### Basic Installation
```bash
pip install kedro-validator
```

### With Pydantic Support
```bash
pip install kedro-validator[pydantic]
```

### Development Installation
```bash
git clone https://github.com/kedro-org/kedro-plugins.git
cd kedro-plugins/kedro-validator
pip install -e .[pydantic]
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
   pip install kedro-validator[pydantic]
   ```

### 3. Define Parameter Models

**Using Pydantic (Recommended):**
```python
# src/your_project/models.py
from pydantic import BaseModel, Field
from typing import List


class ModelOptions(BaseModel):
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
class ModelOptionsDC:
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


def split_data(data: pd.DataFrame, parameters: ModelOptions) -> tuple:
    """Splits data into features and targets training and test sets.

    Args:
        data: Data containing features and target.
        parameters: Parameters defined in parameters/data_science.yml.
    Returns:
        Split data.
    """
    X = data[parameters["features"]]
    y = data["price"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=parameters["test_size"], random_state=parameters["random_state"]
    )
    return X_train, X_test, y_train, y_test
```

### 5. Configure Parameters

```yaml
# conf/base/parameters_data_science.yml
model_options:
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
                inputs=["model_input_table", "params:model_options"],
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
- pipeline=__default__ node=split_data_node param=model_options: pydantic validation failed: 1 validation error for ModelOptions
random_state
  Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value='some', input_type=str]
    For further information visit https://errors.pydantic.dev/2.11/v/int_parsing
- pipeline=data_science node=split_data_node param=model_options: pydantic validation failed: 1 validation error for ModelOptions
random_state
  Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value='some', input_type=str]
    For further information visit https://errors.pydantic.dev/2.11/v/int_parsing
```

## Advanced Usage

### Nested Parameter Models

```python
class DatabaseConfig(BaseModel):
    host: str
    port: int = Field(ge=1, le=65535)
    database: str


class MLConfig(BaseModel):
    model_type: str = Field(regex="^(linear|tree|neural)$")
    hyperparams: dict


class ProjectParams(BaseModel):
    database: DatabaseConfig
    ml: MLConfig
```

```yaml
# conf/base/parameters.yml
project:
  database:
    host: "localhost"
    port: 5432
    database: "kedro_db"
  ml:
    model_type: "linear"
    hyperparams:
      learning_rate: 0.01
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

The plugin implements the `after_context_created` hook to perform validation:

```python
@hook_impl
def after_context_created(self, context) -> None:
    """Validate parameters against function annotations."""
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
