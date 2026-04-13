# Opik Integration

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![Kedro](https://img.shields.io/badge/kedro-compatible-green)](https://kedro.org/)
[![Opik](https://img.shields.io/badge/opik-integration-purple)](https://www.comet.com/site/products/opik/)

## OpikPromptDataset
A Kedro dataset for seamless AI prompt management with Opik versioning, synchronisation, and experiment tracking. Supports both LangChain integration and direct SDK usage with flexible sync policies for development and production workflows.

### Quick Start

```python
from kedro_datasets_experimental.opik import OpikPromptDataset

# Load and use a prompt
dataset = OpikPromptDataset(
    filepath="prompts/customer_support.json",
    prompt_name="customer_support_v1",
    credentials={
        "api_key": "opik_...",  # pragma: allowlist secret
        "workspace": "my-workspace",
    },
)

# Returns Opik Prompt object
prompt = dataset.load()
```

### Installation

#### SDK Mode Only
For basic Opik integration without LangChain dependencies:
```bash
pip install "kedro-datasets[opik-opikpromptdataset]"
```

#### Full Installation
For complete functionality including LangChain integration:
```bash
pip install "kedro-datasets[opik]"
```

#### Requirements:
- Python 3.10+
- Kedro
- Opik SDK
- LangChain Core (optional, for `mode="langchain"`)

### Core Features

#### File Format Support

##### JSON Format
```json
[
  {
    "role": "system",
    "content": "You are a helpful assistant."
  },
  {
    "role": "user",
    "content": "Hello, {question}"
  }
]
```

##### YAML Format
```yaml
- role: system
  content: You are a helpful assistant.
- role: user
  content: "Hello, {question}"
```

#### Prompt Types

##### Text Prompts
Simple string templates with variable placeholders:
```json
"Answer the following question concisely: {question}"
```

##### Chat Prompts
Conversational format with role-based messages:
```json
[
  {
    "role": "system",
    "content": "You are a helpful customer support assistant."
  },
  {
    "role": "user",
    "content": "{customer_query}"
  }
]
```

#### Sync Policies

| Policy | Local File | Remote (Opik) | Use Case |
|--------|------------|---------------|----------|
| **`local`** (default) | ✅ Source of truth | ⬆️ Synced from local | Development, rapid iteration |
| **`remote`** | ⬇️ Synced from Opik | ✅ Source of truth | Production, team collaboration |
| **`strict`** | ✅ Must match remote | ✅ Must match local | Critical deployments, validation |

##### Choosing the Right Policy

- **Development**: Use `local` - iterate quickly on prompts in your IDE
- **Staging**: Use `remote` to test production prompts
- **Production**: Use `remote` to ensure consistency across deployments
- **CI/CD**: Use `strict` to validate prompt consistency

#### Modes

##### SDK Mode (default)
Returns raw Opik Prompt objects for maximum flexibility:

```python
dataset = OpikPromptDataset(
    filepath="prompts/customer.json",
    prompt_name="customer_support_v1",
    credentials={
        "api_key": "opik_...",  # pragma: allowlist secret
        "workspace": "my-workspace",
    },
)

# Returns Opik Prompt object
prompt_obj = dataset.load()

# Access prompt content
content = prompt_obj.prompt

# Access metadata
metadata = prompt_obj.metadata
```

##### LangChain Mode
Returns ready-to-use `ChatPromptTemplate` objects:
```python
dataset = OpikPromptDataset(mode="langchain", ...)

# ChatPromptTemplate object
template = dataset.load()

# Ready to use with LangChain
formatted = template.format(question="What is Kedro?")
```

### Configuration Examples

#### Catalog Configuration (YAML)

##### Local Sync Policy - Development

```yaml
dev_prompt:
  type: kedro_datasets_experimental.opik.OpikPromptDataset
  filepath: data/prompts/customer_support.json
  prompt_name: customer_support_v1
  prompt_type: chat
  credentials: opik_credentials
  sync_policy: local  # Local files are source of truth
  mode: langchain
  save_args:
    metadata:
      environment: development
      version: "2.1"
```

##### Remote Sync Policy - Production
```yaml
production_prompt:
  type: kedro_datasets_experimental.opik.OpikPromptDataset
  filepath: data/prompts/production.json
  prompt_name: customer_support_v1
  prompt_type: chat
  credentials: opik_credentials
  sync_policy: remote  # Opik is source of truth
  mode: sdk
```

##### Strict Sync Policy - CI/CD
```yaml
validation_prompt:
  type: kedro_datasets_experimental.opik.OpikPromptDataset
  filepath: data/prompts/validation.yaml
  prompt_name: customer_support_v1
  prompt_type: chat
  credentials: opik_credentials
  sync_policy: strict  # Error if local and remote differ
  mode: sdk
```

#### Python API Examples

##### Basic Usage
```python
from kedro_datasets_experimental.opik import OpikPromptDataset

# Minimal configuration
dataset = OpikPromptDataset(
    filepath="prompts/support.json",
    prompt_name="support_assistant",
    credentials={
        "api_key": "opik_...",  # pragma: allowlist secret
        "workspace": "my-workspace",
    },
)
```

##### Advanced Configuration
```python
# Full configuration with metadata
dataset = OpikPromptDataset(
    filepath="prompts/assistant.yaml",
    prompt_name="assistant_v2",
    prompt_type="chat",
    sync_policy="remote",
    mode="langchain",
    credentials={
        "api_key": "opik_...",  # pragma: allowlist secret
        "workspace": "my-workspace",
        "project_name": "customer-support",
    },
    save_args={
        "metadata": {
            "environment": "production",
            "team": "ml-ops",
            "version": "2.0.0"
        }
    },
)
```

#### Credentials Management

##### Configuration
```yaml
# conf/local/credentials.yml
# Store securely - not in version control
opik_credentials:
  api_key: "opik_your_api_key"  # pragma: allowlist secret
  workspace: "your-workspace"
  project_name: "your-project"  # optional
```

### Real-World Use Cases

##### Customer Support Assistant
```python
# Dynamic customer support responses
support_dataset = OpikPromptDataset(
    filepath="prompts/support.json",
    prompt_name="support_assistant_v2",
    prompt_type="chat",
    mode="langchain",
)

template = support_dataset.load()
response = template.format(
    customer_name="Alice",
    issue="billing inquiry",
    context="Previous interaction: password reset"
)
```

##### Code Generation
```python
# Code generation prompts
code_dataset = OpikPromptDataset(
    filepath="prompts/code_gen.yaml",
    prompt_name="python_generator",
    prompt_type="text",
    sync_policy="remote",
)

prompt_obj = code_dataset.load()
# Use with your preferred LLM
code_prompt = prompt_obj.prompt.format(
    task="Create a function to validate email addresses"
)
```

##### RAG Applications
```python
# Retrieval-Augmented Generation
rag_dataset = OpikPromptDataset(
    filepath="prompts/rag.json",
    prompt_name="rag_synthesizer",
    prompt_type="chat",
    sync_policy="remote",
    mode="langchain",
)

template = rag_dataset.load()
final_prompt = template.format(
    context="\n".join(retrieved_chunks),
    question=user_question,
    instructions="Cite sources when possible.",
)
```

### Advanced Features

#### Metadata Management
```python
# Track prompt versions with metadata
dataset = OpikPromptDataset(
    save_args={
        "metadata": {
            "version": "2.1.0",
            "author": "ml-team",
            "tested": True,
            "performance_score": 0.95,
            "tags": ["production", "stable"],
        }
    }
)

# Metadata is stored with each prompt version in Opik
dataset.save(prompt_content)
```

#### Experiment Tracking Integration
Opik automatically tracks prompt versions as part of your ML experiments:
```python
# Prompts are versioned and tracked in Opik datasets
# Access via Opik UI to compare prompt performance across experiments
dataset = OpikPromptDataset(
    filepath="prompts/experiment.json",
    prompt_name="experiment_prompt_v3",
    credentials=opik_credentials,
)

# Each save creates a new tracked version
dataset.save(updated_prompt)
```

### Integration Examples

#### Kedro Pipeline Integration
```python
# nodes.py
def process_with_prompt(prompt_template: ChatPromptTemplate, user_input: str):
    formatted_prompt = prompt_template.format(input=user_input)
    # Process with your LLM
    return llm_response


# pipeline.py
from kedro.pipeline import Pipeline, Node


def create_pipeline():
    return Pipeline(
        [
            Node(
                func=process_with_prompt,
                inputs=["customer_prompt", "user_input"],
                outputs="llm_response",
            )
        ]
    )
```

### Troubleshooting

#### Missing Credentials
```
DatasetError: Failed to initialise Opik client
```

##### Solution: Ensure all required credentials are provided:
```python
credentials = {
    "api_key": "opik_...",  # pragma: allowlist secret
    "workspace": "your-workspace",
}
```

---

#### Unsupported File Extension
```
NotImplementedError: Unsupported file extension '.txt'
```

##### Solution: Use supported formats: `.json`, `.yaml`, or `.yml`

---

#### Sync Conflicts
```
DatasetError: Strict sync failed: local and remote prompts differ
```

##### Solution:
- Use `sync_policy="local"` to prefer local files
- Use `sync_policy="remote"` to prefer Opik versions
- Manually resolve conflicts and re-sync

---

#### Import Errors
```
ImportError: The 'langchain-core' package is required when using mode='langchain'
```

##### Solution:
```bash
pip install "kedro-datasets[opik]"  # Full installation
```

---

#### Missing Remote Prompt
```
DatasetError: Remote sync policy specified but no remote prompt exists in Opik
```

##### Solution:
- Create prompt in Opik first using the web UI
- Or switch to `sync_policy="local"` to create from local file
- Or use the Opik Python SDK directly

---

### Support

#### Issues
- **Bug Reports**: [kedro-plugins/issues](https://github.com/kedro-org/kedro-plugins/issues)

#### Related Resources
- **Opik Documentation**: [Opik Platform](https://www.comet.com/docs/opik/)
- **Kedro Documentation**: [Kedro Datasets](https://docs.kedro.org/)

---

## OpikEvaluationDataset

A Kedro dataset for managing LLM evaluation datasets with Opik. Supports a local JSON/YAML file as the authoring surface and keeps it in sync with a remote Opik dataset, or delegates entirely to the remote dataset in production.

### Quick Start

```python
from kedro_datasets_experimental.opik import OpikEvaluationDataset
from opik.evaluation import evaluate

dataset = OpikEvaluationDataset(
    dataset_name="intent-detection-eval",
    credentials={"api_key": "opik_..."},  # pragma: allowlist secret
    filepath="data/evaluation/intent_items.json",
)

# Load returns an opik.Dataset ready for experiments
eval_dataset = dataset.load()
evaluate(
    dataset=eval_dataset,
    task=my_task,
    scoring_functions=[my_scorer],
    experiment_name="my-experiment",
)
```

### Installation

```bash
pip install "kedro-datasets[opik-opikevaluationdataset]"
```

#### Requirements
- Python 3.10+
- Kedro
- Opik SDK (`opik`)

### Core Features

#### Item Format

The local file and `save()` data must be a list of dicts:

```json
[
  {
    "id": "q1",
    "input": {"text": "cancel my order"},
    "expected_output": "cancel_order",
    "metadata": {"source": "production"}
  }
]
```
("q1" is used for local deduplication only, as it is not a UUID v7 and will be stripped on upload)

| Field | Required | Notes |
|-------|----------|-------|
| `input` | Yes | The evaluation input payload |
| `id` | No | Used for local deduplication. Upload behaviour depends on the value: **valid UUID v7** — forwarded to Opik; Opik's API upserts by item ID, so the first sync creates the remote row and subsequent syncs update it in-place (when the content is changed, the already existing row is updated). **All other values** (human-readable strings, other UUID versions, `None`, empty string, or absent) — stripped before upload; Opik auto-generates a new UUID v7 every sync, so a new remote row is created on every sync regardless of content, while the already existing row remains in place. |
| `expected_output` | No | Ground-truth value for scoring |
| `metadata` | No | Arbitrary metadata dict attached to the item |

#### Sync Policies

| Policy | Local File | Remote (Opik) | Use Case |
|--------|------------|---------------|----------|
| **`local`** (default) | Source of truth | All local items are re-inserted on every `load()` via Opik's upsert-by-ID API. Items with a UUID v7 `id` update the existing remote row in-place, without creating a new row; items without a UUID v7 `id` receive a new auto-generated UUID on every sync and create a new remote row each time, while the previous row still remains. | Authoring, development, initial seeding |
| **`remote`** | Not touched | Source of truth | Production, read-only experiments |

> **Note on `remote` mode and empty datasets:** `sync_policy="remote"` never pushes items from the local file to Opik. If the remote dataset does not exist yet, `load()` creates it empty and returns it with no items — experiments run against it will have nothing to evaluate. Before using remote mode, ensure the dataset has been populated by either running with `sync_policy="local"` at least once, or creating and populating the dataset directly via the Opik UI.

### Configuration Examples

#### Catalog Configuration (YAML)

##### Local sync policy - local file seeds and syncs to remote

```yaml
evaluation_dataset:
  type: kedro_datasets_experimental.opik.OpikEvaluationDataset
  dataset_name: intent-detection-eval
  filepath: data/evaluation/intent_items.json
  sync_policy: local
  credentials: opik_credentials
  metadata:
    project: intent-detection
```

##### Remote sync policy - Opik is the source of truth

```yaml
production_eval:
  type: kedro_datasets_experimental.opik.OpikEvaluationDataset
  dataset_name: intent-detection-eval
  sync_policy: remote
  credentials: opik_credentials
```

#### Credentials Management

```yaml
# conf/local/credentials.yml
opik_credentials:
  api_key: "opik_your_api_key"  # pragma: allowlist secret
  workspace: "your-workspace"   # optional
  project_name: "your-project"  # optional
```

#### Kedro Pipeline Integration

```python
# nodes.py
from opik.evaluation import evaluate


def run_experiment(eval_dataset, task, scorer, experiment_name: str, model_name: str):
    evaluate(
        dataset=eval_dataset,
        task=task,
        scoring_functions=[scorer],
        experiment_name=experiment_name,
        experiment_config={"model": model_name},
    )


# pipeline.py
from kedro.pipeline import Pipeline, node


def create_pipeline():
    return Pipeline(
        [
            node(
                func=run_experiment,
                inputs=[
                    "evaluation_dataset",
                    "my_task",
                    "my_scorer",
                    "params:experiment_name",
                    "params:model_name",
                ],
                outputs=None,
                name="run_experiment_node",
            )
        ]
    )
```

### Troubleshooting

#### Missing Credentials

```
DatasetError: Missing required Opik credential: 'api_key'
```

**Solution:** Add `api_key` to your credentials file.

---

#### Unsupported File Extension

```
DatasetError: Unsupported file extension '.txt'. Supported formats: .json, .yaml, .yml
```

**Solution:** Use `.json`, `.yaml`, or `.yml`.

---

#### Item Missing `input` Key

```
DatasetError: Dataset item at index 0 is missing required 'input' key.
```

**Solution:** Every item in the local file and passed to `save()` must contain an `input` key.

---

#### Remote dataset is empty / experiment has no items

**Symptom:** `sync_policy="remote"` run completes without error but the Opik dataset has no items and the experiment results are empty.

**Cause:** `remote` mode never reads from the local file. If the dataset did not exist, it was created empty. If it existed but had no items, it is returned as-is.

**Solution:** Populate the remote dataset first using one of:
- Run the pipeline once with `sync_policy="local"` to seed from your local file, then switch back to `"remote"`.
- Create and populate the dataset directly via the [Opik UI](https://www.comet.com/docs/opik/).

---

## Migrating from LangfuseEvaluationDataset to OpikEvaluationDataset

`OpikEvaluationDataset` and `LangfuseEvaluationDataset` share the same constructor signature and local file format. Migrating is a catalog swap plus evaluation pipeline node changes.

> **Item identity behaves differently between platforms.** Langfuse forwards any string `id` to the API for upsert. Opik only forwards `id` values that are valid UUID v7 — all others are stripped and Opik auto-generates a new UUID v7 on every sync, creating a new remote row each time. If your local items use human-readable or non-UUID v7 `id` values, those items will accumulate new remote rows on every sync after migrating. To preserve stable remote identity, update your item `id` fields to valid UUID v7 values before switching to Opik.

### Step 1 - Update the catalog entry

| | Langfuse | Opik |
|---|---|---|
| `type` | `kedro_datasets_experimental.langfuse.LangfuseEvaluationDataset` | `kedro_datasets_experimental.opik.OpikEvaluationDataset` |
| `credentials` key | `public_key` + `secret_key` | `api_key` |
| Optional credential keys | `host` | `workspace`, `host`, `project_name` |
| `version` param | ✅ Supported (ISO 8601 snapshot pinning) | ❌ Not available |
| Other params (`dataset_name`, `filepath`, `sync_policy`, `metadata`) | - | Identical |

**Before (Langfuse):**

```yaml
evaluation_dataset:
  type: kedro_datasets_experimental.langfuse.LangfuseEvaluationDataset
  dataset_name: intent-detection-eval
  filepath: data/evaluation/intent_items.json
  sync_policy: local
  credentials: langfuse_credentials

# conf/local/credentials.yml
langfuse_credentials:
  public_key: "pk_..."
  secret_key: "sk_..."  # pragma: allowlist secret
```

**After (Opik):**

```yaml
evaluation_dataset:
  type: kedro_datasets_experimental.opik.OpikEvaluationDataset
  dataset_name: intent-detection-eval
  filepath: data/evaluation/intent_items.json
  sync_policy: local
  credentials: opik_credentials

# conf/local/credentials.yml
opik_credentials:
  api_key: "opik_..."  # pragma: allowlist secret
```

### Step 2 - No changes to the local data file

The item format (`input`, `id`, `expected_output`, `metadata`) is identical. No edits to your JSON/YAML evaluation file are needed.

### Step 3 - Update evaluation pipeline nodes

The experiment runner API differs between platforms.

#### Experiment runner

| | Langfuse | Opik |
|---|---|---|
| Method | `dataset.run_experiment(...)` | `opik.evaluation.evaluate(dataset, ...)` |
| Scorer list key | `evaluators=[...]` | `scoring_functions=[...]` |
| Experiment config key | `metadata={...}` | `experiment_config={...}` |
| Return value | Result object with `.format()` | `None` |

#### Scorer / evaluator signature

**Langfuse** - three separate args, returns `Evaluation`:

```python
def my_scorer(input: dict, output: dict, expected_output: dict) -> Evaluation:
    score = compute_score(output, expected_output)
    return Evaluation(name="accuracy", value=score, comment="...")
```

**Opik** - two args (`dataset_item` bundles input + expected_output), returns `ScoreResult`:

```python
from opik.evaluation.metrics import base_metric, score_result


class MyScorer(base_metric.BaseMetric):
    def score(self, dataset_item: dict, task_outputs: dict, **kwargs) -> score_result.ScoreResult:
        expected = dataset_item.get("expected_output", "")
        actual = task_outputs.get("output", "")
        value = float(actual == expected)
        return score_result.ScoreResult(name=self.name, value=value, reason="...")
```

#### Task signature

**Langfuse** - task receives a `DatasetItem` object (attribute access):

```python
def my_task(*, item, **kwargs) -> dict:
    question = item.input.get("question", "")  # attribute access
    return {"output": llm(question)}
```

**Opik** - task receives a plain dict:

```python
def my_task(dataset_item: dict) -> dict:
    question = dataset_item.get("input", {}).get("question", "")  # dict access
    return {"output": llm(question)}
```

### What stays the same

- `dataset_name`, `filepath`, `sync_policy`, `metadata` constructor params
- Local file format (item schema is identical)
- Context nodes that don't interact with the evaluation dataset
- Sync semantics: `local` re-inserts from local file on every load (upsert-by-ID: UUID v7 items update in-place, non-UUID items create new rows each sync); `remote` fetches as-is

### Known limitations

- **`metadata` is local-only**: Opik's `create_dataset()` does not accept a `metadata` argument. The `metadata` param is stored and returned by `_describe()` but is not propagated to the remote dataset (unlike Langfuse, which passes it through).
- **No snapshot versioning**: Opik does not support pinning `load()` to a historical snapshot. The `version` param from `LangfuseEvaluationDataset` has no Opik equivalent.
- **UUID v7 `id` values are forwarded; Opik upserts by item ID**: If a local item's `id` is a valid UUID v7, it is passed to Opik's `create_or_update` API, which upserts by item ID — the first sync creates the remote row; subsequent syncs update that same row in-place (content changes replace the row; unchanged content is a no-op). Items without a valid UUID v7 `id` have it stripped before upload; Opik auto-generates a new UUID v7 each sync, so those items create a new remote row on every sync, even when the content is unchanged.

### Support

#### Issues
- **Bug Reports**: [kedro-plugins/issues](https://github.com/kedro-org/kedro-plugins/issues)

#### Related Resources
- **Opik Documentation**: [Opik Evaluation](https://www.comet.com/docs/opik/evaluation/overview)
- **Kedro Academy**: [Agentic Workflows](https://github.com/kedro-org/kedro-academy/tree/main/kedro-agentic-workflows)
