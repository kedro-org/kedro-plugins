# Langfuse Integration

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![Kedro](https://img.shields.io/badge/kedro-compatible-green)](https://kedro.org/)
[![Langfuse](https://img.shields.io/badge/langfuse-integration-orange)](https://langfuse.com/)

## LangfusePromptDataset
A Kedro dataset for seamless AI prompt management with Langfuse versioning, synchronization, and team collaboration. Supports both LangChain integration and direct SDK usage with flexible sync policies for development and production workflows.

### Quick Start

```python
from kedro_datasets_experimental.langfuse import LangfusePromptDataset

# Load and use a prompt
dataset = LangfusePromptDataset(
    filepath="prompts/intent.json",
    prompt_name="intent-classifier",
    credentials={
        "public_key": "pk_...",
        "secret_key": "sk_...",  # pragma: allowlist secret
    },
)

# Returns Langfuse prompt object
# langfuse.model.TextPromptClient in case of `prompt_type=text`
# langfuse.model.ChatPromptClient in case of `prompt_type=chat`
prompt = dataset.load()
```

### Installation

#### SDK Mode Only
For basic Langfuse integration without LangChain dependencies:
```bash
pip install "kedro-datasets[langfuse-langfusepromptdataset]"
```

#### Full Installation
For complete functionality including LangChain integration:
```bash
pip install "kedro-datasets[langfuse]"
```

#### Requirements:
- Python 3.10+
- Kedro
- Langfuse SDK
- LangChain (optional, for `mode="langchain"`)

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
    "role": "human",
    "content": "{user_input}"
  }
]
```

##### YAML Format

```yaml
- role: system
  content: You are an expert analyst.
- role: human
  content: "{query}"
```

#### Prompt Types

##### Text Prompts
Simple string templates with variable placeholders:

```json
"Classify the following text as positive, negative, or neutral: {input}"
```

##### Chat Prompts
Conversational format with role-based messages:

```json
[
  {
    "role": "system",
    "content": "You are a helpful insurance support assistant."
  },
  {
    "role": "human",
    "content": "{user_query}"
  }
]
```

#### Sync Policies

| Policy | Local File | Remote (Langfuse) | Use Case |
|--------|------------|-------------------|----------|
| **`local`** (default) | ✅ Source of truth | ⬆️ Synced from local | Development, rapid iteration |
| **`remote`** | ⬇️ Synced from Langfuse | ✅ Source of truth | Production, team collaboration |
| **`strict`** | ✅ Must match remote | ✅ Must match local | Critical deployments, validation |

##### Choosing the Right Policy

- **Development**: Use `local` - iterate quickly on prompts in your IDE
- **Staging**: Use `remote` with specific labels (`label: "staging"`)
- **Production**: Use `remote` with production labels (`label: "production"`)
- **CI/CD**: Use `strict` to ensure consistency across environments

#### Modes

##### SDK Mode (default)
Returns raw Langfuse prompt objects for maximum flexibility:

```python
dataset = LangfusePromptDataset(
    filepath="prompts/intent.json",
    prompt_name="intent-classifier",
    credentials={
        "public_key": "pk_...",
        "secret_key": "sk_...",  # pragma: allowlist secret
    },
)

# Returns Langfuse prompt object
# langfuse.model.TextPromptClient in case of `prompt_type=text`
# langfuse.model.ChatPromptClient in case of `prompt_type=chat`
intent_ds = dataset.load()

# Access raw prompt content
content = intent_ds.prompt

# Access version info
version = intent_ds.version

# Compilation
compiled_prompt = intent_ds.compile(user_query="Hello world!")
```

##### LangChain Mode

Returns ready-to-use `ChatPromptTemplate` objects:

```python
dataset = LangfusePromptDataset(mode="langchain", ...)

# ChatPromptTemplate object
template = dataset.load()

# Ready to use
formatted = template.format(user_query="Hello world")
```

### Configuration Examples

#### Catalog Configuration (YAML)

##### Local Sync Policy - Development

```yaml
intent_prompt:
  type: kedro_datasets_experimental.langfuse.LangfusePromptDataset
  filepath: data/prompts/intent.json
  prompt_name: "intent-classifier"
  prompt_type: "chat"
  credentials: langfuse_credentials
  sync_policy: local  # Local files are source of truth
  mode: langchain
  save_args:
    labels: ["development", "v2.1"]
```

##### Remote Sync Policy - Production
```yaml
production_prompt:
  type: kedro_datasets_experimental.langfuse.LangfusePromptDataset
  filepath: data/prompts/production.json
  prompt_name: "intent-classifier"
  prompt_type: "chat"
  credentials: langfuse_credentials
  sync_policy: remote  # Langfuse is source of truth
  mode: langchain
  load_args:
    label: "production"  # Load specific production version
```

##### Strict Sync Policy - CI/CD

```yaml
validation_prompt:
  type: kedro_datasets_experimental.langfuse.LangfusePromptDataset
  filepath: data/prompts/validation.yaml
  prompt_name: "intent-classifier"
  prompt_type: "chat"
  credentials: langfuse_credentials
  sync_policy: strict  # Error if local and remote differ
  mode: sdk
  load_args:
    version: 5  # Specific version for validation
```

#### Python API Examples

##### Basic Usage
```python
from kedro_datasets_experimental.langfuse import LangfusePromptDataset

# Minimal configuration
dataset = LangfusePromptDataset(
    filepath="prompts/intent.json",
    prompt_name="intent-classifier",
    credentials={
        "public_key": "pk_...",
        "secret_key": "sk_...",  # pragma: allowlist secret
    },
)
```

##### Advanced Configuration
```python
# Full configuration with custom host
dataset = LangfusePromptDataset(
    filepath="prompts/support.yaml",
    prompt_name="customer-support",
    prompt_type="chat",
    sync_policy="remote",
    mode="langchain",
    credentials={
        "public_key": "pk_...",
        "secret_key": "sk_...",  # pragma: allowlist secret
        "host": "https://your-langfuse.com",
    },
    load_args={"label": "staging"},
    save_args={"labels": ["staging", "v1.2"]},
)
```

#### Credentials Management

##### Catalog Configuration
```yaml
# conf/local/credentials.yml
# Store securely and should
# not be part of version control
langfuse_credentials:
  public_key: "pk_your_public_key"
  secret_key: "sk_your_secret_key"  # pragma: allowlist secret
```

### Real-World Use Cases

##### Intent Classification

```python
# Multi-intent classification system
intent_dataset = LangfusePromptDataset(
    filepath="prompts/intent.json",
    prompt_name="intent-classifier",
    prompt_type="chat",
    mode="langchain",
)

template = intent_dataset.load()
prompt = template.format(user_input="I want to file a new claim")
# Returns: classified intent with confidence
```

You can read more about this use case on [kedro-academy](https://github.com/kedro-org/kedro-academy/tree/main/kedro-agentic-workflows#-prompt-management)

##### Response Generation
```python
# Dynamic response generation
response_dataset = LangfusePromptDataset(
    filepath="prompts/response.yaml",
    prompt_name="response-generator",
    prompt_type="chat",
    load_args={"label": "production"},
)

# Generate contextual responses
template = response_dataset.load()
response = template.format(
    context=retrieved_docs, user_query="How do I update my policy?"
)
```

##### RAG Applications
```python
# Retrieval-Augmented Generation
rag_dataset = LangfusePromptDataset(
    filepath="prompts/rag.json",
    prompt_name="rag-synthesizer",
    prompt_type="chat",
    sync_policy="remote",
    load_args={"label": "production"},
)

# Combine retrieved context with user query
template = rag_dataset.load()
final_prompt = template.format(
    context="\n".join(retrieved_chunks),
    question=user_question,
    instructions="Be concise and cite sources.",
)
```

### Advanced Features

#### Version Management

##### Labelling Strategy

```python
# Semantic versioning with labels
dataset.save(prompt_content)  # Auto-creates new version

# Apply labels for organization
dataset = LangfusePromptDataset(
    save_args={"labels": ["v2.1.0", "production", "stable"]}
)
```

##### Version-Specific Loading
```python
# Load specific versions
historical_dataset = LangfusePromptDataset(load_args={"version": 3})  # Load version 3

labeled_dataset = LangfusePromptDataset(
    load_args={"label": "production"}  # Load production label
)
```

#### Configuration Reference

##### Load Args (Remote/Strict Policies Only)

```python
load_args = {
    "version": 3,  # Specific version number
    "label": "production",  # Specific label (preferred over version)
}
```

##### Save Args (All Policies)
```python
save_args = {"labels": ["v2.0", "staging", "experimental"]}  # List of labels
```

### Integration Examples

#### Kedro Pipeline Integration

```python
# nodes.py
def classify_intent(prompt_template: ChatPromptTemplate, user_input: str):
    formatted_prompt = prompt_template.format(input=user_input)
    # Process with your LLM
    return classified_intent


# pipeline.py
from kedro.pipeline import Pipeline, Node


def create_pipeline():
    return Pipeline(
        [
            Node(
                func=classify_intent,
                inputs=["intent_prompt", "user_input"],
                outputs="classified_intent",
            )
        ]
    )
```

### Troubleshooting

#### Missing Credentials

```
DatasetError: Missing required Langfuse credential: 'public_key'
```

##### Solution: Add all required credentials to your configuration:
```python
credentials = {
    "public_key": "pk_...",
    "secret_key": "sk_...",  # pragma: allowlist secret
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
- Use `sync_policy="remote"` to prefer Langfuse versions
- Manually resolve conflicts and re-sync

---

#### Import Errors

```
ImportError: The 'langchain' package is required when using mode='langchain'
```
##### Solution:
```bash
pip install "kedro-datasets[langfuse]"  # Full installation
```

---

#### Invalid Credentials

```
Error when fetching prompt from langfuse: 401 Unauthorized
```

##### Solution:
- Verify public_key and secret_key are correct
- Check if keys have proper permissions
- Ensure host URL is correct for self-hosted instances

---

#### Missing Prompts

```
DatasetError: Remote sync policy specified but no remote prompt exists
```

##### Solution:
- Create prompt in Langfuse first, or
- Switch to `sync_policy="local"` to create from local file

---

## LangfuseEvaluationDataset

A Kedro dataset for managing [Langfuse evaluation datasets](https://langfuse.com/docs/evaluation/experiments/datasets). It connects to a remote Langfuse dataset, optionally backed by a local JSON/YAML file, and returns a `DatasetClient` on `load()` — ready for iterating items or running experiments via `dataset.run_experiment()`.

### Quick Start

```python
from kedro_datasets_experimental.langfuse import LangfuseEvaluationDataset

dataset = LangfuseEvaluationDataset(
    dataset_name="intent-detection-eval",
    credentials={
        "public_key": "pk_...",
        "secret_key": "sk_...",  # pragma: allowlist secret
    },
    filepath="data/evaluation/intent_items.json",
)

# Returns a Langfuse DatasetClient
eval_ds = dataset.load()

for item in eval_ds.items:
    print(item.input, item.expected_output)
```

### Installation

```bash
pip install "kedro-datasets[langfuse-langfuseevaluationdataset]"
```

Or install all Langfuse datasets at once:

```bash
pip install "kedro-datasets[langfuse]"
```

#### Requirements:
- Python 3.10+
- Kedro
- Langfuse SDK ≥ 3.14.0

### Evaluation Item Format

The local file and `save()` data must be a list of dicts. Each item accepts the same keys as [`Langfuse.create_dataset_item()`](https://langfuse.com/docs/evaluation/experiments/datasets#create-items-from-production-data):

| Key | Required | Description |
|-----|----------|-------------|
| `input` | **Yes** | The evaluation input payload |
| `id` | No | Stable identifier used for deduplication on sync and upload |
| `expected_output` | No | Ground-truth value for scoring |
| `metadata` | No | Arbitrary metadata dict attached to the item |
| `source_trace_id` | No | Langfuse trace ID to link the item to |
| `source_observation_id` | No | Observation ID within the source trace |
| `status` | No | `"ACTIVE"` (default) or `"ARCHIVED"` |

##### JSON Example

```json
[
  {
    "id": "q1",
    "input": {"text": "cancel my order"},
    "expected_output": "cancel_order",
    "metadata": {"source": "production"}
  },
  {
    "id": "q2",
    "input": {"text": "where is my package?"},
    "expected_output": "track_order"
  }
]
```

##### YAML Example

```yaml
- id: q1
  input:
    text: cancel my order
  expected_output: cancel_order
  metadata:
    source: production
- id: q2
  input:
    text: where is my package?
  expected_output: track_order
```

> **Note:** Items without an `id` cannot be deduplicated and will be re-uploaded on every `load()` or `save()` call. Always assign unique `id` values for predictable sync behaviour.

### Sync Policies

| Policy | Local File | Remote (Langfuse) | Use Case |
|--------|------------|-------------------|----------|
| **`local`** (default) | ✅ Source of truth | ⬆️ New items synced from local | Development, rapid iteration |
| **`remote`** | ❌ No interaction | ✅ Source of truth | Production, shared datasets |

##### Choosing the Right Policy

- **Development**: Use `local` — iterate on items in your IDE, they sync to remote on `load()`
- **Production / shared datasets**: Use `remote` — manage items via the Langfuse UI or API

### Configuration Examples

#### Catalog Configuration (YAML)

##### Local Sync Policy — Development

```yaml
evaluation_dataset:
  type: kedro_datasets_experimental.langfuse.LangfuseEvaluationDataset
  dataset_name: intent-detection-eval
  filepath: data/evaluation/intent_items.json
  sync_policy: local
  credentials: langfuse_credentials
  metadata:
    project: intent-detection
```

##### Remote Sync Policy — Production

```yaml
production_eval:
  type: kedro_datasets_experimental.langfuse.LangfuseEvaluationDataset
  dataset_name: intent-detection-eval
  sync_policy: remote
  credentials: langfuse_credentials
```

##### Remote + Version — Reproducible Snapshot

```yaml
eval_snapshot:
  type: kedro_datasets_experimental.langfuse.LangfuseEvaluationDataset
  dataset_name: intent-detection-eval
  sync_policy: remote
  version: "2026-01-15T00:00:00Z"
  credentials: langfuse_credentials
```

#### Python API Examples

##### Basic Usage

```python
from kedro_datasets_experimental.langfuse import LangfuseEvaluationDataset

dataset = LangfuseEvaluationDataset(
    dataset_name="intent-detection-eval",
    credentials={
        "public_key": "pk_...",
        "secret_key": "sk_...",  # pragma: allowlist secret
    },
    filepath="data/evaluation/intent_items.json",
)

eval_ds = dataset.load()
```

##### Saving New Items

```python
dataset.save(
    [
        {
            "id": "q3",
            "input": {"text": "I need a refund"},
            "expected_output": "refund_request",
        },
    ]
)
```

##### Versioned Remote Load

```python
dataset = LangfuseEvaluationDataset(
    dataset_name="intent-detection-eval",
    credentials={
        "public_key": "pk_...",
        "secret_key": "sk_...",  # pragma: allowlist secret
    },
    sync_policy="remote",
    version="2026-01-15T00:00:00Z",
)

snapshot = dataset.load()
```

### Running Experiments

The `DatasetClient` returned by `load()` integrates directly with Langfuse's experiment runner. Langfuse manages the experiment lifecycle — tracing, scoring, and result aggregation.

```python
from kedro_datasets_experimental.langfuse import LangfuseEvaluationDataset

dataset = LangfuseEvaluationDataset(
    dataset_name="intent-detection-eval",
    credentials={
        "public_key": "pk_...",
        "secret_key": "sk_...",  # pragma: allowlist secret
    },
)

eval_ds = dataset.load()


def run_intent_classification(item):
    # Your model inference logic
    return classify(item.input["text"])


eval_ds.run_experiment(
    name="intent-model-v2",
    run_fn=run_intent_classification,
)
```

> **Lifecycle delegation:** The dataset only handles creation, sync, and retrieval of evaluation items. Experiment execution, tracing, and scoring are delegated entirely to Langfuse's `DatasetClient` API.

### Troubleshooting

#### Missing Credentials

```
DatasetError: Missing required Langfuse credential: 'public_key'
```

##### Solution: Add all required credentials to your configuration:
```python
credentials = {
    "public_key": "pk_...",
    "secret_key": "sk_...",  # pragma: allowlist secret
}
```

---

#### Unsupported File Extension

```
DatasetError: Unsupported file extension '.txt'
```

##### Solution: Use supported formats: `.json`, `.yaml`, or `.yml`

---

#### Version with Local Sync Policy

```
DatasetError: The 'version' parameter can only be used with sync_policy='remote'.
```

##### Solution: Switch to `sync_policy="remote"` when using the `version` parameter.

---

#### Missing Input Key

```
DatasetError: Dataset item at index 0 is missing required 'input' key.
```

##### Solution: Ensure every item in the list contains an `input` key:
```python
[{"id": "q1", "input": {"text": "cancel order"}, "expected_output": "cancel"}]
```

---

#### API Errors

```
DatasetError: Langfuse API error while fetching dataset '...': 401 Unauthorized
```

##### Solution:
- Verify `public_key` and `secret_key` are correct
- Check if keys have proper permissions
- Ensure `host` URL is correct for self-hosted instances

---

#### Issues
- **Bug Reports**: [kedro-plugins/issues](https://github.com/kedro-org/kedro-plugins/issues)

#### Related Resources
- **Kedro Academy**: [Agentic Workflows](https://github.com/kedro-org/kedro-academy/tree/main/kedro-agentic-workflows)
- **Langfuse Evaluation**: [Dataset experiments](https://langfuse.com/docs/evaluation/experiments/datasets)
- **Langfuse Prompts**: [Prompt management](https://langfuse.com/docs/prompt-management)
