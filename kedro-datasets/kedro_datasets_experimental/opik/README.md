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
