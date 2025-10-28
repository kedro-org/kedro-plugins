"""Opik prompt dataset for managing LangChain prompts."""

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

from kedro.io import AbstractDataset, DatasetError
from langchain.prompts import ChatPromptTemplate
from opik import Opik, Prompt


def _hash(data: str | list) -> str:
    """Return SHA-256 hash of a prompt (string or list of messages)."""
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def _get_content(data: str | list) -> str:
    """
    Extract comparable text content from a prompt.
    - If string: return as-is.
    - If list of messages: join their `content`.
    """
    if isinstance(data, str):
        return data
    return "\n".join(msg["content"] for msg in data)


class OpikPromptDataset(AbstractDataset):
    """Kedro dataset for managing LangChain prompts with Opik versioning.

    This dataset synchronizes prompts between local storage and Opik's prompt
    management system, enabling version control and reproducibility of prompts
    used in LLM pipelines.

    Behavior:
    - On save: writes prompt JSON to disk and creates/updates in Opik
    - On load: syncs local and Opik versions, ensuring reproducibility
    - Returns LangChain ChatPromptTemplate when loaded

    Examples:
        Using catalog YAML configuration:
    ```yaml
            customer_support_prompt:
              type: opik_prompt.OpikPromptDataset
              filepath: data/prompts/customer_support.json
              prompt_name: customer_support_v1
              prompt_type: chat
              credentials: opik_credentials
    ```

        Using Python API:
    ```python
        from kedro_datasets_experimental.opik_prompt import OpikPromptDataset

        # Create dataset for chat prompt
        dataset = OpikPromptDataset(
            filepath="data/prompts/customer_support.json",
            prompt_name="customer_support_v1",
            prompt_type="chat",
            credentials={"api_key": "opik_...", "workspace": "my-workspace"}  # pragma: allowlist secret
        )

        # Save a chat prompt
        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Hello, {question}"}
        ]
        dataset.save(messages)

        # Load prompt as LangChain ChatPromptTemplate
        prompt_template = dataset.load()
    ```
    """

    def __init__(
        self,
        filepath: str,
        prompt_name: str,
        prompt_type: Literal["chat", "text"],
        credentials: dict[str, Any] | None = None,
        **opik_kwargs: Any
    ):
        """Initialize OpikPromptDataset with local and remote configuration.

        Args:
            filepath: Local JSON file path for storing prompt.
            prompt_name: Unique identifier for the prompt in Opik.
            prompt_type: Either "chat" for message-based prompts or "text"
            for simple string templates.
            credentials: Dictionary with Opik client configuration. Keys may include
                'api_key', 'workspace', 'project_name', etc.
            **opik_kwargs: Additional kwargs passed to Opik client initialization.

        Raises:
            DatasetError: If required parameters are missing or invalid.

        Examples:
            Basic initialization:
```python
            dataset = OpikPromptDataset(
                filepath="prompts/my_prompt.json",
                prompt_name="my_prompt_v1",
                prompt_type="chat"
            )
```

            With Opik credentials:
```python
            dataset = OpikPromptDataset(
                filepath="prompts/my_prompt.json",
                prompt_name="my_prompt_v1",
                prompt_type="chat",
                credentials={"api_key": "opik_..."}  # pragma: allowlist secret
            )
```
        """
        if not filepath:
            raise DatasetError("filepath cannot be empty")
        if not prompt_name:
            raise DatasetError("prompt_name cannot be empty")
        if prompt_type not in ("chat", "text"):
            raise DatasetError(f"prompt_type must be 'chat' or 'text', got '{prompt_type}'")

        self._filepath = Path(filepath)
        self._prompt_name = prompt_name
        self._prompt_type = prompt_type

        # Initialize Opik client
        try:
            self._opik_client = Opik(**(credentials or {}), **opik_kwargs)
        except Exception as e:
            raise DatasetError(f"Failed to initialize Opik client: {e}")

        # Ensure a dataset exists for prompt tracking
        self._ensure_dataset_exists()

    def _ensure_dataset_exists(self) -> None:
        """Ensure Opik dataset exists for tracking prompt versions."""
        dataset_name = f"prompts-{self._prompt_name}"
        try:
            self._dataset = self._opik_client.get_dataset(name=dataset_name)
        except Exception:
            try:
                self._dataset = self._opik_client.create_dataset(
                    name=dataset_name,
                    description=f"Prompt versions for {self._prompt_name}",
                )
            except Exception as e:
                raise DatasetError(f"Failed to create Opik dataset '{dataset_name}': {e}")

    def _describe(self) -> dict[str, Any]:
        """Return a description of the dataset for Kedro's internal use.

        Returns:
            Dictionary containing dataset description.
        """
        return {
            "filepath": str(self._filepath),
            "prompt_name": self._prompt_name,
            "prompt_type": self._prompt_type
        }

    def save(self, data: str | list[dict[str, str]]) -> None:
        """Save prompt to local JSON and push to Opik."""
        # Validate data format
        if self._prompt_type == "chat" and not isinstance(data, list):
            raise DatasetError("Chat prompts must be a list of message dicts")
        if self._prompt_type == "text" and not isinstance(data, str):
            raise DatasetError("Text prompts must be a string")

        # Save locally
        try:
            self._filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(self._filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise DatasetError(f"Failed to save prompt locally to {self._filepath}: {e}")

        # Push to Opik
        try:
            self._opik_client.create_prompt(
                name=self._prompt_name,
                prompt=json.dumps(data, ensure_ascii=False),
                metadata={"type": self._prompt_type},
            )
        except Exception as e:
            raise DatasetError(f"Failed to save prompt to Opik: {e}")

    def _get_prompt_data(self) -> tuple[Prompt | None, str | list[dict[str, str]] | None]:
        """Fetch latest prompt from Opik.

        Returns:
            Tuple of (Prompt object or None, prompt data as str/list or None).
        """
        try:
            opik_prompt = self._opik_client.get_prompt(name=self._prompt_name)
            prompt_data = opik_prompt.prompt

            # Try to parse JSON if it's a string
            if isinstance(prompt_data, str):
                try:
                    prompt_data = json.loads(prompt_data)
                except json.JSONDecodeError:
                    # Keep as string if not valid JSON
                    pass

            return opik_prompt, prompt_data
        except Exception:
            return None, None

    def _sync_with_opik(
            self,
            local_data: str | list[dict[str, str]] | None,
            opik_prompt: Prompt | None
    ) -> tuple[Prompt | None, str | list[dict[str, str]] | None]:
        """Ensure local file and Opik prompt are consistent.

        Returns latest Opik prompt and its parsed data.

        Cases handled:
        - Local exists but not in Opik → push local to Opik
        - Both exist but differ → update Opik with local
        - Only Opik exists → write to local

        Args:
            local_data: Data from local file or None if file doesn't exist.
            opik_prompt: Prompt object from Opik or None if not found.

        Returns:
            Tuple of (updated Prompt object, prompt data).

        Raises:
            DatasetError: If no prompt found in either location.
        """
        if local_data is not None:
            if opik_prompt is None:
                # Push local to Opik
                self.save(local_data)
                return self._get_prompt_data()

            # Check if content differs
            opik_data = opik_prompt.prompt
            if isinstance(opik_data, str):
                try:
                    opik_data = json.loads(opik_data)
                except json.JSONDecodeError:
                    pass

            if _hash(_get_content(local_data)) != _hash(_get_content(opik_data)):
                self.save(local_data)
                return self._get_prompt_data()

            return opik_prompt, local_data

        if opik_prompt:
            # Save Opik prompt locally
            prompt_data = opik_prompt.prompt
            if isinstance(prompt_data, str):
                try:
                    prompt_data = json.loads(prompt_data)
                except json.JSONDecodeError:
                    pass

            try:
                self._filepath.parent.mkdir(parents=True, exist_ok=True)
                with open(self._filepath, "w", encoding="utf-8") as f:
                    json.dump(prompt_data, f, indent=2, ensure_ascii=False)
            except Exception as e:
                raise DatasetError(f"Failed to sync Opik prompt to local file: {e}")

            return opik_prompt, prompt_data

        raise DatasetError(
            f"No prompt found locally at '{self._filepath}' or in Opik for '{self._prompt_name}'"
        )

    def load(self) -> ChatPromptTemplate:
        """Load prompt with synchronization logic.

        Synchronizes local and Opik versions before returning the prompt.

        Returns:
            LangChain ChatPromptTemplate constructed from the prompt data.

        Raises:
            DatasetError: If prompt cannot be loaded or has invalid format.

        Examples:
            Load and use a prompt:
```python
            template = dataset.load()
            result = template.format(question="How are you?")
```
        """
        opik_prompt, prompt_data = self._get_prompt_data()

        # Load local data if file exists
        local_data = None
        if self._filepath.exists():
            try:
                with open(self._filepath, encoding="utf-8") as f:
                    local_data = json.load(f)
            except Exception as e:
                raise DatasetError(f"Failed to read local prompt file: {e}")

        # Sync and get final data
        opik_prompt, prompt_data = self._sync_with_opik(local_data, opik_prompt)

        # Convert to ChatPromptTemplate
        if isinstance(prompt_data, list):
            try:
                messages = [(m["role"], m["content"]) for m in prompt_data]
                return ChatPromptTemplate.from_messages(messages)
            except (KeyError, TypeError) as e:
                raise DatasetError(f"Invalid chat prompt format: {e}")

        if isinstance(prompt_data, str):
            return ChatPromptTemplate.from_template(prompt_data)

        raise DatasetError(
            f"Unsupported prompt data format for '{self._prompt_name}': {type(prompt_data)}"
        )
