import json
import hashlib
from pathlib import Path
from typing import Any, Literal, Tuple

from kedro.io import AbstractDataset
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
    """
    Kedro dataset for managing LangChain prompts with Opik versioning.

    Behavior:
    - On save: writes prompt JSON to disk and creates/updates in Opik.
    - On load: syncs local and Opik versions, ensuring reproducibility.
    - Returns LangChain `ChatPromptTemplate` when loaded.
    """

    def __init__(
        self,
        filepath: str,
        prompt_name: str,
        prompt_type: Literal["chat", "text"],
        credentials: dict[str, Any] | None = None,
    ):
        """
        Args:
            filepath: Local JSON file path for storing prompt.
            prompt_name: Unique identifier for the prompt in Opik.
            prompt_type: Either "chat" or "text".
            credentials: Dict with Opik client configuration.
        """
        self._filepath = Path(filepath)
        self._prompt_name = prompt_name
        self._prompt_type = prompt_type
        self._opik_client = Opik(**(credentials or {}))

        # Ensure a dataset exists for prompt tracking
        try:
            self._dataset = self._opik_client.get_dataset(name=f"prompts-{prompt_name}")
        except Exception:
            self._dataset = self._opik_client.create_dataset(
                name=f"prompts-{prompt_name}",
                description=f"Prompt versions for {prompt_name}",
            )

    def _describe(self):
        return {"filepath": self._filepath, "prompt_name": self._prompt_name}

    def save(self, data: str | list) -> None:
        """
        Save prompt to local JSON and push to Opik.
        If prompt already exists in Opik, a new version is created.
        """
        self._filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(self._filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        self._opik_client.create_prompt(
            name=self._prompt_name,
            prompt=json.dumps(data, ensure_ascii=False),
            metadata={"type": self._prompt_type},
        )

    def _get_prompt_data(self) -> Tuple[Prompt | None, str | list | None]:
        """
        Fetch latest prompt from Opik.

        Returns:
            Tuple of (Prompt object or None, prompt data as str/list/None).
        """
        try:
            opik_prompt = self._opik_client.get_prompt(name=self._prompt_name)
            prompt_data = opik_prompt.prompt
            if isinstance(prompt_data, str):
                try:
                    prompt_data = json.loads(prompt_data)
                except json.JSONDecodeError:
                    pass
        except Exception:
            return None, None

        return opik_prompt, prompt_data

    def _sync_with_opik(
        self, local_data: str | list | None, opik_prompt: Prompt | None
    ) -> Tuple[Prompt | None, str | list | None]:
        """
        Ensure local file and Opik prompt are consistent.
        Returns latest Opik prompt and its parsed data.

        Cases handled:
        - Local exists but not in Opik → push local to Opik.
        - Both exist but differ → update Opik with local.
        - Only Opik exists → write to local.
        """
        if local_data is not None:
            if opik_prompt is None:
                # Push local to Opik
                self.save(local_data)
                return self._get_prompt_data()

            if _hash(_get_content(local_data)) != _hash(
                _get_content(opik_prompt.prompt)
            ):
                self.save(local_data)
                return self._get_prompt_data()

            return opik_prompt, local_data

        if opik_prompt:
            # Save Opik prompt locally
            self._filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(self._filepath, "w", encoding="utf-8") as f:
                json.dump(opik_prompt.prompt, f, indent=2)
            return opik_prompt, opik_prompt.prompt

        raise FileNotFoundError(
            f"No prompt found locally or in Opik for '{self._prompt_name}'"
        )

    def load(self) -> ChatPromptTemplate:
        """
        Load prompt with synchronization logic.
        Returns LangChain `ChatPromptTemplate`.
        """
        opik_prompt, prompt_data = self._get_prompt_data()

        local_data = None
        if self._filepath.exists():
            with open(self._filepath, "r", encoding="utf-8") as f:
                local_data = json.load(f)

        opik_prompt, prompt_data = self._sync_with_opik(local_data, opik_prompt)

        if isinstance(prompt_data, list):
            messages = [(m["role"], m["content"]) for m in prompt_data]
            return ChatPromptTemplate.from_messages(messages)
        if isinstance(prompt_data, str):
            return ChatPromptTemplate.from_template(prompt_data)

        raise ValueError(
            f"Unsupported prompt data format for '{self._prompt_name}': {type(prompt_data)}"
        )
