import json
import hashlib
import logging
from pathlib import Path
from typing import Any, Literal, Union, TYPE_CHECKING

from kedro.io import AbstractDataset

if TYPE_CHECKING:
    from kedro_datasets.json import JSONDataset
    from kedro_datasets.yaml import YAMLDataset

from langchain.prompts import ChatPromptTemplate
from langfuse import Langfuse

# Type mapping for normalizing Langfuse message types to local storage conventions
_MESSAGE_TYPE_MAP = {
    "message": "chatmessage"
}

# Supported file extensions for prompt storage
SUPPORTED_FILE_EXTENSIONS = {".json", ".yaml", ".yml"}

# Required credentials for Langfuse authentication
REQUIRED_LANGFUSE_CREDENTIALS = {"public_key", "secret_key"}
OPTIONAL_LANGFUSE_CREDENTIALS = {"host"}

# Logger for this module
logger = logging.getLogger(__name__)


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


class LangfusePromptDataset(AbstractDataset):
    """Kedro dataset for managing prompts with Langfuse versioning and synchronization.

    This dataset provides seamless integration between local prompt files (JSON/YAML)
    and Langfuse prompt management, supporting version control, labeling, and
    different synchronization policies.

    On save: writes prompt to local file and creates new version in Langfuse.
    On load: synchronizes based on sync_policy and returns LangChain ChatPromptTemplate
    (langchain mode) or raw Langfuse object (sdk mode).

    Sync policies:
    - local: local file takes precedence (default)
    - remote: Langfuse version takes precedence
    - strict: error if local and remote differ

    Examples:
        Using catalog YAML configuration:

        ```yaml
        intent_prompt:
          type: kedro_agentic_workflows.datasets.langfuse_prompt_dataset.LangfusePromptDataset
          filepath: data/prompts/intent.json
          prompt_name: "intent-classifier"
          prompt_type: "chat"
          credentials: langfuse_credentials
          sync_policy: local
          mode: langchain
          load_args:
            label: "production"
          save_args:
            labels: ["staging", "v2.1"]
        ```

        Using Python API:

        ```python
        from kedro_agentic_workflows.datasets.langfuse_prompt_dataset import (
            LangfusePromptDataset,
        )

        # Basic usage (using default Langfuse cloud)
        dataset = LangfusePromptDataset(
            filepath="data/prompts/intent.json",
            prompt_name="intent-classifier",
            credentials={
                "public_key": "pk_...",
                "secret_key": "sk_...",  # pragma: allowlist secret
            },
        )

        # With custom host
        dataset = LangfusePromptDataset(
            filepath="data/prompts/intent.json",
            prompt_name="intent-classifier",
            credentials={
                "public_key": "pk_...",
                "secret_key": "sk_...",  # pragma: allowlist secret
                "host": "https://custom.langfuse.com",
            },
        )

        # Load and use prompt
        prompt_template = dataset.load()
        formatted = prompt_template.format(user_input="Hello world")

        # Save new version with labels
        chat_prompt = [
            {"type": "chatmessage", "role": "system", "content": "You are helpful."},
            {"type": "chatmessage", "role": "human", "content": "{input}"},
        ]
        dataset.save(chat_prompt)
        ```
    """
    def __init__(
        self,
        filepath: str,
        prompt_name: str,
        credentials: dict[str, Any],
        prompt_type: Literal["chat", "text"] = "text",
        sync_policy: Literal["local", "remote", "strict"] = "local",
        mode: Literal["langchain", "sdk"] = "langchain",
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize LangfusePromptDataset for managing prompts with Langfuse versioning.

        This dataset provides seamless integration between local prompt files (JSON/YAML)
        and Langfuse prompt management, supporting version control, labeling, and
        different synchronization policies.

        Args:
            filepath: Local file path for storing prompt. Supports .json, .yaml, .yml extensions.
            prompt_name: Unique identifier for the prompt in Langfuse.
            prompt_type: Type of prompt - "chat" for conversation or "text" for single prompts.
            credentials: Dict with Langfuse credentials. Required: {public_key, secret_key}.
                Optional: {host} (defaults to Langfuse cloud if not provided).
            sync_policy: How to handle conflicts between local and remote:
                - "local": Local file takes precedence (default)
                - "remote": Langfuse version takes precedence
                - "strict": Error if local and remote differ
            mode: Return type for load() method:
                - "langchain": Returns ChatPromptTemplate object (default)
                - "sdk": Returns raw Langfuse prompt object
            load_args: Dict with loading parameters. Supported keys:
                - version (int): Specific version number to load
                - label (str): Specific label to load (e.g., "production", "staging")
                Note: Cannot specify both version and label simultaneously.
            save_args: Dict with saving parameters. Supported keys:
                - labels (list[str]): List of labels to assign to new prompt versions

        Examples:
            >>> # Basic usage with default settings (using Langfuse cloud)
            >>> dataset = LangfusePromptDataset(
            ...     filepath="prompts/intent.json",
            ...     prompt_name="intent-classifier",
            ...     credentials={"public_key": "pk_...", "secret_key": "sk_..."}  # pragma: allowlist secret
            ... )

            >>> # With custom host
            >>> dataset = LangfusePromptDataset(
            ...     filepath="prompts/intent.json",
            ...     prompt_name="intent-classifier",
            ...     credentials={"public_key": "pk_...", "secret_key": "sk_...", "host": "https://custom.langfuse.com"}  # pragma: allowlist secret
            ... )

            >>> # Load specific version with remote-first policy
            >>> dataset = LangfusePromptDataset(
            ...     filepath="prompts/intent.yaml",
            ...     prompt_name="intent-classifier",
            ...     credentials=creds,
            ...     sync_policy="remote",
            ...     load_args={"version": 3}
            ... )

            >>> # Auto-label new versions when saving
            >>> dataset = LangfusePromptDataset(
            ...     filepath="prompts/intent.json",
            ...     prompt_name="intent-classifier",
            ...     credentials=creds,
            ...     save_args={"labels": ["staging", "v2.1"]}
            ... )

        Raises:
            ValueError: If credentials are missing required keys.
            NotImplementedError: If filepath has unsupported extension.
        """
        # Validate all parameters before assignment
        self._validate_init_params(filepath, credentials)

        self._filepath = Path(filepath)
        self._prompt_name = prompt_name
        self._prompt_type: Literal["chat", "text"] = prompt_type
        self._langfuse = Langfuse(
            public_key=credentials["public_key"],
            secret_key=credentials["secret_key"],
            host=credentials["host"],
        )
        self._sync_policy = sync_policy
        self._mode = mode
        self._load_args = load_args or {}
        self._save_args = save_args or {}
        self._file_dataset = None

    def _validate_init_params(
        self,
        filepath: str,
        credentials: dict[str, Any]
    ) -> None:
        """Validate initialization parameters.

        Validates that required credentials (public_key, secret_key) are present
        and not empty. Optional credentials (host) are validated only if provided.

        Args:
            filepath: File path to validate for supported extensions.
            credentials: Credentials dictionary to validate for required keys.

        Raises:
            ValueError: If required credentials are missing or empty, or if
                optional credentials are provided but empty.
            NotImplementedError: If filepath has unsupported extension.
        """
        # Validate required keys
        for key in REQUIRED_LANGFUSE_CREDENTIALS:
            if key not in credentials:
                raise ValueError(f"Missing required Langfuse credential: '{key}'")

            # Validate that credential is not empty
            if not credentials[key] or not str(credentials[key]).strip():
                raise ValueError(f"Langfuse credential '{key}' cannot be empty")

        # Validate optional keys if present
        for key in OPTIONAL_LANGFUSE_CREDENTIALS:
            if key in credentials:
                # If host is provided, it cannot be empty
                if not credentials[key] or not str(credentials[key]).strip():
                    raise ValueError(f"Langfuse credential '{key}' cannot be empty if provided")

        # Validate file extension
        file_path = Path(filepath)
        if file_path.suffix.lower() not in SUPPORTED_FILE_EXTENSIONS:
            raise NotImplementedError(
                f"Unsupported file extension '{file_path.suffix}'. "
                f"Supported formats: {', '.join(sorted(SUPPORTED_FILE_EXTENSIONS))}"
            )

    def _describe(self) -> dict[str, Any]:
        """Return a description of the dataset for Kedro's internal use.

        Returns:
            Dictionary containing dataset description with filepath and prompt_name.
        """
        return {"filepath": self._filepath, "prompt_name": self._prompt_name}

    @property
    def file_dataset(self) -> Union["JSONDataset", "YAMLDataset"]:
        """Get appropriate Kedro dataset based on file extension (cached).

        Returns:
            JSONDataset for .json files, YAMLDataset for .yaml/.yml files.

        Raises:
            NotImplementedError: If file extension is not supported.
        """
        if self._file_dataset is None:
            if self._filepath.suffix.lower() in [".yaml", ".yml"]:
                from kedro_datasets.yaml import YAMLDataset
                self._file_dataset = YAMLDataset(filepath=str(self._filepath))
            elif self._filepath.suffix.lower() == ".json":
                from kedro_datasets.json import JSONDataset
                self._file_dataset = JSONDataset(filepath=str(self._filepath))
            else:
                raise NotImplementedError(
                    f"Unsupported file extension '{self._filepath.suffix}'. "
                    f"Supported formats: {', '.join(sorted(SUPPORTED_FILE_EXTENSIONS))}"
                )
        return self._file_dataset

    def save(self, data: Union[str, list]) -> None:
        """Save prompt data to local file and create new version in Langfuse.

        Args:
            data: The prompt content to save. Can be string for text prompts
                or list of message dictionaries for chat prompts.

        Raises:
            ValueError: If Langfuse API call fails or invalid data format.
        """
        self.file_dataset.save(data)

        create_kwargs = {
            "name": self._prompt_name,
            "prompt": data,
            "type": self._prompt_type,
        }

        # Add labels from save_args if specified
        if "labels" in self._save_args:
            create_kwargs["labels"] = self._save_args["labels"]

        self._langfuse.create_prompt(**create_kwargs)

    def _build_get_kwargs(self) -> dict[str, Any]:
        """Build kwargs for fetching prompt from Langfuse based on load_args.

        Returns:
            Kwargs dictionary for langfuse.get_prompt() with name, type, and
            optional version or label parameters.
        """
        get_kwargs = {"name": self._prompt_name, "type": self._prompt_type}
        if self._load_args.get("label") is not None:
            get_kwargs["label"] = self._load_args["label"]
        elif self._load_args.get("version") is not None:
            get_kwargs["version"] = self._load_args["version"]
        else:
            get_kwargs["label"] = "latest"
        return get_kwargs

    def _sync_strict_policy(
        self, local_data: Union[str, list, None], langfuse_prompt: Any | None
    ) -> Any:
        """Handle strict sync policy - error if local and remote differ.

        Args:
            local_data: Content from local file, None if file doesn't exist.
            langfuse_prompt: Langfuse prompt object, None if not found remotely.

        Returns:
            Langfuse prompt object if sync is successful.

        Raises:
            ValueError: If either local_data or langfuse_prompt is missing, or if they differ.
        """
        if not local_data or not langfuse_prompt:
            missing_parts = []
            if not local_data:
                missing_parts.append("local file")
            if not langfuse_prompt:
                missing_parts.append("remote prompt")

            raise ValueError(
                f"Strict sync policy specified for '{self._prompt_name}' . "
                f"Both local and remote prompts must exist in strict mode."
                f"Missing: {' and '.join(missing_parts)}."
            )

        local_hash = _hash(_get_content(local_data))
        remote_hash = _hash(_get_content(langfuse_prompt.prompt))
        if local_hash != remote_hash:
            raise ValueError(
                f"Strict sync failed for '{self._prompt_name}': "
                f"local and remote prompts differ. Use 'local' or 'remote' policy to resolve."
            )
        return langfuse_prompt

    def _adapt_langfuse_chat_format(self, prompt_data: Union[str, list]) -> Union[str, list]:
        """Adapt Langfuse chat message format to local storage conventions.

        TODO: This exists because Langfuse returns prompts with 'message' type,
        but our local storage standardizes on 'chatmessage' for consistency.
        This discrepancy requires format adaptation during synchronization.

        Args:
            prompt_data: The prompt data from Langfuse (string or list of messages).

        Returns:
            New prompt data with message types converted according to _MESSAGE_TYPE_MAP
            for chat prompts, unchanged for other prompt types.
        """
        if self._prompt_type == "chat" and isinstance(prompt_data, list):
            # Return new list instead of mutating input
            adapted_messages = []
            for msg in prompt_data:
                if isinstance(msg, dict):
                    # Create new dict with adapted type if needed
                    adapted_msg = msg.copy()
                    msg_type = adapted_msg.get("type")
                    if msg_type in _MESSAGE_TYPE_MAP:
                        adapted_msg["type"] = _MESSAGE_TYPE_MAP[msg_type]
                    adapted_messages.append(adapted_msg)
                else:
                    adapted_messages.append(msg)
            return adapted_messages
        return prompt_data

    def _sync_remote_policy(
        self, local_data: str | None, langfuse_prompt: Any | None
    ) -> Any:
        """
        Handle remote sync policy - Langfuse version takes precedence.

        Args:
            local_data: Content from local file, None if file doesn't exist
            langfuse_prompt: Langfuse prompt object, None if not found remotely

        Returns:
            Any: Langfuse prompt object after updating local file if needed

        Raises:
            ValueError: If remote prompt doesn't exist
        """
        if not langfuse_prompt:
            raise ValueError(
                f"Remote sync policy specified for '{self._prompt_name}' "
                f"but no remote prompt exists in Langfuse. Create the prompt in Langfuse first."
            )
        if not local_data or _hash(_get_content(local_data)) != _hash(_get_content(langfuse_prompt.prompt)):
            normalized_prompt = self._adapt_langfuse_chat_format(langfuse_prompt.prompt)
            logger.info(f"Overwriting local file '{self._filepath}' with remote prompt '{self._prompt_name}' from Langfuse (remote sync policy)")
            self.file_dataset.save(normalized_prompt)
        return langfuse_prompt

    def _sync_local_policy(
        self, local_data: str | None, langfuse_prompt: Any | None
    ) -> Any:
        """
        Handle local sync policy - local file takes precedence.

        Args:
            local_data: Content from local file, None if file doesn't exist
            langfuse_prompt: Langfuse prompt object, None if not found remotely

        Returns:
            Any: Langfuse prompt object after syncing

        Raises:
            FileNotFoundError: If neither local nor remote prompt exists
        """
        if local_data is not None:
            if langfuse_prompt is None:
                # Push local to Langfuse
                self.save(local_data)
                return self._langfuse.get_prompt(**self._build_get_kwargs())

            # If mismatch → update Langfuse with local
            if _hash(_get_content(local_data)) != _hash(
                _get_content(langfuse_prompt.prompt)
            ):
                self.save(local_data)
                return self._langfuse.get_prompt(**self._build_get_kwargs())
            return langfuse_prompt

        # If local missing but Langfuse exists → persist locally
        if langfuse_prompt:
            normalized_prompt = self._adapt_langfuse_chat_format(langfuse_prompt.prompt)
            logger.info(f"Creating local file '{self._filepath}' from remote prompt '{self._prompt_name}' from Langfuse (local sync policy) as local file is missing")
            self.file_dataset.save(normalized_prompt)
            return langfuse_prompt

        raise FileNotFoundError(
            f"No prompt found locally or in Langfuse for '{self._prompt_name}'"
        )

    def _sync_with_langfuse(
        self, local_data: str | None, langfuse_prompt: Any | None
    ) -> Any:
        """
        Synchronize local file and Langfuse prompt based on configured sync policy.

        This method delegates to specialized sync policy handlers based on the
        configured sync_policy setting.

        Args:
            local_data: Content from local file, None if file doesn't exist
            langfuse_prompt: Langfuse prompt object, None if not found remotely

        Returns:
            Any: Langfuse prompt object after synchronization

        Raises:
            ValueError: Based on sync_policy conflicts (see individual policy methods)
            FileNotFoundError: If no prompt found locally or in Langfuse
        """
        if self._sync_policy == "strict":
            return self._sync_strict_policy(local_data, langfuse_prompt)
        elif self._sync_policy == "remote":
            return self._sync_remote_policy(local_data, langfuse_prompt)
        else:  # local policy (default)
            return self._sync_local_policy(local_data, langfuse_prompt)

    def load(self) -> ChatPromptTemplate | Any:
        """
        Load prompt from Langfuse with local file synchronization.

        This method performs the complete load workflow:
        1. Attempts to fetch prompt from Langfuse using configured version/label (if specified)
        2. Handles network/API errors gracefully with fallback to local synchronization
        3. Loads local file if it exists
        4. Synchronizes local and remote versions based on sync_policy
        5. Returns prompt in the format specified by mode parameter

        The method respects load_args for version/label specification and handles
        various sync scenarios automatically, including first-time loading scenarios.

        Returns:
            ChatPromptTemplate: If mode="langchain" (default)
                Ready-to-use LangChain prompt template with variable substitution
            Any: If mode="sdk"
                Raw Langfuse prompt object with full API access

        Raises:
            ValueError: Based on sync_policy conflicts (see _sync_with_langfuse)
            FileNotFoundError: If no prompt found locally or in Langfuse
            NotImplementedError: If file extension is not supported

        Note:
            Network errors (ConnectionError, TimeoutError) are handled gracefully
            with warning logs and fallback to local file synchronization.

        Examples:
            >>> # Load with default settings (latest version, langchain mode)
            >>> prompt_template = dataset.load()
            >>> formatted = prompt_template.format(user_input="Hello world")

            >>> # Load specific version in SDK mode
            >>> dataset_v3 = LangfusePromptDataset(
            ...     filepath="prompts/intent.json",
            ...     prompt_name="intent-classifier",
            ...     credentials=creds,
            ...     mode="sdk",
            ...     load_args={"version": 3}
            ... )
            >>> langfuse_prompt = dataset_v3.load()
            >>> print(f"Version: {langfuse_prompt.version}")
            >>> print(f"Labels: {langfuse_prompt.labels}")

            >>> # Load from staging environment
            >>> staging_dataset = LangfusePromptDataset(
            ...     filepath="prompts/staging.yaml",
            ...     prompt_name="intent-classifier",
            ...     credentials=creds,
            ...     sync_policy="remote",
            ...     load_args={"label": "staging"}
            ... )
            >>> prompt = staging_dataset.load()

            >>> # Handle different prompt types
            >>> if dataset._prompt_type == "chat":
            ...     # Chat prompts return templates with message formatting
            ...     messages = prompt.format_messages(user_input="test")
            ... else:
            ...     # Text prompts return simple string templates
            ...     text = prompt.format(user_input="test")
        """
        try:
            langfuse_prompt = self._langfuse.get_prompt(**self._build_get_kwargs())
        except ConnectionError as e:
            logger.warning(
                f"Network connection error when fetching prompt '{self._prompt_name}': {e}. "
                f"Falling back to local file sync."
            )
            langfuse_prompt = None
        except TimeoutError as e:
            logger.warning(
                f"Timeout error when fetching prompt '{self._prompt_name}': {e}. "
                f"Falling back to local file sync."
            )
            langfuse_prompt = None
        except Exception as e:
            logger.warning(
                f"Unexpected error when fetching prompt '{self._prompt_name}': {type(e).__name__}: {e}. "
                f"Falling back to local file sync."
            )
            langfuse_prompt = None

        # Load local file if it exists
        local_data = None
        if self._filepath.exists():
            local_data = self.file_dataset.load()

        # Synchronize local and remote
        langfuse_prompt = self._sync_with_langfuse(local_data, langfuse_prompt)

        _RETURN_MODES = {
            "sdk": lambda prompt: prompt,
            "langchain": lambda prompt: ChatPromptTemplate.from_messages(prompt.get_langchain_prompt()),
        }
        return _RETURN_MODES[self._mode](langfuse_prompt)
