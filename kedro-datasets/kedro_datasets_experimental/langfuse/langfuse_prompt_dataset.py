import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Union

from kedro.io import AbstractDataset, DatasetError

if TYPE_CHECKING:
    from langchain.prompts import ChatPromptTemplate

    from kedro_datasets.json import JSONDataset
    from kedro_datasets.yaml import YAMLDataset

from langfuse import Langfuse

from kedro_datasets._typing import JSONPreview

# Supported file extensions for prompt storage
SUPPORTED_FILE_EXTENSIONS = {".json", ".yaml", ".yml"}

# Required credentials for Langfuse authentication
REQUIRED_LANGFUSE_CREDENTIALS = {"public_key", "secret_key"}
OPTIONAL_LANGFUSE_CREDENTIALS = {"host"}

# Valid parameter values
VALID_PROMPT_TYPES = {"chat", "text"}
VALID_SYNC_POLICIES = {"local", "remote", "strict"}
VALID_MODES = {"langchain", "sdk"}

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
    and Langfuse prompt management, supporting version control, labelling, and
    different synchronization policies.

    On save: Creates a new version of prompt in Langfuse with the local data.
    On load: synchronizes based on sync_policy and returns raw Langfuse object
    (sdk mode) or LangChain ChatPromptTemplate (langchain mode)

    Sync policies:
    - local: local file takes precedence (default). load_args (version/label) are
      ignored with warning and latest prompt from langfuse is loaded if available,
      since local files are the source of truth.
    - remote: Langfuse version takes precedence. load_args are respected.
    - strict: error if local and remote differ. load_args are respected.

    Examples:
        Using catalog YAML configuration:

        ```yaml
        # Local sync policy - local files are source of truth
        intent_prompt:
          type: langfuse.LangfusePromptDataset
          filepath: data/prompts/intent.json
          prompt_name: "intent-classifier"
          prompt_type: "chat"
          credentials: langfuse_credentials
          sync_policy: local
          mode: langchain
          # load_args are ignored in local mode with warning
          # and latest prompt from langfuse is loaded if available
          save_args:
            labels: ["staging", "v2.1"]

        # Remote sync policy - Langfuse versions are source of truth
        production_prompt:
          type: langfuse.LangfusePromptDataset
          filepath: data/prompts/production.json
          prompt_name: "intent-classifier"
          sync_policy: remote
          load_args:
            label: "production"  # This is respected in remote mode
        ```

        Using Python API:

        ```python
        from kedro_datasets_experimental.langfuse import LangfusePromptDataset

        # Basic usage (using default Langfuse cloud)
        dataset = LangfusePromptDataset(
            filepath="data/prompts/intent.json",
            prompt_name="intent-classifier",
            prompt_type="chat",
            credentials={
                "public_key": "pk_...",
                "secret_key": "sk_...",  # pragma: allowlist secret
            },
        )

        # With custom host
        dataset = LangfusePromptDataset(
            filepath="data/prompts/intent.json",
            prompt_name="intent-classifier",
            prompt_type="chat",
            mode="langchain",
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
    def __init__(   # noqa: PLR0913
        self,
        filepath: str,
        prompt_name: str,
        credentials: dict[str, Any],
        prompt_type: Literal["chat", "text"] = "text",
        sync_policy: Literal["local", "remote", "strict"] = "local",
        mode: Literal["langchain", "sdk"] = "sdk",
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize LangfusePromptDataset for managing prompts with Langfuse versioning.

        Args:
            filepath: Local file path for storing prompt. Supports .json, .yaml, .yml extensions.
            prompt_name: Unique identifier for the prompt in Langfuse.
            prompt_type: Type of prompt - "chat" for conversation or "text" for single prompts.
            credentials: Dictionary with Langfuse credentials. Required: {public_key, secret_key}.
                Optional: {host} (defaults to Langfuse cloud if not provided).
            sync_policy: How to handle conflicts between local and remote:
                - "local": Local file takes precedence (default)
                - "remote": Langfuse version takes precedence
                - "strict": Error if local and remote differ
            mode: Return type for load() method:
                - "sdk": Returns raw Langfuse prompt object (default)
                - "langchain": Returns ChatPromptTemplate object
            load_args: Dictionary with loading parameters. Only used when sync_policy="remote" or "strict".
                Ignored with warning when sync_policy="local". Supported keys:
                - version (int): Specific version number to load
                - label (str): Specific label to load (e.g., "production", "staging")
                Note: Langfuse will throw an error if both version and label are used together.
                So label is preferred over version if provided.
            save_args: Dictionary with saving parameters. Supported keys:
                - labels (list[str]): List of labels to assign to new prompt versions

        Examples:
            >>> # Local sync policy (default) - local files are source of truth
            >>> dataset = LangfusePromptDataset(
            ...     filepath="prompts/intent.json",
            ...     prompt_name="intent-classifier",
            ...     credentials={"public_key": "pk_...", "secret_key": "sk_..."}  # pragma: allowlist secret
            ... )

            >>> # Remote sync policy - load specific version from Langfuse
            >>> dataset = LangfusePromptDataset(
            ...     filepath="prompts/intent.yaml",
            ...     prompt_name="intent-classifier",
            ...     credentials=creds,
            ...     sync_policy="remote",
            ...     load_args={"version": 3}  # This is respected in remote mode
            ... )

            >>> # Remote sync policy - load specific label from Langfuse
            >>> dataset = LangfusePromptDataset(
            ...     filepath="prompts/production.json",
            ...     prompt_name="intent-classifier",
            ...     credentials=creds,
            ...     sync_policy="remote",
            ...     load_args={"label": "production"}  # This is respected in remote mode
            ... )

            >>> # With custom host
            >>> dataset = LangfusePromptDataset(
            ...     filepath="prompts/intent.json",
            ...     prompt_name="intent-classifier",
            ...     credentials={"public_key": "pk_...", "secret_key": "sk_...", "host": "https://custom.langfuse.com"}  # pragma: allowlist secret
            ... )

            >>> # Auto-label new versions when saving (works with any sync policy)
            >>> dataset = LangfusePromptDataset(
            ...     filepath="prompts/intent.json",
            ...     prompt_name="intent-classifier",
            ...     credentials=creds,
            ...     save_args={"labels": ["staging", "v2.1"]}
            ... )

        Raises:
            DatasetError: If credentials are missing required keys.
            NotImplementedError: If filepath has unsupported extension.
        """
        # Validate all parameters before assignment
        self._validate_init_params(filepath, credentials, prompt_type, sync_policy, mode, load_args, save_args)

        self._filepath = Path(filepath)
        self._prompt_name = prompt_name
        self._prompt_type: Literal["chat", "text"] = prompt_type or "text"
        self._langfuse = Langfuse(
            public_key=credentials["public_key"],
            secret_key=credentials["secret_key"],
            host=credentials.get("host"),
        )
        self._sync_policy = sync_policy or "local"
        self._mode = mode or "sdk"
        self._load_args = load_args or {}
        self._save_args = save_args or {}
        self._file_dataset = None
        self._cached_build_args = None

    def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        """Validate Langfuse credentials.

        Args:
            credentials: Credentials dictionary to validate.

        Raises:
            DatasetError: If required credentials are missing or empty.
        """
        # Validate required keys
        for key in REQUIRED_LANGFUSE_CREDENTIALS:
            if key not in credentials:
                raise DatasetError(f"Missing required Langfuse credential: '{key}'")
            if not credentials[key] or not str(credentials[key]).strip():
                raise DatasetError(f"Langfuse credential '{key}' cannot be empty")

        # Validate optional keys
        for key in OPTIONAL_LANGFUSE_CREDENTIALS:
            if key in credentials:
                if not credentials[key] or not str(credentials[key]).strip():
                    raise DatasetError(f"Langfuse credential '{key}' cannot be empty if provided")

    def _validate_enum_params(self, prompt_type: str, sync_policy: str, mode: str) -> None:
        """Validate enum parameters.

        Args:
            prompt_type: Prompt type to validate.
            sync_policy: Sync policy to validate.
            mode: Mode to validate.

        Raises:
            DatasetError: If parameter values are invalid.
        """
        if prompt_type and prompt_type not in VALID_PROMPT_TYPES:
            raise DatasetError(
                f"Invalid prompt_type '{prompt_type}'. Must be one of: {', '.join(sorted(VALID_PROMPT_TYPES))}"
            )

        if sync_policy and sync_policy not in VALID_SYNC_POLICIES:
            raise DatasetError(
                f"Invalid sync_policy '{sync_policy}'. Must be one of: {', '.join(sorted(VALID_SYNC_POLICIES))}"
            )

        if mode and mode not in VALID_MODES:
            raise DatasetError(
                f"Invalid mode '{mode}'. Must be one of: {', '.join(sorted(VALID_MODES))}"
            )

    def _validate_args(self, load_args: dict[str, Any] | None, save_args: dict[str, Any] | None) -> None:
        """Validate load_args and save_args.

        Args:
            load_args: Load arguments to validate.
            save_args: Save arguments to validate.

        Raises:
            DatasetError: If argument types are invalid.
        """
        if load_args is not None:
            if "version" in load_args and load_args["version"] is not None:
                if not isinstance(load_args["version"], int):
                    raise DatasetError(
                        f"load_args['version'] must be an integer, got {type(load_args['version']).__name__}: {load_args['version']}"
                    )
            if "label" in load_args and load_args["label"] is not None:
                if not isinstance(load_args["label"], str):
                    raise DatasetError(
                        f"load_args['label'] must be a string, got {type(load_args['label']).__name__}: {load_args['label']}"
                    )

        if save_args is not None:
            if "labels" in save_args and save_args["labels"] is not None:
                if not isinstance(save_args["labels"], list):
                    raise DatasetError(
                        f"save_args['labels'] must be a list, got {type(save_args['labels']).__name__}: {save_args['labels']}"
                    )
                for i, label in enumerate(save_args["labels"]):
                    if not isinstance(label, str):
                        raise DatasetError(
                            f"save_args['labels'][{i}] must be a string, got {type(label).__name__}: {label}"
                        )

    def _validate_init_params(  # noqa: PLR0913
        self,
        filepath: str,
        credentials: dict[str, Any],
        prompt_type: str,
        sync_policy: str,
        mode: str,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
    ) -> None:
        """Validate initialization parameters.

        Args:
            filepath: File path to validate for supported extensions.
            credentials: Credentials dictionary to validate.
            prompt_type: Prompt type to validate.
            sync_policy: Sync policy to validate.
            mode: Mode to validate.
            load_args: Load arguments to validate.
            save_args: Save arguments to validate.

        Raises:
            DatasetError: If parameters are invalid.
            NotImplementedError: If filepath has unsupported extension.
            ImportError: If langchain package is required but not available.
        """
        # Validate file extension
        file_path = Path(filepath)
        if file_path.suffix.lower() not in SUPPORTED_FILE_EXTENSIONS:
            raise NotImplementedError(
                f"Unsupported file extension '{file_path.suffix}'. "
                f"Supported formats: {', '.join(sorted(SUPPORTED_FILE_EXTENSIONS))}"
            )

        # Validate mode-specific requirements
        if mode == "langchain":
            try:
                from langchain.prompts import ChatPromptTemplate  # noqa: PLC0415
            except ImportError as exc:
                raise ImportError(
                    "The 'langchain' package is required when using mode='langchain'. "
                    "Install it with: pip install 'kedro-datasets[langfuse]'"
                ) from exc

        # Delegate to specialized validation methods
        self._validate_credentials(credentials)
        self._validate_enum_params(prompt_type, sync_policy, mode)
        self._validate_args(load_args, save_args)

    def _describe(self) -> dict[str, Any]:
        """Return a description of the dataset for Kedro's internal use.

        Returns:
            Dictionary containing dataset description with filepath and Langfuse prompt details.
        """
        return {
            "filepath": self._filepath,
            "prompt_name": self._prompt_name,
            "langfuse_prompt_args": self._get_build_args
        }

    def _get_prompt_description(self) -> str:
        """Get consistent prompt description for error messages."""
        return f"'{self._prompt_name}' with args {self._get_build_args}"

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
                from kedro_datasets.yaml import YAMLDataset  # noqa: PLC0415
                self._file_dataset = YAMLDataset(filepath=str(self._filepath))
            elif self._filepath.suffix.lower() == ".json":
                from kedro_datasets.json import JSONDataset  # noqa: PLC0415
                self._file_dataset = JSONDataset(filepath=str(self._filepath))
            else:
                raise NotImplementedError(
                    f"Unsupported file extension '{self._filepath.suffix}'. "
                    f"Supported formats: {', '.join(sorted(SUPPORTED_FILE_EXTENSIONS))}"
                )
        return self._file_dataset

    def save(self, data: str | list) -> None:
        """Create a new version of prompt in Langfuse with the local data.

        Args:
            data: The prompt content to save. Can be string for text prompts
                or list of message dictionaries for chat prompts.

        Raises:
            DatasetError: If Langfuse API call fails or invalid data format.
        """
        create_kwargs = {
            "name": self._prompt_name,
            "prompt": data,
            "type": self._prompt_type,
        }

        # Add labels from save_args if specified
        if "labels" in self._save_args:
            create_kwargs["labels"] = self._save_args["labels"]

        self._langfuse.create_prompt(**create_kwargs)

    @property
    def _get_build_args(self) -> dict[str, Any]:
        """Build kwargs for fetching prompt from Langfuse based on load_args and sync_policy.

        This is a cached property that computes the arguments once and reuses them for
        performance optimization, as these parameters are accessed frequently during
        load operations and error message generation.

        When sync_policy="local", load_args (version/label) are ignored since local files
        are the source of truth. Users get a warning and the latest version is fetched
        for synchronization purposes only.

        When sync_policy="remote" or "strict", load_args are respected since remote
        versions matter for these policies.

        Returns:
            Cached kwargs dictionary for langfuse.get_prompt() with name, type, and
            optional version or label parameters.
        """
        if self._cached_build_args is not None:
            return self._cached_build_args

        get_kwargs = {"name": self._prompt_name, "type": self._prompt_type}

        # Check if user specified version/label with local sync policy
        has_load_args = (self._load_args.get("label") is not None or
                           self._load_args.get("version") is not None)

        if self._sync_policy == "local" and has_load_args:
            # Warn user that load_args are ignored in local mode
            specified_args = []
            if self._load_args.get("label") is not None:
                specified_args.append(f"label='{self._load_args['label']}'")
            if self._load_args.get("version") is not None:
                specified_args.append(f"version={self._load_args['version']}")

            logger.warning(
                f"Ignoring load_args ({', '.join(specified_args)}) for prompt '{self._prompt_name}' "
                f"because sync_policy='local'. Local files are the source of truth. "
                f"'latest' label is used to fetch remote prompt for synchronization. "
                f"To use specific versions/labels, switch to sync_policy='remote'."
            )
            # Always fetch latest for local sync policy
            get_kwargs["label"] = "latest"
        elif self._load_args.get("label") is not None:
            get_kwargs["label"] = self._load_args["label"]
        elif self._load_args.get("version") is not None:
            get_kwargs["version"] = self._load_args["version"]
        else:
            get_kwargs["label"] = "latest"

        self._cached_build_args = get_kwargs
        return self._cached_build_args

    def _sync_strict_policy(
        self, local_data: str | list | None, langfuse_prompt: Any | None
    ) -> Any:
        """Handle strict sync policy - error if local and remote differ.

        Args:
            local_data: Content from local file, None if file doesn't exist.
            langfuse_prompt: Langfuse prompt object, None if not found remotely.

        Returns:
            Langfuse prompt object if sync is successful.

        Raises:
            DatasetError: If either local_data or langfuse_prompt is missing, or if they differ.
        """
        if not local_data or not langfuse_prompt:
            missing_parts = []
            if not local_data:
                missing_parts.append("local file")
            if not langfuse_prompt:
                missing_parts.append("remote prompt")

            raise DatasetError(
                f"Strict sync policy specified for {self._get_prompt_description()}. "
                f"Both local and remote prompts must exist in strict mode. "
                f"Missing: {' and '.join(missing_parts)}."
            )

        local_hash = _hash(_get_content(local_data))
        remote_hash = _hash(_get_content(langfuse_prompt.prompt))
        if local_hash != remote_hash:
            raise DatasetError(
                f"Strict sync failed for {self._get_prompt_description()}: "
                f"local and remote prompts differ. Use 'local' or 'remote' policy to resolve."
            )
        return langfuse_prompt

    def _adapt_langfuse_chat_format(self, prompt_data: str | list) -> str | list:
        """Remove Langfuse-specific 'type' key from chat messages for local file compatibility.

        Args:
            prompt_data: The prompt data from Langfuse (string or list of messages).

        Returns:
            New prompt data with 'type' key removed from messages if present.
            For string prompts, returns the input unchanged.
        """
        if isinstance(prompt_data, list):
            # Return new list instead of mutating input
            adapted_messages = []
            for msg in prompt_data:
                if isinstance(msg, dict) and "type" in msg:
                    # Create new dict without the type key
                    adapted_msg = {k: v for k, v in msg.items() if k != "type"}
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
            DatasetError: If remote prompt doesn't exist
        """
        if not langfuse_prompt:
            raise DatasetError(
                f"Remote sync policy specified for {self._get_prompt_description()} "
                "but no remote prompt exists in Langfuse. Confirm that you've configured "
                f"the correct Langfuse host and create the prompt in Langfuse first or use 'local' sync policy."
            )
        if not local_data or _hash(_get_content(local_data)) != _hash(_get_content(langfuse_prompt.prompt)):
            normalized_prompt = self._adapt_langfuse_chat_format(langfuse_prompt.prompt)
            logger.warning(f"Creating/Overwriting local file '{self._filepath}' with remote prompt '{self._prompt_name}' from Langfuse (remote sync policy)")
            self.file_dataset.save(normalized_prompt)
        return langfuse_prompt

    def _sync_local_policy(
        self, local_data: str | None, langfuse_prompt: Any | None
    ) -> Any:
        """
        Handle local sync policy - local file takes precedence.

        Local files are the source of truth. When local content differs from remote,
        the local content is pushed to Langfuse as a new version. If local file is missing
        but remote exists, the remote content is saved locally.

        Args:
            local_data: Content from local file, None if file doesn't exist
            langfuse_prompt: Langfuse prompt object, None if not found remotely

        Returns:
            Any: Langfuse prompt object after syncing

        Raises:
            DatasetError: If neither local nor remote prompt exists
        """
        if local_data is not None:
            if langfuse_prompt is None:
                # Push local to Langfuse
                logger.info(f"Creating '{self._prompt_name}' prompt in Langfuse from local file '{self._filepath}' as remote prompt does not exist (local sync policy)")
                self.save(local_data)
                return self._langfuse.get_prompt(**self._get_build_args)

            # If mismatch → update Langfuse with local
            if _hash(_get_content(local_data)) != _hash(
                _get_content(langfuse_prompt.prompt)
            ):
                logger.warning(f"Creating a new version of '{self._prompt_name}' prompt in Langfuse from local file '{self._filepath}' as local file prompt content does not match with remote prompt (local sync policy)")
                # Push local to Langfuse
                self.save(local_data)
                return self._langfuse.get_prompt(**self._get_build_args)
            return langfuse_prompt

        # If local missing but Langfuse exists → persist locally
        if langfuse_prompt:
            normalized_prompt = self._adapt_langfuse_chat_format(langfuse_prompt.prompt)
            logger.warning(f"Creating local file '{self._filepath}' from remote prompt '{self._prompt_name}' from Langfuse as local file is missing (local sync policy)")
            self.file_dataset.save(normalized_prompt)
            return langfuse_prompt

        raise DatasetError(
            f"No prompt found locally at {self._filepath} or in Langfuse for {self._get_prompt_description()}"
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
            DatasetError: Based on sync_policy conflicts (see individual policy methods)
            DatasetError: If no prompt found locally or in Langfuse
        """
        if self._sync_policy == "strict":
            return self._sync_strict_policy(local_data, langfuse_prompt)
        elif self._sync_policy == "remote":
            return self._sync_remote_policy(local_data, langfuse_prompt)
        else:  # local policy (default)
            return self._sync_local_policy(local_data, langfuse_prompt)

    def load(self) -> Union["ChatPromptTemplate", Any]:
        """Loads prompt from Langfuse, local file if present, and synchronizes
        based on sync_policy. Returns prompt in format specified by mode.

        Returns:
            ChatPromptTemplate: If mode="langchain", ready-to-use LangChain template.
            Any: If mode="sdk", raw Langfuse prompt object.

        Raises:
            DatasetError: If sync_policy conflicts or no prompt found.
        """
        # Temporarily suppress Langfuse logger to prevent Langfuse ERROR logs for 404s
        langfuse_logger = logging.getLogger('langfuse')
        original_level = langfuse_logger.level
        langfuse_logger.setLevel(logging.CRITICAL)

        try:
            langfuse_prompt = self._langfuse.get_prompt(**self._get_build_args)
        except (ConnectionError, TimeoutError) as e:
            logger.warning(f"Network error when fetching prompt '{self._prompt_name}' from langfuse: {e}. ")
            langfuse_prompt = None
        except Exception as e:
            logger.warning(
                f"Error when fetching prompt '{self._prompt_name}' from langfuse: {type(e).__name__}: {e}. ")
            langfuse_prompt = None
        finally:
            # Restore original logging level
            langfuse_logger.setLevel(original_level)

        # Load local file if it exists
        local_data = None
        if self._filepath.exists():
            local_data = self.file_dataset.load()

        # Synchronize local and remote
        langfuse_prompt = self._sync_with_langfuse(local_data, langfuse_prompt)

        if self._mode == "sdk":
            return langfuse_prompt
        elif self._mode == "langchain":
            from langchain.prompts import ChatPromptTemplate  # noqa: PLC0415
            return ChatPromptTemplate.from_messages(langfuse_prompt.get_langchain_prompt())
        else:
            raise DatasetError(f"Unsupported mode: {self._mode}. Must be 'sdk' or 'langchain'.")

    def preview(self) -> JSONPreview:
        """
        Generate a JSON-compatible preview of the underlying prompt data for Kedro-Viz.

        Automatically wraps string content in a JSON object to ensure compatibility
        with Kedro-Viz's JSON preview requirements. This prevents "src property must
        be a valid json object" errors when the local file contains plain text.

        Returns:
            JSONPreview: A Kedro-Viz-compatible object containing a serialized JSON string.
                String content is wrapped in {"content": <string>} format for proper
                JSON object structure. Returns error message if local file doesn't exist.
        """
        if self._filepath.exists():
            local_data = self.file_dataset.load()

            # If local_data is just a string, wrap it in a JSON object
            if isinstance(local_data, str):
                local_data = {"content": local_data}

            return JSONPreview(json.dumps(local_data))

        return JSONPreview("Local prompt does not exist.")
