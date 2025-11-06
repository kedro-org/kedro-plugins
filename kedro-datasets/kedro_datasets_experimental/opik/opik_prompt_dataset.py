"""Opik prompt dataset for seamless integration between local prompt files (JSON/YAML) and Opik prompt management"""

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Union

from kedro.io import AbstractDataset, DatasetError

if TYPE_CHECKING:
    from langchain_core.prompts import ChatPromptTemplate

    from kedro_datasets.json import JSONDataset
    from kedro_datasets.yaml import YAMLDataset

from opik import Opik, Prompt

from kedro_datasets._typing import JSONPreview

# Supported file extensions for prompt storage
SUPPORTED_FILE_EXTENSIONS = {".json", ".yaml", ".yml"}

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


class OpikPromptDataset(AbstractDataset):
    """Kedro dataset for managing prompts with Opik versioning and synchronisation.

    This dataset provides seamless integration between local prompt files (JSON/YAML)
    and Opik's prompt management system, supporting version control, metadata tracking,
    and different synchronisation policies.

    **On save / load behaviour:**

    - **On save:** Creates a new version of the prompt in Opik with the local data.
    - **On load:** Synchronises based on `sync_policy` and returns either the raw
      Opik object (SDK mode) or a LangChain `ChatPromptTemplate` (LangChain mode).

    **Sync policies:**

    - **local:** Local file takes precedence (default). `load_args` are ignored with a
      warning, since local files are the source of truth.
    - **remote:** Opik version takes precedence. `load_args` are respected if supported.
    - **strict:** Raises an error if local and remote differ. `load_args` are respected
      if supported

    Examples:
        Using catalog YAML configuration:

        ```yaml
        # Local sync policy - local files are source of truth
        customer_prompt:
          type: kedro_datasets_experimental.opik.OpikPromptDataset
          filepath: data/prompts/customer.json
          prompt_name: customer_support_v1
          prompt_type: chat
          credentials: opik_credentials
          sync_policy: local
          mode: langchain

        # Remote sync policy - Opik versions are source of truth
        production_prompt:
          type: kedro_datasets_experimental.opik.OpikPromptDataset
          filepath: data/prompts/production.yaml
          prompt_name: customer_support_v1
          sync_policy: remote
          mode: sdk
        ```

        Using Python API:

        ```python
        from kedro_datasets_experimental.opik import OpikPromptDataset

        # Create dataset for chat prompt
        dataset = OpikPromptDataset(
            filepath="data/prompts/customer_support.json",
            prompt_name="customer_support_v1",
            prompt_type="chat",
            credentials={
                "api_key": "opik_...",  # pragma: allowlist secret
                "workspace": "my-workspace",
            },
        )

        # Load prompt as LangChain ChatPromptTemplate
        prompt_template = dataset.load()
        formatted = prompt_template.format(question="How are you?")

        # Save with metadata
        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Hello, {question}"},
        ]
        dataset.save(messages)
        ```
    """

    def __init__(  # noqa: PLR0913
        self,
        filepath: str,
        prompt_name: str,
        credentials: dict[str, Any],
        prompt_type: Literal["chat", "text"] = "text",
        sync_policy: Literal["local", "remote", "strict"] = "local",
        mode: Literal["langchain", "sdk"] = "sdk",
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        **opik_kwargs: Any
    ):
        """Initialise OpikPromptDataset with local and remote configuration.

        Args:
            filepath: Local file path for storing prompt. Supports .json, .yaml, .yml extensions.
            prompt_name: Unique identifier for the prompt in Opik.
            prompt_type: Either "chat" for message-based prompts or "text" for simple strings.
            sync_policy: How to handle conflicts between local and remote:
                - "local": Local file takes precedence (default)
                - "remote": Opik version takes precedence
                - "strict": Error if local and remote differ
            mode: Return type for load() method:
                - "sdk": Returns raw Opik prompt object (default)
                - "langchain": Returns ChatPromptTemplate object
            credentials: Dictionary with Opik client configuration. Keys may include
                'api_key', 'workspace', 'project_name', etc.
            load_args: Dictionary with loading parameters. Currently reserved for future use
                when Opik supports version/label retrieval.
            save_args: Dictionary with saving parameters. Keys may include:
                - metadata: Additional metadata to store with the prompt
            **opik_kwargs: Additional kwargs passed to Opik client initialisation.

        Raises:
            DatasetError: If required parameters are missing or invalid.
            NotImplementedError: If filepath has unsupported extension.
            ImportError: If langchain is required but not installed.
        """
        # Validate parameters
        self._validate_init_params(filepath, prompt_type, sync_policy, mode)

        self._filepath = Path(filepath)
        self._prompt_name = prompt_name
        self._prompt_type = prompt_type or "text"
        self._sync_policy = sync_policy or "local"
        self._mode = mode or "sdk"
        self._load_args = load_args or {}
        self._save_args = save_args or {}
        self._file_dataset = None

        # Initialise Opik client
        try:
            self._opik_client = Opik(**credentials, **opik_kwargs)
        except Exception as e:
            raise DatasetError(f"Failed to initialise Opik client: {e}")

        # Ensure dataset exists for tracking
        self._ensure_dataset_exists()

    def _validate_init_params(
        self,
        filepath: str,
        prompt_type: str,
        sync_policy: str,
        mode: str
    ) -> None:
        """Validate initialisation parameters.

        Args:
            filepath: File path to validate.
            prompt_type: Prompt type to validate.
            sync_policy: Sync policy to validate.
            mode: Mode to validate.

        Raises:
            DatasetError: If parameters are invalid.
            NotImplementedError: If filepath has unsupported extension.
            ImportError: If required packages are missing.
        """
        # Validate file extension
        file_path = Path(filepath)
        if file_path.suffix.lower() not in SUPPORTED_FILE_EXTENSIONS:
            raise NotImplementedError(
                f"Unsupported file extension '{file_path.suffix}'. "
                f"Supported formats: {', '.join(sorted(SUPPORTED_FILE_EXTENSIONS))}"
            )

        # Validate enum parameters
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

        # Validate mode-specific requirements
        if mode == "langchain":
            try:
                from langchain_core.prompts import ChatPromptTemplate  # noqa: PLC0415
            except ImportError as exc:
                raise ImportError(
                    "The 'langchain-core' package is required when using mode='langchain'. "
                    "Install it with: pip install 'kedro-datasets[opik]'"
                ) from exc

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

    def _describe(self) -> dict[str, Any]:
        """Return a description of the dataset for Kedro's internal use."""
        return {
            "filepath": str(self._filepath),
            "prompt_name": self._prompt_name,
            "prompt_type": self._prompt_type,
            "sync_policy": self._sync_policy,
            "mode": self._mode
        }

    def save(self, data: str | list[dict[str, str]]) -> None:
        """Save prompt to Opik.

        Args:
            data: The prompt content to save. Can be string for text prompts
                or list of message dictionaries for chat prompts.

        Raises:
            DatasetError: If data format is invalid or save fails.
        """
        # Validate data format
        if self._prompt_type == "chat" and not isinstance(data, list):
            raise DatasetError("Chat prompts must be a list of message dicts")
        if self._prompt_type == "text" and not isinstance(data, str):
            raise DatasetError("Text prompts must be a string")

        try:
            create_kwargs = {
                "name": self._prompt_name,
                "prompt": json.dumps(data, ensure_ascii=False) if not isinstance(data, str) else data,
            }

            # Add metadata if provided
            metadata = {"type": self._prompt_type}
            if "metadata" in self._save_args:
                metadata.update(self._save_args["metadata"])
            create_kwargs["metadata"] = metadata

            self._opik_client.create_prompt(**create_kwargs)
            logger.info(f"Successfully saved prompt '{self._prompt_name}' to Opik")
        except Exception as e:
            raise DatasetError(f"Failed to save prompt to Opik: {e}")

    def _get_prompt_data(self) -> tuple[Prompt | None, str | list[dict[str, str]] | None]:
        """Fetch latest prompt from Opik.

        Returns:
            Tuple of (Prompt object or None, prompt data as str/list or None).
        """
        try:
            # TODO: If Opik supports version/label in the future, add logic here to fetch prompt by version/label using self._load_args.

            opik_prompt = self._opik_client.get_prompt(name=self._prompt_name)
            prompt_data = opik_prompt.prompt

            # Try to parse JSON if it's a string
            if isinstance(prompt_data, str):
                try:
                    prompt_data = json.loads(prompt_data)
                except json.JSONDecodeError:
                    pass  # Keep as string

            return opik_prompt, prompt_data
        except Exception as e:
            logger.debug(f"Could not fetch prompt from Opik: {e}")
            return None, None

    def _sync_strict_policy(
        self, local_data: str | list | None, opik_prompt: Prompt | None
    ) -> tuple[Prompt | None, str | list | None]:
        """Handle strict sync policy - error if local and remote differ.

        Args:
            local_data: Local prompt data (string or list of messages) or None.
            opik_prompt: Opik Prompt object or None.

        Returns:
            Tuple of (Prompt object, prompt data).

        Raises:
            DatasetError: If local and remote prompts don't match or if either is missing.
        """
        if not local_data or not opik_prompt:
            missing_parts = []
            if not local_data:
                missing_parts.append("local file")
            if not opik_prompt:
                missing_parts.append("remote prompt")

            raise DatasetError(
                f"Strict sync policy specified for '{self._prompt_name}'. "
                f"Both local and remote prompts must exist in strict mode. "
                f"Missing: {' and '.join(missing_parts)}."
            )

        opik_data = opik_prompt.prompt
        if isinstance(opik_data, str):
            try:
                opik_data = json.loads(opik_data)
            except json.JSONDecodeError:
                pass

        local_hash = _hash(_get_content(local_data))
        remote_hash = _hash(_get_content(opik_data))

        if local_hash != remote_hash:
            raise DatasetError(
                f"Strict sync failed for '{self._prompt_name}': "
                f"local and remote prompts differ. Use 'local' or 'remote' policy to resolve."
            )

        return opik_prompt, opik_data

    def _sync_remote_policy(
        self, local_data: str | list | None, opik_prompt: Prompt | None
    ) -> tuple[Prompt | None, str | list | None]:
        """Handle remote sync policy - Opik version takes precedence.

        Args:
            local_data: Local prompt data (string or list of messages) or None.
            opik_prompt: Opik Prompt object or None.

        Returns:
            Tuple of (Prompt object, prompt data).

        Raises:
            DatasetError: If no remote prompt exists in Opik.
        """
        if not opik_prompt:
            raise DatasetError(
                f"Remote sync policy specified for '{self._prompt_name}' "
                f"but no remote prompt exists in Opik. Create the prompt in Opik first.\n"
                f"You can create it by:\n"
                f"1. Switching to sync_policy='local' and running once to push local to Opik\n"
                f"2. Using the Opik web UI at your configured workspace\n"
                f"3. Using the Opik Python SDK directly: "
                f"opik_client.create_prompt(name='{self._prompt_name}', prompt=...)"
            )

        opik_data = opik_prompt.prompt
        if isinstance(opik_data, str):
            try:
                opik_data = json.loads(opik_data)
            except json.JSONDecodeError:
                pass

        if not local_data or _hash(_get_content(local_data)) != _hash(_get_content(opik_data)):
            logger.info(
                f"Creating/Overwriting local file '{self._filepath}' with remote prompt "
                f"'{self._prompt_name}' from Opik (remote sync policy)"
            )
            self.file_dataset.save(opik_data)

        return opik_prompt, opik_data

    def _sync_local_policy(
        self, local_data: str | list | None, opik_prompt: Prompt | None
    ) -> tuple[Prompt | None, str | list | None]:
        """Handle local sync policy - local file takes precedence.

        Args:
            local_data: Local prompt data (string or list of messages) or None.
            opik_prompt: Opik Prompt object or None.

        Returns:
            Tuple of (Prompt object, prompt data).

        Raises:
            DatasetError: If no prompt found locally or in Opik.
        """
        if local_data is not None:
            if opik_prompt is None:
                logger.info(
                    f"Creating '{self._prompt_name}' prompt in Opik from local file '{self._filepath}' "
                    f"as remote prompt does not exist (local sync policy)"
                )
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
                logger.info(
                    f"Creating a new version of '{self._prompt_name}' prompt in Opik from local file "
                    f"'{self._filepath}' as local file content differs from remote (local sync policy)"
                )
                self.save(local_data)
                return self._get_prompt_data()

            return opik_prompt, local_data

        # If local missing but Opik exists → persist locally
        if opik_prompt:
            prompt_data = opik_prompt.prompt
            if isinstance(prompt_data, str):
                try:
                    prompt_data = json.loads(prompt_data)
                except json.JSONDecodeError:
                    pass

            logger.info(
                f"Creating local file '{self._filepath}' from remote prompt '{self._prompt_name}' "
                f"from Opik as local file is missing (local sync policy)"
            )
            try:
                self.file_dataset.save(prompt_data)
            except Exception as e:
                raise DatasetError(f"Failed to sync Opik prompt to local file: {e}")

            return opik_prompt, prompt_data

        raise DatasetError(
            f"No prompt found locally at '{self._filepath}' or in Opik for '{self._prompt_name}'"
        )

    def _sync_with_opik(
        self, local_data: str | list | None, opik_prompt: Prompt | None
    ) -> tuple[Prompt | None, str | list | None]:
        """Synchronise local file and Opik prompt based on sync policy.

        Args:
            local_data: Local prompt data (string or list of messages) or None.
            opik_prompt: Opik Prompt object or None.

        Returns:
            Tuple of (Prompt object, prompt data) after synchronisation.
        """
        if self._sync_policy == "strict":
            return self._sync_strict_policy(local_data, opik_prompt)
        elif self._sync_policy == "remote":
            return self._sync_remote_policy(local_data, opik_prompt)
        else:  # local policy (default)
            return self._sync_local_policy(local_data, opik_prompt)

    def _convert_to_langchain_template(
        self, prompt_data: str | list | None
    ) -> "ChatPromptTemplate":
        """Convert prompt data to LangChain ChatPromptTemplate.

        Args:
            prompt_data: Raw prompt data (string or list of messages).

        Returns:
            ChatPromptTemplate ready for use in LangChain pipelines.

        Raises:
            DatasetError: If prompt data format is invalid.
        """
        from langchain_core.prompts import ChatPromptTemplate  # noqa: PLC0415

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

    def load(self) -> Union["ChatPromptTemplate", Any]:
        """Load prompt with synchronisation logic.

        Returns:
            ChatPromptTemplate: If mode="langchain", ready-to-use LangChain template.
            Any: If mode="sdk", raw Opik prompt object.

        Raises:
            DatasetError: If prompt cannot be loaded or has invalid format.
        """
        # Warn about load_args in local mode
        if self._sync_policy == "local" and self._load_args:
            logger.warning(
                f"Ignoring load_args for prompt '{self._prompt_name}' "
                f"because sync_policy='local'. Local files are the source of truth."
            )

        # Try to fetch from Opik with error handling
        try:
            opik_prompt, prompt_data = self._get_prompt_data()
        except (ConnectionError, TimeoutError) as e:
            logger.warning(
                f"Network error when fetching prompt '{self._prompt_name}' from Opik: {e}. "
                f"Falling back to local file."
            )
            opik_prompt, prompt_data = None, None
        except Exception as e:
            logger.warning(
                f"Error when fetching prompt '{self._prompt_name}' from Opik: {e}. "
                f"Falling back to local file."
            )
            opik_prompt, prompt_data = None, None

        # Load local data if file exists
        local_data = None
        if self._filepath.exists():
            try:
                local_data = self.file_dataset.load()
            except Exception as e:
                raise DatasetError(f"Failed to read local prompt file: {e}")

        # Sync and get final data
        opik_prompt, prompt_data = self._sync_with_opik(local_data, opik_prompt)

        # Return based on mode
        if self._mode == "sdk":
            return opik_prompt
        elif self._mode == "langchain":
            return self._convert_to_langchain_template(prompt_data)
        else:
            raise DatasetError(f"Unsupported mode: {self._mode}")

    def preview(self) -> JSONPreview:
        """Generate a JSON-compatible preview for Kedro-Viz.

        Returns:
            JSONPreview: A Kedro-Viz-compatible preview of the prompt.
        """
        if self._filepath.exists():
            local_data = self.file_dataset.load()

            # Wrap string content in JSON object for Kedro-Viz compatibility
            if isinstance(local_data, str):
                local_data = {"content": local_data}

            return JSONPreview(json.dumps(local_data))

        return JSONPreview("Local prompt file does not exist.")
