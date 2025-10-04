import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from kedro.io import AbstractDataset, DatasetError
from kedro.io.catalog_config_resolver import CREDENTIALS_KEY
from kedro.io.core import get_filepath_str, parse_dataset_definition
from langchain.prompts import ChatPromptTemplate, PromptTemplate

from kedro_datasets._typing import JSONPreview


class LangChainPromptDataset(AbstractDataset[PromptTemplate | ChatPromptTemplate, Any]):
    """Kedro dataset for loading LangChain prompts using existing Kedro datasets."""

    TEMPLATES = {
        "PromptTemplate": "_create_prompt_template",
        "ChatPromptTemplate": "_create_chat_prompt_template",
    }

    def __init__(  # noqa: PLR0913
        self,
        filepath: str,
        template: str = "PromptTemplate",
        dataset: dict[str, Any] | str | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        """
        Initialize the LangChain prompt dataset.

        Args:
            filepath: Path to the prompt file
            template: Name of the LangChain template class ("PromptTemplate" or "ChatPromptTemplate")
            dataset: Configuration for the underlying Kedro dataset
            credentials: Credentials passed to the underlying dataset unless already defined
            fs_args: Extra arguments passed to the filesystem, if supported
            metadata: Arbitrary metadata
            **kwargs: Additional arguments (ignored)
        """
        super().__init__()

        self.metadata = metadata
        self._filepath = get_filepath_str(Path(filepath), kwargs.get("protocol"))

        try:
            self._template_name = template
            self._create_template_function = getattr(self, self.TEMPLATES[template])
        except KeyError:
            raise DatasetError(
                f"Invalid template '{template}'. Must be one of: {list(self.TEMPLATES)}"
            )

        # Infer dataset type if not explicitly provided
        dataset_config = self._build_dataset_config(dataset)

        # Handle credentials
        self._credentials = deepcopy(credentials or {})
        self._fs_args = deepcopy(fs_args or {})

        if self._credentials:
            if CREDENTIALS_KEY in dataset_config:
                self._logger.warning(
                    "Top-level credentials will not propagate into the underlying dataset "
                    "since credentials were explicitly defined in the dataset config."
                )
            else:
                dataset_config[CREDENTIALS_KEY] = deepcopy(self._credentials)

        if self._fs_args:
            if "fs_args" in dataset_config:
                self._logger.warning(
                    "Top-level fs_args will not propagate into the underlying dataset "
                    "since fs_args were explicitly defined in the dataset config."
                )
            else:
                dataset_config["fs_args"] = deepcopy(self._fs_args)

        try:
            dataset_class, dataset_kwargs = parse_dataset_definition(dataset_config)
            self._dataset = dataset_class(**dataset_kwargs)
        except Exception as e:
            raise DatasetError(f"Failed to create underlying dataset: {e}")

    def _build_dataset_config(self, dataset: dict[str, Any] | str | None) -> dict[str, Any]:
        """Infer and normalize dataset configuration."""
        if dataset is None:
            if self._filepath.endswith(".txt"):
                dataset = {"type": "text.TextDataset"}
            elif self._filepath.endswith(".json"):
                dataset = {"type": "json.JSONDataset"}
            elif self._filepath.endswith((".yaml", ".yml")):
                dataset = {"type": "yaml.YAMLDataset"}
            else:
                raise DatasetError(f"Cannot auto-detect dataset type for file: {self._filepath}")

        dataset_config = dataset if isinstance(dataset, dict) else {"type": dataset}
        dataset_config = deepcopy(dataset_config)
        dataset_config["filepath"] = self._filepath

        return dataset_config

    def load(self) -> PromptTemplate | ChatPromptTemplate:
        """Load data using underlying dataset and wrap in LangChain template."""
        try:
            raw_data = self._dataset.load()
        except Exception as e:
            raise DatasetError(f"Failed to load data from {self._filepath}: {e}")

        if raw_data is None:
            raise DatasetError(f"No data loaded from {self._filepath}")

        try:
            return self._create_template_function(raw_data)
        except Exception as e:
            raise DatasetError(f"Failed to create {self._template_name}: {e}")

    def _create_prompt_template(self, raw_data: str | dict[str]) -> PromptTemplate:
        """
        Create a `PromptTemplate` from loaded raw data.

        This method supports either a string template or a dictionary
        containing the prompt configuration.

        Args:
            raw_data (str | dict): Either a string representing the template,
                or a dictionary with keys compatible with `PromptTemplate` initialization
                (e.g., `template`, `input_variables`).

        Returns:
            PromptTemplate: A LangChain `PromptTemplate` instance initialized
                with the provided template data.

        Raises:
            DatasetError: If `raw_data` is not a string or dictionary.

        Examples:
            >>> dataset._create_prompt_template("Hello {name}!")
            PromptTemplate(template='Hello {name}!', input_variables=['name'])

            >>> dataset._create_prompt_template({
            ...     "template": "Hello {name}!",
            ...     "input_variables": ["name"]
            ... })
            PromptTemplate(template='Hello {name}!', input_variables=['name'])
        """
        if isinstance(raw_data, str):
            return PromptTemplate.from_template(raw_data)

        if isinstance(raw_data, dict):
           return PromptTemplate(**raw_data)

        raise DatasetError(f"Unsupported data type for PromptTemplate: {type(raw_data)}")

    def _validate_chat_prompt_data(self, data: dict | list[tuple[str, str]]) -> None:
        """Validate that chat prompt data exists and is not empty."""
        messages = data.get("messages") if isinstance(data, dict) else data
        if not messages:
            raise DatasetError(
                "ChatPromptTemplate requires a non-empty list of messages"
            )

        return messages

    def _create_chat_prompt_template(self, data: dict | list[tuple[str, str]]) -> ChatPromptTemplate:
        """
        Create a `ChatPromptTemplate` from validated chat data.

        Supports either:
        - A dictionary in the LangChain chat JSON format (`{"messages": [{"role": "...", "content": "..."}]}`),
        - Or a list of `(role, content)` tuples.

        Args:
            data (dict | list[tuple[str, str]]): Chat prompt data to validate and transform.

        Returns:
            ChatPromptTemplate: A LangChain `ChatPromptTemplate` instance.

        Raises:
            DatasetError: If  cannot be used to create a `ChatPromptTemplate`.

        Examples:
            >>> dataset._create_chat_prompt_template({
            ...     "messages": [
            ...         {"role": "system", "content": "You are a helpful assistant."},
            ...         {"role": "user", "content": "Hello, who are you?"}
            ...     ]
            ... })
            ChatPromptTemplate(messages=[...])

            >>> dataset._create_chat_prompt_template([
            ...     ("user", "Hello"),
            ...     ("ai", "Hi there!")
            ... ])
            ChatPromptTemplate(messages=[...])
        """
        messages = self._validate_chat_prompt_data(data)
        return ChatPromptTemplate.from_messages(messages)

    def save(self, data: Any) -> None:
        raise DatasetError("Saving is not supported for LangChainPromptDataset")

    def _describe(self) -> dict[str, Any]:
        clean_config = {
            k: v for k, v in getattr(self._dataset, "_config", {}).items() if k != CREDENTIALS_KEY
        }
        return {
            "path": self._filepath,
            "template": self._template_name,
            "underlying_dataset": self._dataset.__class__.__name__,
            "dataset_config": clean_config,
        }

    def _exists(self) -> bool:
        return self._dataset._exists() if hasattr(self._dataset, "_exists") else True

    def preview(self) -> JSONPreview:
        """Generate a preview of the prompt data for Kedro-Viz."""
        try:
            data = self._dataset.load()

            if isinstance(data, str):
                # Wrap plain text in a dictionary or Viz doesn't render it
                data = {"text": data}

            # Restructure output so it's compatible with JSON
            if isinstance(data, dict) and "messages" in data:
                msgs = data["messages"]
                if isinstance(msgs, dict):
                    data["messages"] = [{"role": k, "content": v} for k, v in msgs.items()]

            return JSONPreview(json.dumps(data))

        except Exception as e:
            return JSONPreview(f"Error generating preview: {e}")
