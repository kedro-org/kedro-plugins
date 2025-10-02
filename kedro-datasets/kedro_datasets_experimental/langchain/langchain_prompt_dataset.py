from copy import deepcopy
from pathlib import Path
from typing import Any

from kedro.io import AbstractDataset, DatasetError
from kedro.io.catalog_config_resolver import CREDENTIALS_KEY
from kedro.io.core import get_filepath_str, parse_dataset_definition
from langchain.prompts import ChatPromptTemplate, PromptTemplate

# Minimum number of elements required for a message (role, content)
MIN_MESSAGE_LENGTH = 2


class LangChainPromptDataset(AbstractDataset[PromptTemplate | ChatPromptTemplate, Any]):
    """Kedro dataset for loading LangChain prompts using existing Kedro datasets."""

    TEMPLATES = {
        "PromptTemplate": PromptTemplate,
        "ChatPromptTemplate": ChatPromptTemplate,
    }

    def __init__(  # noqa: PLR0913
        self,
        filepath: str,
        template: str = "PromptTemplate",
        dataset: dict[str, Any] | str | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs,
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

        # Pick template
        if template not in self.TEMPLATES:
            raise DatasetError(
                f"Invalid template '{template}'. Must be one of: {list(self.TEMPLATES.keys())}"
            )
        self._template_class = self.TEMPLATES[template]

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
            if self._template_class == ChatPromptTemplate:
                return self._create_chat_prompt_template(raw_data)
            else:
                return self._create_prompt_template(raw_data)
        except Exception as e:
            raise DatasetError(f"Failed to create {self._template_class.__name__}: {e}")

    def _create_prompt_template(self, raw_data: Any) -> PromptTemplate:
        """Create a PromptTemplate from loaded data."""
        if isinstance(raw_data, str):
            return PromptTemplate.from_template(raw_data)

        if isinstance(raw_data, dict):
            if "template" in raw_data and "input_variables" not in raw_data:
                return PromptTemplate.from_template(raw_data["template"])
            return PromptTemplate(**raw_data)

        raise DatasetError(f"Unsupported data type for PromptTemplate: {type(raw_data)}")

    def _create_chat_prompt_template(self, raw_data: Any) -> ChatPromptTemplate:
        """Create a ChatPromptTemplate from loaded data."""
        if not isinstance(raw_data, dict):
            raise DatasetError(f"ChatPromptTemplate requires dict data, got: {type(raw_data)}")

        if "messages" not in raw_data:
            raise DatasetError("ChatPromptTemplate requires a 'messages' key in the data")

        messages = raw_data["messages"]
        if not messages or not isinstance(messages, list):
            raise DatasetError("Messages must be a non-empty list")

        converted_messages = [
            tuple(msg) if isinstance(msg, list) and len(msg) >= MIN_MESSAGE_LENGTH else msg
            for msg in messages
        ]
        return ChatPromptTemplate.from_messages(converted_messages)

    def save(self, data: Any) -> None:
        raise DatasetError("Saving is not supported for LangChainPromptDataset")

    def _describe(self) -> dict[str, Any]:
        clean_config = {
            k: v for k, v in getattr(self._dataset, "_config", {}).items() if k != CREDENTIALS_KEY
        }
        return {
            "path": self._filepath,
            "template": self._template_class.__name__,
            "underlying_dataset": self._dataset.__class__.__name__,
            "dataset_config": clean_config,
        }

    def _exists(self) -> bool:
        return self._dataset._exists() if hasattr(self._dataset, "_exists") else True
