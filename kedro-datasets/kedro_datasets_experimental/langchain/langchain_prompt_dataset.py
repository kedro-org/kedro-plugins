from pathlib import Path
from typing import Any, Dict, Union
from kedro.io import AbstractDataset, DatasetError
from kedro.io.core import get_filepath_str, parse_dataset_definition
from langchain.prompts import PromptTemplate, ChatPromptTemplate


class LangChainPromptDataset(AbstractDataset[Union[PromptTemplate, ChatPromptTemplate], Any]):
    """Kedro dataset for loading LangChain prompts."""
    
    TEMPLATES = {
        "PromptTemplate": PromptTemplate,
        "ChatPromptTemplate": ChatPromptTemplate,
    }
    
    def __init__(
        self,
        filepath: str,
        template: str = "PromptTemplate",
        dataset: Dict[str, Any] = None,
        **kwargs
    ):
        """
        Initialize the LangChain prompt dataset.
        
        Args:
            filepath: Path to the prompt file
            template: Name of the LangChain template class ("PromptTemplate" or "ChatPromptTemplate")
            dataset: Configuration for the underlying Kedro dataset
            **kwargs: Additional arguments (ignored)
        """
        super().__init__()
        
        self._filepath = get_filepath_str(Path(filepath), kwargs.get("protocol"))
        
        # We can add more templates in the future
        if template not in self.TEMPLATES:
            raise DatasetError(
                f"Invalid template '{template}'. Must be one of: {list(self.TEMPLATES.keys())}"
            )
        self._template_class = self.TEMPLATES[template]
        
        # If the user doesn't explicitly pick a dataset, try to infer from the file extension
        if dataset is None:
            if self._filepath.endswith('.txt'):
                dataset = {"type": "text.TextDataset"}
            elif self._filepath.endswith('.json'):
                dataset = {"type": "json.JSONDataset"}
            elif self._filepath.endswith(('.yaml', '.yml')):
                dataset = {"type": "yaml.YAMLDataset"}
            else:
                raise DatasetError(f"Cannot auto-detect dataset type for file: {self._filepath}")
        
        dataset_config = dict(dataset)
        dataset_config["filepath"] = self._filepath
        
        try:
            dataset_class, dataset_kwargs = parse_dataset_definition(dataset_config)
            self._dataset = dataset_class(**dataset_kwargs)
        except Exception as e:
            raise DatasetError(f"Failed to create underlying dataset: {e}")
    
    def load(self) -> Union[PromptTemplate, ChatPromptTemplate]:
        """Load data using underlying dataset and wrap in LangChain template."""
        try:
            raw_data = self._dataset.load()
        except Exception as e:
            raise DatasetError(f"Failed to load data from {self._filepath}: {e}")
        
        if raw_data is None:
            raise DatasetError(f"No data loaded from {self._filepath}")
        
        # Create LangChain template based chosen options
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
                # If it has a template key, handle it specially, otherwise pass all kwargs
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
        
        # Convert lists to tuples: ["role", "content"] -> ("role", "content")
        converted_messages = [
            tuple(msg) if isinstance(msg, list) and len(msg) >= 2 else msg
            for msg in messages
        ]
        
        return ChatPromptTemplate.from_messages(converted_messages)
    
    def save(self, data: Any) -> None:
        """Saving is not supported."""
        raise DatasetError("Saving is not supported for LangChainPromptDataset")
    
    def _describe(self) -> Dict[str, Any]:
        """Return dataset description."""
        return {
            "path": self._filepath,
            "template": self._template_class.__name__,
            "underlying_dataset": self._dataset.__class__.__name__,
        }
    
    def _exists(self) -> bool:
        """Check if the dataset exists."""
        return self._dataset._exists() if hasattr(self._dataset, '_exists') else True
