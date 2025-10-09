import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from kedro.io import DatasetError
from langchain.prompts import ChatPromptTemplate, PromptTemplate

from kedro_datasets_experimental.langchain.langchain_prompt_dataset import (
    LangChainPromptDataset,
)


@pytest.fixture
def txt_prompt_file(tmp_path: Path) -> Path:
    """Create a temporary text file with a simple prompt."""
    prompt_file = tmp_path / "prompt.txt"
    prompt_file.write_text("Hello, this is {name}!")
    return prompt_file


@pytest.fixture
def json_prompt_file(tmp_path: Path) -> Path:
    """Create a temporary JSON file with a prompt configuration."""
    prompt_file = tmp_path / "prompt.json"
    prompt_file.write_text(
        """
        {
            "template": "Hello, this is {name}!",
            "input_variables": ["name"]
        }
        """
    )
    return prompt_file


@pytest.fixture
def chat_prompt_file(tmp_path: Path) -> Path:
    """Create a temporary JSON file with a chat prompt configuration."""
    prompt_file = tmp_path / "chat.json"
    prompt_file.write_text(
        """
        {
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Hello, this is {name}!"}
            ]
        }
        """
    )
    return prompt_file


@pytest.fixture
def yaml_prompt_file(tmp_path: Path) -> Path:
    """Create a temporary YAML file with a simple prompt configuration."""
    prompt_file = tmp_path / "prompt.yaml"
    content = {
        "template": "Hello, this is {name}!",
        "input_variables": ["name"]
    }
    prompt_file.write_text(yaml.dump(content))
    return prompt_file


@pytest.fixture
def chat_yaml_prompt_file(tmp_path: Path) -> Path:
    """Create a temporary YAML file with a chat prompt configuration."""
    prompt_file = tmp_path / "chat_prompt.yaml"
    content = {
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Hello, this is {name}!"}
        ]
    }
    prompt_file.write_text(yaml.dump(content))
    return prompt_file


class TestLangChainPromptDataset:
    def test_init_with_txt_file(self, txt_prompt_file: Path) -> None:
        """Test dataset initialization with a .txt file."""
        dataset = LangChainPromptDataset(
            filepath=str(txt_prompt_file),
            dataset={"type": "text.TextDataset"}
            )
        assert dataset._template_name == "PromptTemplate"
        assert dataset._dataset.__class__.__name__ == "TextDataset"

    def test_init_with_json_file(self, json_prompt_file: Path) -> None:
        """Test dataset initialization with a .json file."""
        dataset = LangChainPromptDataset(
            filepath=str(json_prompt_file),
            dataset={"type": "json.JSONDataset"}
            )
        assert dataset._template_name == "PromptTemplate"
        assert dataset._dataset.__class__.__name__ == "JSONDataset"

    def test_init_with_invalid_template(self, txt_prompt_file: Path) -> None:
        """Test initialization with invalid template type."""
        with pytest.raises(DatasetError, match="Invalid template"):
            LangChainPromptDataset(
                filepath=str(txt_prompt_file),
                dataset={"type": "text.TextDataset"},
                template="InvalidTemplate"
            )

    def test_load_text_prompt(self, txt_prompt_file: Path) -> None:
        """Test loading a simple text prompt."""
        dataset = LangChainPromptDataset(
            filepath=str(txt_prompt_file),
            dataset={"type": "kedro_datasets.text.TextDataset"}
            )
        prompt = dataset.load()
        assert isinstance(prompt, PromptTemplate)
        assert prompt.format(name="Kedro") == "Hello, this is Kedro!"

    def test_load_json_prompt(self, json_prompt_file: Path) -> None:
        """Test loading a JSON prompt configuration."""
        dataset = LangChainPromptDataset(
            filepath=str(json_prompt_file),
            dataset={"type": "kedro_datasets.json.JSONDataset"})
        prompt = dataset.load()
        assert isinstance(prompt, PromptTemplate)
        assert prompt.format(name="Kedro") == "Hello, this is Kedro!"

    def test_load_chat_prompt(self, chat_prompt_file: Path) -> None:
        """Test loading a chat prompt configuration."""
        dataset = LangChainPromptDataset(
            filepath=str(chat_prompt_file),
            dataset={"type": "json.JSONDataset"},
            template="ChatPromptTemplate"
        )
        prompt = dataset.load()
        assert isinstance(prompt, ChatPromptTemplate)
        messages = prompt.format_messages(name="Kedro")
        assert len(messages) == 2
        assert messages[0].content == "You are a helpful assistant."
        assert messages[1].content == "Hello, this is Kedro!"

    def test_load_yaml_prompt(self, yaml_prompt_file: Path) -> None:
        """Test loading a YAML prompt configuration."""
        dataset = LangChainPromptDataset(
            filepath=str(yaml_prompt_file),
            dataset={"type": "kedro_datasets.yaml.YAMLDataset"}
            )
        prompt = dataset.load()
        assert isinstance(prompt, PromptTemplate)
        assert prompt.format(name="Kedro") == "Hello, this is Kedro!"

    def test_load_yaml_chat_prompt(self, chat_yaml_prompt_file: Path) -> None:
        """Test loading a YAML chat prompt configuration."""
        dataset = LangChainPromptDataset(
            filepath=str(chat_yaml_prompt_file),
            dataset={"type": "yaml.YAMLDataset"},
            template="ChatPromptTemplate"
        )
        prompt = dataset.load()
        assert isinstance(prompt, ChatPromptTemplate)
        messages = prompt.format_messages(name="Kedro")
        assert len(messages) == 2
        assert messages[0].content == "You are a helpful assistant."
        assert messages[1].content == "Hello, this is Kedro!"


    def test_save_not_supported(self, txt_prompt_file: Path) -> None:
        """Test that save operation raises an error."""
        dataset = LangChainPromptDataset(
            filepath=str(txt_prompt_file),
            dataset={"type": "text.TextDataset"}
            )
        with pytest.raises(DatasetError, match="Saving is not supported"):
            dataset.save({})

    def test_describe(self, txt_prompt_file: Path) -> None:
        """Test the _describe method returns correct information."""
        dataset = LangChainPromptDataset(
            filepath=str(txt_prompt_file),
            dataset={"type": "text.TextDataset"})
        desc = dataset._describe()
        assert desc["template"] == "PromptTemplate"
        assert desc["underlying_dataset"] == "TextDataset"
        assert desc["path"] == str(txt_prompt_file)

    def test_exists(self, txt_prompt_file: Path) -> None:
        """Test the _exists method."""
        dataset = LangChainPromptDataset(
            filepath=str(txt_prompt_file),
            dataset={"type": "text.TextDataset"}
            )
        assert dataset._exists() is True

    def test_credentials_propagation(self, json_prompt_file: Path) -> None:
        """Test that credentials are properly propagated to the underlying dataset."""
        credentials = {"key": "value"}
        dataset = LangChainPromptDataset(
            filepath=str(json_prompt_file),
            dataset={"type": "json.JSONDataset"},
            credentials=credentials,
        )

        # JSONDataset stores credentials in the fsspec filesystem options
        # We are just checking that the credentials were passed correctly
        fs_options = getattr(dataset._dataset._fs, "storage_options", {})
        for k, v in credentials.items():
            assert fs_options.get(k) == v

    def test_chat_prompt_template_with_plain_string(self, tmp_path: Path) -> None:
        """Test that plain string data raises DatasetError for ChatPromptTemplate."""
        prompt_file = tmp_path / "plain_string.json"
        prompt_file.write_text('"Just a plain string, not a chat prompt."')
        dataset = LangChainPromptDataset(
            filepath=str(prompt_file),
            dataset={"type": "text.TextDataset"},
            template="ChatPromptTemplate"
        )
        with pytest.raises(DatasetError, match="Plain string data is only supported for PromptTemplate, not ChatPromptTemplate."):
            dataset.load()

    def test_invalid_dataset_type_raises_error(self, txt_prompt_file: Path) -> None:
        """Test that using an invalid dataset type raises DatasetError."""
        invalid_dataset = {"type": "pandas.CSVDataset"}
        with pytest.raises(DatasetError, match="Unsupported dataset type 'pandas.CSVDataset'"):
            LangChainPromptDataset(filepath=str(txt_prompt_file), dataset=invalid_dataset)

    def test_none_dataset_type_raises_error(self, txt_prompt_file: Path) -> None:
        """Test that passing no dataset type raises DatasetError."""
        invalid_dataset = None
        with pytest.raises(DatasetError, match="Underlying dataset type cannot be empty"):
            LangChainPromptDataset(filepath=str(txt_prompt_file), dataset=invalid_dataset)

    @pytest.mark.parametrize(
        "bad_data,error_pattern",
        [
            ({}, "ChatPromptTemplate requires a non-empty list of messages"),
            ([], "ChatPromptTemplate requires a non-empty list of messages"),
            (("single item"), "Expecting value"),
            ([{"content": "hello"}], "Expecting property name enclosed in double quotes"),
        ],
    )
    def test_invalid_chat_prompt_data(
        self, tmp_path: Path, bad_data: Any, error_pattern: str
    ) -> None:
        """Test validation of chat prompt data."""
        prompt_file = tmp_path / "bad_chat.json"
        prompt_file.write_text(str(bad_data))
        dataset = LangChainPromptDataset(
            filepath=str(prompt_file),
            dataset={"type": "json.JSONDataset"},
            template="ChatPromptTemplate"
        )

        with pytest.raises(DatasetError, match=f".*{error_pattern}.*"):
            dataset.load()

    @pytest.mark.parametrize(
        "bad_data,error_pattern",
        [
            ({}, "ChatPromptTemplate requires a non-empty list of messages"),
            ([], "ChatPromptTemplate requires a non-empty list of messages"),
            ([{"role": "user"}], "Expected dict to have exact keys 'role' and 'content'"),
            ([{"content": "hello"}], "Expected dict to have exact keys 'role' and 'content'"),
        ]
    )
    def test_invalid_yaml_chat_prompt(self, tmp_path: Path, bad_data: any, error_pattern: str) -> None:
        """Test invalid YAML chat prompts for various error cases."""
        prompt_file = tmp_path / "bad_chat.yaml"
        prompt_file.write_text(yaml.dump(bad_data))

        dataset = LangChainPromptDataset(
            filepath=str(prompt_file),
            dataset={"type": "yaml.YAMLDataset"},
            template="ChatPromptTemplate"
        )

        with pytest.raises(DatasetError, match=f".*{error_pattern}.*"):
            dataset.load()

    def test_preview_txt_prompt(self, txt_prompt_file: str):
        """Preview a plain text prompt returns raw string."""
        dataset = LangChainPromptDataset(
            filepath=str(txt_prompt_file),
            dataset={"type": "text.TextDataset"}
            )
        preview = dataset.preview()
        assert preview == '{"text": "Hello, this is {name}!"}'

    def test_preview_json_prompt(self, json_prompt_file: str):
        """Preview a JSON prompt returns serialized dict."""
        dataset = LangChainPromptDataset(
            filepath=str(json_prompt_file),
            dataset={"type": "json.JSONDataset"}
            )
        preview = dataset.preview()
        data = json.loads(preview)
        assert data["template"] == "Hello, this is {name}!"
        assert data["input_variables"] == ["name"]

    def test_preview_yaml_prompt(self, yaml_prompt_file: str):
        """Preview a YAML prompt returns serialized dict."""
        dataset = LangChainPromptDataset(
            filepath=str(yaml_prompt_file),
            dataset={"type": "yaml.YAMLDataset"}
            )
        preview = dataset.preview()
        data = json.loads(preview)
        assert data["template"] == "Hello, this is {name}!"
        assert data["input_variables"] == ["name"]

    def test_preview_chat_json_prompt(self, chat_prompt_file: str):
        """Preview a JSON chat prompt returns serialized messages list."""
        dataset = LangChainPromptDataset(
            filepath=str(chat_prompt_file),
            dataset={"type": "json.JSONDataset"},
            template="ChatPromptTemplate")
        preview = dataset.preview()
        data = json.loads(preview)
        assert isinstance(data, dict)
        assert data['messages'][0]['role'] == 'system'
        assert data['messages'][1]['role'] == 'user'

    def test_preview_chat_yaml_prompt(self, chat_yaml_prompt_file: str):
        """Preview a YAML chat prompt returns serialized messages list."""
        dataset = LangChainPromptDataset(
            filepath=str(chat_yaml_prompt_file),
            dataset={"type": "yaml.YAMLDataset"},
            template="ChatPromptTemplate")
        preview = dataset.preview()
        data = json.loads(preview)
        assert isinstance(data, dict)
        assert data['messages'][0]['role'] == 'system'
        assert data['messages'][1]['role'] == 'user'
