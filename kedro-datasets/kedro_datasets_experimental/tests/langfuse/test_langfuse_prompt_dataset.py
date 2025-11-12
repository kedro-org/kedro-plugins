import json
from unittest.mock import Mock, patch

import pytest
import yaml
from kedro.io import DatasetError

from kedro_datasets_experimental.langfuse.langfuse_prompt_dataset import (
    LangfusePromptDataset,
    _get_content,
    _hash,
)


@pytest.fixture
def mock_langfuse():
    """Mock Langfuse client for testing."""
    with patch("kedro_datasets_experimental.langfuse.langfuse_prompt_dataset.Langfuse") as mock:
        langfuse_instance = Mock()
        mock.return_value = langfuse_instance
        yield langfuse_instance


@pytest.fixture
def mock_credentials():
    """Valid Langfuse credentials for testing."""
    return {
        "public_key": "pk_test_12345",
        "secret_key": "sk_test_67890",  # pragma: allowlist secret
        "host": "https://cloud.langfuse.com"
    }

@pytest.fixture
def filepath_json_chat(tmp_path):
    """Create a temporary JSON file with a chat prompt."""
    prompt_file = tmp_path / "chat_prompt.json"
    chat_data = [
        {
            "role": "system",
            "content": "You are a helpful insurance support assistant."
        },
        {
            "role": "human",
            "content": "{input}"
        }
    ]
    prompt_file.write_text(json.dumps(chat_data, indent=2))
    return prompt_file.as_posix()


@pytest.fixture
def filepath_yaml_chat(tmp_path):
    """Create a temporary YAML file with a chat prompt."""
    prompt_file = tmp_path / "chat_prompt.yaml"
    chat_data = [
        {
            "role": "system",
            "content": "You are a helpful insurance support assistant."
        },
        {
            "role": "human",
            "content": "{input}"
        }
    ]
    prompt_file.write_text(yaml.dump(chat_data))
    return prompt_file.as_posix()


@pytest.fixture
def filepath_json_text(tmp_path):
    """Create a temporary JSON file with a text prompt."""
    prompt_file = tmp_path / "text_prompt.json"
    text_data = "Classify the following user query: {input}"
    prompt_file.write_text(json.dumps(text_data))
    return prompt_file.as_posix()


@pytest.fixture
def filepath_yaml_text(tmp_path):
    """Create a temporary YAML file with a text prompt."""
    prompt_file = tmp_path / "text_prompt.yaml"
    text_data = "Classify the following user query: {input}"
    prompt_file.write_text(yaml.dump(text_data))
    return prompt_file.as_posix()


@pytest.fixture
def mock_langfuse_prompt():
    """Mock Langfuse prompt object."""
    prompt = Mock()
    prompt.prompt = [
        {
            "role": "system",
            "content": "You are helpful."
        },
        {
            "role": "human",
            "content": "{input}"
        }
    ]
    prompt.version = 1
    prompt.labels = ["latest"]
    prompt.get_langchain_prompt.return_value = [
        {"role": "system", "content": "You are helpful."},
        {"role": "human", "content": "{input}"}
    ]
    return prompt


@pytest.fixture
def langfuse_dataset(filepath_json_chat, mock_credentials, mock_langfuse):
    """Basic LangfusePromptDataset for testing."""
    return LangfusePromptDataset(
        filepath=filepath_json_chat,
        prompt_name="test-prompt",
        credentials=mock_credentials,
        mode="langchain"
    )


class TestLangfusePromptDatasetInit:
    """Test LangfusePromptDataset initialization."""

    def test_init_minimal_params(self, filepath_json_chat, mock_credentials, mock_langfuse):
        """Test initialization with minimal required parameters."""
        dataset = LangfusePromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            credentials=mock_credentials
        )
        assert dataset._prompt_name == "test-prompt"
        assert dataset._prompt_type == "text"  # default
        assert dataset._sync_policy == "local"  # default
        assert dataset._mode == "sdk"  # default

    def test_init_all_params(self, filepath_json_chat, mock_credentials, mock_langfuse):
        """Test initialization with all parameters."""
        load_args = {"version": 1}
        save_args = {"labels": ["test"]}

        dataset = LangfusePromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            prompt_type="chat",
            sync_policy="remote",
            mode="sdk",
            load_args=load_args,
            save_args=save_args
        )

        assert dataset._prompt_name == "test-prompt"
        assert dataset._prompt_type == "chat"
        assert dataset._sync_policy == "remote"
        assert dataset._mode == "sdk"
        assert dataset._load_args == load_args
        assert dataset._save_args == save_args

    @pytest.mark.parametrize(
        "missing_key,credentials_dict",
        [
            ("public_key", {"secret_key": "sk_test_67890"}), # pragma: allowlist secret
            ("secret_key", {"public_key": "pk_test_12345"}),
        ]
    )
    def test_init_missing_required_credentials(self, filepath_json_chat, missing_key, credentials_dict, mock_langfuse):
        """Test initialization with missing required credentials raises DatasetError."""
        with pytest.raises(DatasetError, match=f"Missing required Langfuse credential: '{missing_key}'"):
            LangfusePromptDataset(
                filepath=filepath_json_chat,
                prompt_name="test-prompt",
                credentials=credentials_dict
            )

    @pytest.mark.parametrize(
        "invalid_value",
        ["", "   "]  # empty string and whitespace-only
    )
    def test_init_empty_credentials(self, filepath_json_chat, invalid_value, mock_langfuse):
        """Test initialization with empty credential values raises DatasetError."""
        invalid_credentials = {"public_key": invalid_value, "secret_key": "sk_test_67890"} # pragma: allowlist secret

        with pytest.raises(DatasetError, match="Langfuse credential 'public_key' cannot be empty"):
            LangfusePromptDataset(
                filepath=filepath_json_chat,
                prompt_name="test-prompt",
                credentials=invalid_credentials
            )

    def test_init_empty_host(self, filepath_json_chat, mock_langfuse):
        """Test initialization with empty host raises DatasetError."""
        invalid_credentials = {
            "public_key": "pk_test_12345",
            "secret_key": "sk_test_67890",  # pragma: allowlist secret
            "host": ""
        }

        with pytest.raises(DatasetError, match="Langfuse credential 'host' cannot be empty if provided"):
            LangfusePromptDataset(
                filepath=filepath_json_chat,
                prompt_name="test-prompt",
                credentials=invalid_credentials
            )

    def test_init_unsupported_file_extension(self, tmp_path, mock_credentials, mock_langfuse):
        """Test initialization with unsupported file extension raises NotImplementedError."""
        unsupported_file = tmp_path / "prompt.txt"
        unsupported_file.write_text("test prompt")

        with pytest.raises(NotImplementedError, match="Unsupported file extension '.txt'"):
            LangfusePromptDataset(
                filepath=str(unsupported_file),
                prompt_name="test-prompt",
                credentials=mock_credentials
            )


class TestLangfusePromptDatasetSave:
    """Test LangfusePromptDataset save functionality."""

    def test_save_text_prompt(self, filepath_json_text, mock_credentials, mock_langfuse):
        """Test saving a text prompt."""
        dataset = LangfusePromptDataset(
            filepath=filepath_json_text,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            prompt_type="text"
        )

        test_prompt = "This is a test prompt with {variable}"
        dataset.save(test_prompt)

        mock_langfuse.create_prompt.assert_called_once_with(
            name="test-prompt",
            prompt=test_prompt,
            type="text"
        )

    def test_save_chat_prompt(self, filepath_json_chat, mock_credentials, mock_langfuse):
        """Test saving a chat prompt."""
        dataset = LangfusePromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            prompt_type="chat"
        )

        test_prompt = [
            {"role": "system", "content": "You are helpful"},
            {"role": "human", "content": "{input}"}
        ]
        dataset.save(test_prompt)

        mock_langfuse.create_prompt.assert_called_once_with(
            name="test-prompt",
            prompt=test_prompt,
            type="chat"
        )

    def test_save_with_labels(self, filepath_json_chat, mock_credentials, mock_langfuse):
        """Test saving a prompt with labels."""
        save_args = {"labels": ["production", "v2.0"]}
        dataset = LangfusePromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            save_args=save_args
        )

        test_prompt = "Test prompt"
        dataset.save(test_prompt)

        mock_langfuse.create_prompt.assert_called_once_with(
            name="test-prompt",
            prompt=test_prompt,
            type="text",
            labels=["production", "v2.0"]
        )


class TestLangfusePromptDatasetLoad:
    """Test LangfusePromptDataset load functionality."""

    def test_load_sdk_mode(self, langfuse_dataset, mock_langfuse, mock_langfuse_prompt):
        """Test successful load in SDK mode."""
        langfuse_dataset._mode = "sdk"
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        # Reset the mock to ensure clean state
        mock_langfuse.get_prompt.reset_mock()

        result = langfuse_dataset.load()
        assert result == mock_langfuse_prompt
        assert mock_langfuse.get_prompt.call_count == 2

    def test_load_with_version(self, filepath_json_chat, mock_credentials, mock_langfuse, mock_langfuse_prompt):
        """Test load with specific version."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            mode="sdk",
            sync_policy="remote",
            load_args={"version": 3}
        )

        dataset.load()
        call_args = mock_langfuse.get_prompt.call_args[1]
        assert call_args["version"] == 3

    def test_load_with_label(self, filepath_json_chat, mock_credentials, mock_langfuse, mock_langfuse_prompt):
        """Test load with specific label."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            mode="sdk",
            sync_policy="remote",
            load_args={"label": "production"}
        )

        dataset.load()
        call_args = mock_langfuse.get_prompt.call_args[1]
        assert call_args["label"] == "production"

    def test_load_langchain_mode(self, langfuse_dataset, mock_langfuse, mock_langfuse_prompt):
        """Test successful load in LangChain mode."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        # Reset the mock to ensure clean state
        mock_langfuse.get_prompt.reset_mock()

        # Mock the entire langchain.prompts module since import happens inside the method
        mock_chat_template_class = Mock()
        mock_template_instance = Mock()
        mock_chat_template_class.from_messages.return_value = mock_template_instance

        with patch('langchain.prompts.ChatPromptTemplate', mock_chat_template_class):
            result = langfuse_dataset.load()
            assert result == mock_template_instance
            mock_chat_template_class.from_messages.assert_called_once_with(
                mock_langfuse_prompt.get_langchain_prompt()
            )

    def test_load_invalid_mode(self, langfuse_dataset, mock_langfuse, mock_langfuse_prompt):
        """Test load with invalid mode raises DatasetError."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt
        langfuse_dataset._mode = "invalid_mode"

        with pytest.raises(DatasetError, match="Unsupported mode: invalid_mode"):
            langfuse_dataset.load()


class TestLangfusePromptDatasetSyncPolicies:
    """Test different sync policies."""

    def test_sync_local_no_remote(self, filepath_json_chat, mock_credentials, mock_langfuse):
        """Test local sync policy when no remote prompt exists."""
        mock_langfuse.get_prompt.side_effect = Exception("Prompt not found")

        dataset = LangfusePromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            sync_policy="local",
            mode="sdk"
        )

        with patch.object(dataset, 'save') as mock_save:
            mock_langfuse.get_prompt.side_effect = [Exception("Not found"), Mock()]
            dataset.load()
            mock_save.assert_called_once()

    def test_sync_remote_no_remote_fails(self, filepath_json_chat, mock_credentials, mock_langfuse):
        """Test remote sync policy when no remote prompt exists raises DatasetError."""
        mock_langfuse.get_prompt.side_effect = Exception("Prompt not found")

        dataset = LangfusePromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            sync_policy="remote",
            mode="sdk"
        )

        with pytest.raises(DatasetError, match="Remote sync policy specified.*but no remote prompt exists"):
            dataset.load()

    def test_sync_strict_no_local_fails(self, tmp_path, mock_credentials, mock_langfuse, mock_langfuse_prompt):
        """Test strict sync policy when no local file exists raises DatasetError."""
        non_existent_file = (tmp_path / "nonexistent.json").as_posix()
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=non_existent_file,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            sync_policy="strict",
            mode="sdk"
        )

        with pytest.raises(DatasetError, match="Strict sync policy.*Missing.*local file"):
            dataset.load()

    def test_load_args_warning_local_policy(self, filepath_json_chat, mock_credentials, mock_langfuse, mock_langfuse_prompt):
        """Test that load_args produce warning in local sync policy."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            sync_policy="local",
            mode="sdk",
            load_args={"version": 1}
        )

        with patch("kedro_datasets_experimental.langfuse.langfuse_prompt_dataset.logger") as mock_logger:
            # Access _get_build_args to trigger the warning
            _ = dataset._get_build_args
            mock_logger.warning.assert_called()
            warning_message = mock_logger.warning.call_args[0][0]
            assert "Ignoring load_args" in warning_message

class TestLangfusePromptDatasetFileFormats:
    """Test different file format support."""

    @pytest.mark.parametrize(
        "filepath_fixture",
        ["filepath_json_chat", "filepath_yaml_chat", "filepath_json_text", "filepath_yaml_text"]
    )
    def test_file_format_loading(self, request, mock_credentials, mock_langfuse, mock_langfuse_prompt, filepath_fixture):
        """Test loading different file formats."""
        filepath = request.getfixturevalue(filepath_fixture)
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=filepath,
            prompt_name="test-prompt",
            credentials=mock_credentials,
            mode="sdk"
        )

        result = dataset.load()
        assert result == mock_langfuse_prompt


class TestLangfusePromptDatasetUtilityMethods:
    """Test utility methods and functions."""

    def test_describe(self, langfuse_dataset):
        """Test _describe method returns correct information."""
        desc = langfuse_dataset._describe()
        assert desc["prompt_name"] == "test-prompt"
        assert "filepath" in desc

    @pytest.mark.parametrize(
        "filepath_fixture,expected_class",
        [
            ("filepath_json_chat", "JSONDataset"),
            ("filepath_yaml_chat", "YAMLDataset"),
        ]
    )
    def test_file_dataset_property(self, request, mock_credentials, mock_langfuse, filepath_fixture, expected_class):
        """Test file_dataset property returns correct dataset type."""
        filepath = request.getfixturevalue(filepath_fixture)
        dataset = LangfusePromptDataset(
            filepath=filepath,
            prompt_name="test-prompt",
            credentials=mock_credentials
        )

        file_dataset = dataset.file_dataset
        assert file_dataset.__class__.__name__ == expected_class

    def test_file_dataset_caching(self, langfuse_dataset):
        """Test that file_dataset is cached."""
        file_dataset1 = langfuse_dataset.file_dataset
        file_dataset2 = langfuse_dataset.file_dataset
        assert file_dataset1 is file_dataset2

    def test_hash_function(self):
        """Test hashing utility function."""
        content1 = "This is a test prompt"
        content2 = "This is a test prompt"
        content3 = "Different content"

        assert _hash(content1) == _hash(content2)  # Same content, same hash
        assert _hash(content1) != _hash(content3)  # Different content, different hash
        assert len(_hash(content1)) == 64  # SHA-256 produces 64 character hex string

    def test_get_content_function(self):
        """Test content extraction utility function."""
        # String content
        string_content = "This is a test prompt"
        assert _get_content(string_content) == string_content

        # Message list content
        message_content = [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "Hello"}
        ]
        expected = "You are helpful\nHello"
        assert _get_content(message_content) == expected

    def test_preview_existing_file(self, langfuse_dataset):
        """Test preview returns JSON string for existing file."""
        preview = langfuse_dataset.preview()
        # JSONPreview acts like a string, so we can parse it directly
        parsed = json.loads(str(preview))
        assert isinstance(parsed, list)

    def test_preview_nonexistent_file(self, tmp_path, mock_credentials, mock_langfuse):
        """Test preview returns error message for nonexistent file."""
        nonexistent_file = (tmp_path / "nonexistent.json").as_posix()
        dataset = LangfusePromptDataset(
            filepath=nonexistent_file,
            prompt_name="test-prompt",
            credentials=mock_credentials
        )

        preview = dataset.preview()
        assert "Local prompt does not exist" in str(preview)
