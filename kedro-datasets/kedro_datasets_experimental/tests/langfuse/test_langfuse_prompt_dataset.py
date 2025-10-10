import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml
from kedro.io import DatasetError

from kedro_datasets_experimental.langfuse.langfuse_prompt_dataset import (
    LangfusePromptDataset,
)


@pytest.fixture
def mock_langfuse():
    """Mock Langfuse client for testing."""
    with patch("kedro_datasets_experimental.langfuse.langfuse_prompt_dataset.Langfuse") as mock:
        langfuse_instance = Mock()
        mock.return_value = langfuse_instance
        yield langfuse_instance


@pytest.fixture
def valid_credentials():
    """Valid Langfuse credentials for testing."""
    return {
        "public_key": "pk_test_12345",
        "secret_key": "sk_test_67890",  # pragma: allowlist secret
        "host": "https://cloud.langfuse.com"
    }


@pytest.fixture
def minimal_credentials():
    """Minimal valid credentials (no host)."""
    return {
        "public_key": "pk_test_12345",
        "secret_key": "sk_test_67890"  # pragma: allowlist secret
    }


@pytest.fixture
def json_chat_prompt_file(tmp_path: Path) -> Path:
    """Create a temporary JSON file with a chat prompt."""
    prompt_file = tmp_path / "chat_prompt.json"
    chat_data = [
        {
            "type": "chatmessage",
            "role": "system",
            "content": "You are a helpful insurance support assistant."
        },
        {
            "type": "chatmessage",
            "role": "human",
            "content": "{input}"
        }
    ]
    prompt_file.write_text(json.dumps(chat_data, indent=2))
    return prompt_file


@pytest.fixture
def yaml_chat_prompt_file(tmp_path: Path) -> Path:
    """Create a temporary YAML file with a chat prompt."""
    prompt_file = tmp_path / "chat_prompt.yaml"
    chat_data = [
        {
            "type": "chatmessage",
            "role": "system",
            "content": "You are a helpful insurance support assistant."
        },
        {
            "type": "chatmessage",
            "role": "human",
            "content": "{input}"
        }
    ]
    prompt_file.write_text(yaml.dump(chat_data))
    return prompt_file


@pytest.fixture
def json_text_prompt_file(tmp_path: Path) -> Path:
    """Create a temporary JSON file with a text prompt."""
    prompt_file = tmp_path / "text_prompt.json"
    text_data = "Classify the following user query: {input}"
    prompt_file.write_text(json.dumps(text_data))
    return prompt_file


@pytest.fixture
def yaml_text_prompt_file(tmp_path: Path) -> Path:
    """Create a temporary YAML file with a text prompt."""
    prompt_file = tmp_path / "text_prompt.yaml"
    text_data = {"prompt": "Classify the following user query: {input}"}
    prompt_file.write_text(yaml.dump(text_data))
    return prompt_file


@pytest.fixture
def mock_langfuse_prompt():
    """Mock Langfuse prompt object."""
    prompt = Mock()
    prompt.prompt = [
        {
            "type": "chatmessage",
            "role": "system",
            "content": "You are helpful."
        },
        {
            "type": "chatmessage",
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


class TestLangfusePromptDatasetInit:
    """Test LangfusePromptDataset initialization."""

    def test_init_with_minimal_params(self, json_chat_prompt_file: Path, minimal_credentials: dict, mock_langfuse):
        """Test initialization with minimal required parameters."""
        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=minimal_credentials
        )
        assert dataset._prompt_name == "test-prompt"
        assert dataset._prompt_type == "text"  # default
        assert dataset._sync_policy == "local"  # default
        assert dataset._mode == "langchain"  # default

    def test_init_with_all_params(self, json_chat_prompt_file: Path, valid_credentials: dict, mock_langfuse):
        """Test initialization with all parameters."""
        load_args = {"version": 1}
        save_args = {"labels": ["test"]}

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
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

    def test_init_missing_public_key(self, json_chat_prompt_file: Path):
        """Test initialization with missing public_key raises DatasetError."""
        invalid_credentials = {"secret_key": "sk_test_67890"}  # pragma: allowlist secret

        with pytest.raises(DatasetError, match="Missing required Langfuse credential: 'public_key'"):
            LangfusePromptDataset(
                filepath=str(json_chat_prompt_file),
                prompt_name="test-prompt",
                credentials=invalid_credentials
            )

    def test_init_missing_secret_key(self, json_chat_prompt_file: Path):
        """Test initialization with missing secret_key raises DatasetError."""
        invalid_credentials = {"public_key": "pk_test_12345"}

        with pytest.raises(DatasetError, match="Missing required Langfuse credential: 'secret_key'"):
            LangfusePromptDataset(
                filepath=str(json_chat_prompt_file),
                prompt_name="test-prompt",
                credentials=invalid_credentials
            )

    def test_init_empty_credentials(self, json_chat_prompt_file: Path):
        """Test initialization with empty credential values raises DatasetError."""
        invalid_credentials = {"public_key": "", "secret_key": "sk_test_67890"}  # pragma: allowlist secret

        with pytest.raises(DatasetError, match="Langfuse credential 'public_key' cannot be empty"):
            LangfusePromptDataset(
                filepath=str(json_chat_prompt_file),
                prompt_name="test-prompt",
                credentials=invalid_credentials
            )

    def test_init_empty_host(self, json_chat_prompt_file: Path):
        """Test initialization with empty host raises DatasetError."""
        invalid_credentials = {
            "public_key": "pk_test_12345",
            "secret_key": "sk_test_67890",  # pragma: allowlist secret
            "host": ""
        }

        with pytest.raises(DatasetError, match="Langfuse credential 'host' cannot be empty if provided"):
            LangfusePromptDataset(
                filepath=str(json_chat_prompt_file),
                prompt_name="test-prompt",
                credentials=invalid_credentials
            )

    def test_init_unsupported_file_extension(self, tmp_path: Path, valid_credentials: dict):
        """Test initialization with unsupported file extension raises NotImplementedError."""
        unsupported_file = tmp_path / "prompt.txt"
        unsupported_file.write_text("test prompt")

        with pytest.raises(NotImplementedError, match="Unsupported file extension '.txt'"):
            LangfusePromptDataset(
                filepath=str(unsupported_file),
                prompt_name="test-prompt",
                credentials=valid_credentials
            )


class TestLangfusePromptDatasetSave:
    """Test LangfusePromptDataset save functionality."""

    def test_save_text_prompt(self, json_text_prompt_file: Path, valid_credentials: dict, mock_langfuse):
        """Test saving a text prompt."""
        dataset = LangfusePromptDataset(
            filepath=str(json_text_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            prompt_type="text"
        )

        test_prompt = "This is a test prompt with {variable}"
        dataset.save(test_prompt)

        mock_langfuse.create_prompt.assert_called_once_with(
            name="test-prompt",
            prompt=test_prompt,
            type="text"
        )

    def test_save_chat_prompt(self, json_chat_prompt_file: Path, valid_credentials: dict, mock_langfuse):
        """Test saving a chat prompt."""
        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            prompt_type="chat"
        )

        test_prompt = [
            {"type": "chatmessage", "role": "system", "content": "You are helpful"},
            {"type": "chatmessage", "role": "human", "content": "{input}"}
        ]
        dataset.save(test_prompt)

        mock_langfuse.create_prompt.assert_called_once_with(
            name="test-prompt",
            prompt=test_prompt,
            type="chat"
        )

    def test_save_with_labels(self, json_chat_prompt_file: Path, valid_credentials: dict, mock_langfuse):
        """Test saving a prompt with labels."""
        save_args = {"labels": ["production", "v2.0"]}
        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
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


class TestLangfusePromptDatasetLoadSDKMode:
    """Test LangfusePromptDataset load functionality in SDK mode."""

    def test_load_sdk_mode_success(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                   mock_langfuse, mock_langfuse_prompt):
        """Test successful load in SDK mode."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            mode="sdk"
        )

        result = dataset.load()
        assert result == mock_langfuse_prompt
        mock_langfuse.get_prompt.assert_called_once()

    def test_load_sdk_mode_with_version(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                        mock_langfuse, mock_langfuse_prompt):
        """Test load in SDK mode with specific version."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            mode="sdk",
            sync_policy="remote",
            load_args={"version": 3}
        )

        result = dataset.load()
        assert result == mock_langfuse_prompt

        # Check that version was passed to get_prompt
        call_args = mock_langfuse.get_prompt.call_args[1]
        assert call_args["version"] == 3

    def test_load_sdk_mode_with_label(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                      mock_langfuse, mock_langfuse_prompt):
        """Test load in SDK mode with specific label."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            mode="sdk",
            sync_policy="remote",
            load_args={"label": "production"}
        )

        result = dataset.load()
        assert result == mock_langfuse_prompt

        # Check that label was passed to get_prompt
        call_args = mock_langfuse.get_prompt.call_args[1]
        assert call_args["label"] == "production"


class TestLangfusePromptDatasetLoadLangChainMode:
    """Test LangfusePromptDataset load functionality in LangChain mode."""

    @patch("kedro_datasets_experimental.langfuse.langfuse_prompt_dataset.ChatPromptTemplate")
    def test_load_langchain_mode_success(self, mock_chat_template, json_chat_prompt_file: Path,
                                         valid_credentials: dict, mock_langfuse, mock_langfuse_prompt):
        """Test successful load in LangChain mode."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt
        mock_template_instance = Mock()
        mock_chat_template.from_messages.return_value = mock_template_instance

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            mode="langchain"
        )

        result = dataset.load()
        assert result == mock_template_instance
        mock_chat_template.from_messages.assert_called_once_with(
            mock_langfuse_prompt.get_langchain_prompt()
        )

    def test_load_langchain_mode_missing_langchain(self, json_chat_prompt_file: Path,
                                                   valid_credentials: dict, mock_langfuse,
                                                   mock_langfuse_prompt):
        """Test load in LangChain mode raises error when langchain is not installed."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        with patch("kedro_datasets_experimental.langfuse.langfuse_prompt_dataset.ChatPromptTemplate",
                   side_effect=ImportError("No module named 'langchain'")):
            dataset = LangfusePromptDataset(
                filepath=str(json_chat_prompt_file),
                prompt_name="test-prompt",
                credentials=valid_credentials,
                mode="langchain"
            )

            with pytest.raises(ImportError, match="The 'langchain' package is required when using mode='langchain'"):
                dataset.load()

    def test_load_invalid_mode(self, json_chat_prompt_file: Path, valid_credentials: dict,
                               mock_langfuse, mock_langfuse_prompt):
        """Test load with invalid mode raises DatasetError."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            mode="invalid_mode"
        )

        with pytest.raises(DatasetError, match="Unsupported mode: invalid_mode"):
            dataset.load()


class TestLangfusePromptDatasetSyncPolicies:
    """Test different sync policies."""

    def test_sync_policy_local_no_remote(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                         mock_langfuse):
        """Test local sync policy when no remote prompt exists."""
        # Mock no remote prompt found
        mock_langfuse.get_prompt.side_effect = Exception("Prompt not found")

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            sync_policy="local",
            mode="sdk"
        )

        # This should create the prompt in Langfuse and then fetch it
        with patch.object(dataset, 'save') as mock_save:
            mock_langfuse.get_prompt.side_effect = [Exception("Not found"), Mock()]
            dataset.load()
            mock_save.assert_called_once()

    def test_sync_policy_remote_no_remote(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                          mock_langfuse):
        """Test remote sync policy when no remote prompt exists raises DatasetError."""
        mock_langfuse.get_prompt.side_effect = Exception("Prompt not found")

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            sync_policy="remote",
            mode="sdk"
        )

        with pytest.raises(DatasetError, match="Remote sync policy specified.*but no remote prompt exists"):
            dataset.load()

    def test_sync_policy_strict_no_local(self, tmp_path: Path, valid_credentials: dict,
                                         mock_langfuse, mock_langfuse_prompt):
        """Test strict sync policy when no local file exists raises DatasetError."""
        non_existent_file = tmp_path / "nonexistent.json"
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=str(non_existent_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            sync_policy="strict",
            mode="sdk"
        )

        with pytest.raises(DatasetError, match="Strict sync policy.*Missing.*local file"):
            dataset.load()

    def test_sync_policy_strict_no_remote(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                          mock_langfuse):
        """Test strict sync policy when no remote prompt exists raises DatasetError."""
        mock_langfuse.get_prompt.side_effect = Exception("Prompt not found")

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            sync_policy="strict",
            mode="sdk"
        )

        with pytest.raises(DatasetError, match="Strict sync policy.*Missing.*remote prompt"):
            dataset.load()

    def test_load_args_warning_local_policy(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                            mock_langfuse, mock_langfuse_prompt):
        """Test that load_args produce warning in local sync policy."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            sync_policy="local",
            mode="sdk",
            load_args={"version": 1}
        )

        with patch("kedro_datasets_experimental.langfuse.langfuse_prompt_dataset.logger") as mock_logger:
            dataset.load()
            mock_logger.warning.assert_called()
            warning_message = mock_logger.warning.call_args[0][0]
            assert "Ignoring load_args" in warning_message


class TestLangfusePromptDatasetNetworkHandling:
    """Test network error handling."""

    def test_network_error_fallback(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                    mock_langfuse):
        """Test that network errors fall back to local file gracefully."""
        mock_langfuse.get_prompt.side_effect = ConnectionError("Network error")

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            sync_policy="local",
            mode="sdk"
        )

        with patch("kedro_datasets_experimental.langfuse.langfuse_prompt_dataset.logger") as mock_logger:
            # This should create and then load the prompt locally
            with patch.object(dataset, 'save') as mock_save:
                mock_langfuse.get_prompt.side_effect = [ConnectionError("Network error"), Mock()]
                dataset.load()
                mock_logger.warning.assert_called()
                warning_message = mock_logger.warning.call_args[0][0]
                assert "Network error" in warning_message

    def test_timeout_error_fallback(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                    mock_langfuse):
        """Test that timeout errors fall back to local file gracefully."""
        mock_langfuse.get_prompt.side_effect = TimeoutError("Request timeout")

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            sync_policy="local",
            mode="sdk"
        )

        with patch("kedro_datasets_experimental.langfuse.langfuse_prompt_dataset.logger") as mock_logger:
            with patch.object(dataset, 'save') as mock_save:
                mock_langfuse.get_prompt.side_effect = [TimeoutError("Request timeout"), Mock()]
                dataset.load()
                mock_logger.warning.assert_called()
                warning_message = mock_logger.warning.call_args[0][0]
                assert "Network error" in warning_message


class TestLangfusePromptDatasetFileFormats:
    """Test different file format support."""

    def test_json_chat_prompt_loading(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                      mock_langfuse, mock_langfuse_prompt):
        """Test loading JSON chat prompt file."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            mode="sdk"
        )

        result = dataset.load()
        assert result == mock_langfuse_prompt

    def test_yaml_chat_prompt_loading(self, yaml_chat_prompt_file: Path, valid_credentials: dict,
                                      mock_langfuse, mock_langfuse_prompt):
        """Test loading YAML chat prompt file."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=str(yaml_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            mode="sdk"
        )

        result = dataset.load()
        assert result == mock_langfuse_prompt

    def test_json_text_prompt_loading(self, json_text_prompt_file: Path, valid_credentials: dict,
                                      mock_langfuse, mock_langfuse_prompt):
        """Test loading JSON text prompt file."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=str(json_text_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            mode="sdk"
        )

        result = dataset.load()
        assert result == mock_langfuse_prompt

    def test_yaml_text_prompt_loading(self, yaml_text_prompt_file: Path, valid_credentials: dict,
                                      mock_langfuse, mock_langfuse_prompt):
        """Test loading YAML text prompt file."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        dataset = LangfusePromptDataset(
            filepath=str(yaml_text_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            mode="sdk"
        )

        result = dataset.load()
        assert result == mock_langfuse_prompt


class TestLangfusePromptDatasetUtilityMethods:
    """Test utility methods."""

    def test_describe(self, json_chat_prompt_file: Path, valid_credentials: dict, mock_langfuse):
        """Test _describe method returns correct information."""
        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials
        )

        desc = dataset._describe()
        assert desc["filepath"] == json_chat_prompt_file
        assert desc["prompt_name"] == "test-prompt"

    def test_file_dataset_json(self, json_chat_prompt_file: Path, valid_credentials: dict, mock_langfuse):
        """Test file_dataset property returns JSONDataset for .json files."""
        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials
        )

        file_dataset = dataset.file_dataset
        assert file_dataset.__class__.__name__ == "JSONDataset"

    def test_file_dataset_yaml(self, yaml_chat_prompt_file: Path, valid_credentials: dict, mock_langfuse):
        """Test file_dataset property returns YAMLDataset for .yaml files."""
        dataset = LangfusePromptDataset(
            filepath=str(yaml_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials
        )

        file_dataset = dataset.file_dataset
        assert file_dataset.__class__.__name__ == "YAMLDataset"

    def test_file_dataset_caching(self, json_chat_prompt_file: Path, valid_credentials: dict, mock_langfuse):
        """Test that file_dataset is cached."""
        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials
        )

        file_dataset1 = dataset.file_dataset
        file_dataset2 = dataset.file_dataset
        assert file_dataset1 is file_dataset2


class TestLangfusePromptDatasetEdgeCases:
    """Test edge cases and error scenarios."""

    def test_no_prompt_found_anywhere(self, tmp_path: Path, valid_credentials: dict, mock_langfuse):
        """Test DatasetError when no prompt found locally or remotely."""
        non_existent_file = tmp_path / "nonexistent.json"
        mock_langfuse.get_prompt.side_effect = Exception("Prompt not found")

        dataset = LangfusePromptDataset(
            filepath=str(non_existent_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            sync_policy="local",
            mode="sdk"
        )

        with pytest.raises(DatasetError, match="No prompt found locally or in Langfuse"):
            dataset.load()

    def test_message_type_adaptation(self, mock_langfuse, valid_credentials: dict, tmp_path: Path):
        """Test that Langfuse message types are adapted to local storage conventions."""
        # Create a file for testing
        test_file = tmp_path / "test.json"
        test_file.write_text("[]")

        # Mock Langfuse prompt with 'message' type
        mock_prompt = Mock()
        mock_prompt.prompt = [
            {"type": "message", "role": "system", "content": "Test"}
        ]
        mock_langfuse.get_prompt.return_value = mock_prompt

        dataset = LangfusePromptDataset(
            filepath=str(test_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            sync_policy="remote",
            mode="sdk",
            prompt_type="chat"
        )

        with patch.object(dataset, 'file_dataset') as mock_file_dataset:
            result = dataset.load()

            # Check that save was called with adapted message type
            save_call_args = mock_file_dataset.save.call_args[0][0]
            assert save_call_args[0]["type"] == "chatmessage"  # Adapted from "message"

    @pytest.mark.parametrize("policy", ["local", "remote", "strict"])
    def test_all_sync_policies_with_valid_setup(self, json_chat_prompt_file: Path, valid_credentials: dict,
                                                mock_langfuse, mock_langfuse_prompt, policy: str):
        """Test all sync policies work with proper setup."""
        mock_langfuse.get_prompt.return_value = mock_langfuse_prompt

        load_args = {} if policy == "local" else {"label": "latest"}

        dataset = LangfusePromptDataset(
            filepath=str(json_chat_prompt_file),
            prompt_name="test-prompt",
            credentials=valid_credentials,
            sync_policy=policy,
            mode="sdk",
            load_args=load_args
        )

        result = dataset.load()
        assert result == mock_langfuse_prompt
