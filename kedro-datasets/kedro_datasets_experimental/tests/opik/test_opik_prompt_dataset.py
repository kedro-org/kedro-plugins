import json
from unittest.mock import Mock, patch

import pytest
import yaml
from kedro.io import DatasetError

from kedro_datasets_experimental.opik.opik_prompt_dataset import (
    OpikPromptDataset,
    _get_content,
    _hash,
)


@pytest.fixture
def mock_opik():
    """Mock Opik client for testing."""
    with patch("kedro_datasets_experimental.opik.opik_prompt_dataset.Opik") as mock:
        opik_instance = Mock()
        mock.return_value = opik_instance
        yield opik_instance


@pytest.fixture
def mock_credentials():
    """Valid Opik credentials for testing."""
    return {
        "api_key": "opik_test_12345", # pragma: allowlist secret
        "workspace": "test-workspace",
        "project_name": "test-project"
    }


@pytest.fixture
def filepath_json_chat(tmp_path):
    """Create a temporary JSON file with a chat prompt."""
    prompt_file = tmp_path / "chat_prompt.json"
    chat_data = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello, {question}"}
    ]
    prompt_file.write_text(json.dumps(chat_data, indent=2))
    return str(prompt_file)


@pytest.fixture
def filepath_yaml_chat(tmp_path):
    """Create a temporary YAML file with a chat prompt."""
    prompt_file = tmp_path / "chat_prompt.yaml"
    chat_data = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello, {question}"}
    ]
    prompt_file.write_text(yaml.dump(chat_data))
    return str(prompt_file)


@pytest.fixture
def filepath_json_text(tmp_path):
    """Create a temporary JSON file with a text prompt."""
    prompt_file = tmp_path / "text_prompt.json"
    text_data = "Answer the question: {question}"
    prompt_file.write_text(json.dumps(text_data))
    return str(prompt_file)


@pytest.fixture
def filepath_yaml_text(tmp_path):
    """Create a temporary YAML file with a text prompt."""
    prompt_file = tmp_path / "text_prompt.yaml"
    text_data = "Answer the question: {question}"
    prompt_file.write_text(yaml.dump(text_data))
    return str(prompt_file)


@pytest.fixture
def mock_opik_prompt():
    """Mock Opik prompt object."""
    prompt = Mock()
    prompt.name = "test-prompt"
    prompt.prompt = [
        {"role": "system", "content": "You are helpful."},
        {"role": "user", "content": "{input}"}
    ]
    prompt.metadata = {"type": "chat", "version": 1}
    return prompt


@pytest.fixture
def mock_opik_dataset():
    """Mock Opik dataset object."""
    dataset = Mock()
    dataset.name = "prompts-test-prompt"
    return dataset


@pytest.fixture
def opik_dataset(filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset):
    """Basic OpikPromptDataset for testing."""
    return OpikPromptDataset(
        filepath=filepath_json_chat,
        prompt_name="test-prompt",
        prompt_type="chat",
        mode="langchain",
        credentials=mock_credentials
    )


class TestOpikPromptDatasetInit:
    """Test OpikPromptDataset initialisation."""

    def test_init_minimal_params(self, filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset):
        """Test initialisation with minimal required parameters."""

        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            credentials=mock_credentials
        )

        assert dataset._prompt_name == "test-prompt"
        assert dataset._prompt_type == "chat"
        assert dataset._sync_policy == "local"  # default
        assert dataset._mode == "sdk"  # default

    def test_init_all_params(self, filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset):
        """Test initialisation with all parameters."""

        load_args = {"version": 1}  # For future use
        save_args = {"metadata": {"environment": "test"}}

        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            sync_policy="remote",
            mode="langchain",
            credentials=mock_credentials,
            load_args=load_args,
            save_args=save_args
        )

        assert dataset._prompt_name == "test-prompt"
        assert dataset._prompt_type == "chat"
        assert dataset._sync_policy == "remote"
        assert dataset._mode == "langchain"
        assert dataset._load_args == load_args
        assert dataset._save_args == save_args

    def test_init_invalid_prompt_type(self, filepath_json_chat, mock_opik):
        """Test initialisation with invalid prompt type raises DatasetError."""
        with pytest.raises(DatasetError, match="Invalid prompt_type 'invalid'"):
            OpikPromptDataset(
                filepath=filepath_json_chat,
                prompt_name="test-prompt",
                prompt_type="invalid",
                credentials=mock_credentials
            )

    def test_init_invalid_sync_policy(self, filepath_json_chat, mock_opik):
        """Test initialisation with invalid sync policy raises DatasetError."""
        with pytest.raises(DatasetError, match="Invalid sync_policy 'invalid'"):
            OpikPromptDataset(
                filepath=filepath_json_chat,
                prompt_name="test-prompt",
                prompt_type="chat",
                sync_policy="invalid",
                credentials=mock_credentials
            )

    def test_init_invalid_mode(self, filepath_json_chat, mock_opik):
        """Test initialisation with invalid mode raises DatasetError."""
        with pytest.raises(DatasetError, match="Invalid mode 'invalid'"):
            OpikPromptDataset(
                filepath=filepath_json_chat,
                prompt_name="test-prompt",
                prompt_type="chat",
                mode="invalid",
                credentials=mock_credentials
            )

    def test_init_unsupported_file_extension(self, tmp_path, mock_opik):
        """Test initialisation with unsupported file extension raises NotImplementedError."""
        unsupported_file = tmp_path / "prompt.txt"
        unsupported_file.write_text("test prompt")

        with pytest.raises(NotImplementedError, match="Unsupported file extension '.txt'"):
            OpikPromptDataset(
                filepath=str(unsupported_file),
                prompt_name="test-prompt",
                prompt_type="text",
                credentials=mock_credentials
            )

    def test_init_opik_client_failure(self, filepath_json_chat, mock_credentials):
        """Test initialisation handles Opik client creation failure."""
        with patch("kedro_datasets_experimental.opik.opik_prompt_dataset.Opik") as mock_opik_class:
            mock_opik_class.side_effect = Exception("Connection failed")

            with pytest.raises(DatasetError, match="Failed to initialise Opik client"):
                OpikPromptDataset(
                    filepath=filepath_json_chat,
                    prompt_name="test-prompt",
                    prompt_type="chat",
                    credentials=mock_credentials
                )

    def test_init_langchain_mode_without_package(self, filepath_json_chat, mock_opik, mock_opik_dataset):
        """Test initialisation with langchain mode when package not installed."""

        with patch("kedro_datasets_experimental.opik.opik_prompt_dataset.TYPE_CHECKING", False):
            with patch.dict("sys.modules", {"langchain_core.prompts": None}):
                with pytest.raises(ImportError, match="'langchain-core' package is required"):
                    OpikPromptDataset(
                        filepath=filepath_json_chat,
                        prompt_name="test-prompt",
                        prompt_type="chat",
                        mode="langchain",
                        credentials=mock_credentials
                    )


class TestOpikPromptDatasetSave:
    """Test OpikPromptDataset save functionality."""

    def test_save_text_prompt(self, filepath_json_text, mock_credentials, mock_opik, mock_opik_dataset):
        """Test saving a text prompt."""

        dataset = OpikPromptDataset(
            filepath=filepath_json_text,
            prompt_name="test-prompt",
            prompt_type="text",
            credentials=mock_credentials
        )

        test_prompt = "Answer the question: {question}"
        dataset.save(test_prompt)

        mock_opik.create_prompt.assert_called_once()
        call_kwargs = mock_opik.create_prompt.call_args[1]
        assert call_kwargs["name"] == "test-prompt"
        assert call_kwargs["prompt"] == test_prompt
        assert call_kwargs["metadata"]["type"] == "text"

    def test_save_chat_prompt(self, filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset):
        """Test saving a chat prompt."""

        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            credentials=mock_credentials
        )

        test_prompt = [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "{input}"}
        ]
        dataset.save(test_prompt)

        mock_opik.create_prompt.assert_called_once()
        call_kwargs = mock_opik.create_prompt.call_args[1]
        assert call_kwargs["name"] == "test-prompt"
        assert json.loads(call_kwargs["prompt"]) == test_prompt
        assert call_kwargs["metadata"]["type"] == "chat"

    def test_save_with_metadata(self, filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset):
        """Test saving a prompt with additional metadata."""

        save_args = {"metadata": {"environment": "production", "version": "2.0"}}
        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            credentials=mock_credentials,
            save_args=save_args
        )

        test_prompt = [{"role": "user", "content": "Test"}]
        dataset.save(test_prompt)

        call_kwargs = mock_opik.create_prompt.call_args[1]
        assert call_kwargs["metadata"]["type"] == "chat"
        assert call_kwargs["metadata"]["environment"] == "production"
        assert call_kwargs["metadata"]["version"] == "2.0"

    def test_save_invalid_chat_format(self, opik_dataset):
        """Test saving invalid chat format raises DatasetError."""
        with pytest.raises(DatasetError, match="Chat prompts must be a list"):
            opik_dataset.save("This should be a list")

    def test_save_invalid_text_format(self, filepath_json_text, mock_credentials, mock_opik, mock_opik_dataset):
        """Test saving invalid text format raises DatasetError."""

        dataset = OpikPromptDataset(
            filepath=filepath_json_text,
            prompt_name="test-prompt",
            prompt_type="text",
            credentials=mock_credentials
        )

        with pytest.raises(DatasetError, match="Text prompts must be a string"):
            dataset.save(["This should be a string"])


class TestOpikPromptDatasetLoad:
    """Test OpikPromptDataset load functionality."""

    def test_load_sdk_mode(self, opik_dataset, mock_opik, mock_opik_prompt):
        """Test successful load in SDK mode."""
        opik_dataset._mode = "sdk"
        mock_opik.get_prompt.return_value = mock_opik_prompt
        mock_opik.get_prompt.reset_mock()

        result = opik_dataset.load()

        assert result == mock_opik_prompt
        assert mock_opik.get_prompt.call_count >= 1

    def test_load_langchain_mode_chat(self, opik_dataset, mock_opik, mock_opik_prompt):
        """Test successful load in LangChain mode with chat prompt."""
        mock_opik.get_prompt.return_value = mock_opik_prompt
        mock_opik.get_prompt.reset_mock()

        mock_template = Mock()
        with patch("langchain_core.prompts.ChatPromptTemplate") as mock_chat_template:
            mock_chat_template.from_messages.return_value = mock_template

            result = opik_dataset.load()

            assert result == mock_template
            mock_chat_template.from_messages.assert_called_once()

    def test_load_langchain_mode_text(self, filepath_json_text, mock_credentials, mock_opik, mock_opik_dataset):
        """Test successful load in LangChain mode with text prompt."""

        mock_text_prompt = Mock()
        mock_text_prompt.prompt = "Answer the question: {question}"
        mock_opik.get_prompt.return_value = mock_text_prompt

        dataset = OpikPromptDataset(
            filepath=filepath_json_text,
            prompt_name="test-prompt",
            prompt_type="text",
            mode="langchain",
            credentials=mock_credentials
        )

        mock_template = Mock()
        with patch("langchain_core.prompts.ChatPromptTemplate") as mock_chat_template:
            mock_chat_template.from_template.return_value = mock_template

            result = dataset.load()

            assert result == mock_template
            mock_chat_template.from_template.assert_called_once_with("Answer the question: {question}")

    def test_load_args_warning_local_policy(self, filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset,
                                            mock_opik_prompt):
        """Test that load_args produce warning in local sync policy."""
        mock_opik.get_prompt.return_value = mock_opik_prompt

        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            sync_policy="local",
            mode="sdk",
            credentials=mock_credentials,
            load_args={"version": 1}
        )

        with patch("kedro_datasets_experimental.opik.opik_prompt_dataset.logger") as mock_logger:
            dataset.load()

            # Check that at least one warning contains "Ignoring load_args"
            warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
            assert any("Ignoring load_args" in msg for msg in warning_calls), \
                f"Expected 'Ignoring load_args' warning, but got: {warning_calls}"


class TestOpikPromptDatasetSyncPolicies:
    """Test different sync policies."""

    def test_sync_local_no_remote(self, filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset):
        """Test local sync policy when no remote prompt exists."""
        mock_opik.get_prompt.side_effect = Exception("Prompt not found")

        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            sync_policy="local",
            mode="sdk",
            credentials=mock_credentials
        )

        with patch.object(dataset, 'save') as mock_save:
            mock_opik.get_prompt.side_effect = [Exception("Not found"), Mock()]
            dataset.load()
            mock_save.assert_called_once()

    def test_sync_remote_no_remote_fails(self, filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset):
        """Test remote sync policy when no remote prompt exists raises DatasetError."""
        mock_opik.get_prompt.return_value = None

        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            sync_policy="remote",
            mode="sdk",
            credentials=mock_credentials
        )

        with pytest.raises(DatasetError, match="Remote sync policy specified.*but no remote prompt exists"):
            dataset.load()

    def test_sync_strict_no_local_fails(self, tmp_path, mock_credentials, mock_opik, mock_opik_dataset, mock_opik_prompt):
        """Test strict sync policy when no local file exists raises DatasetError."""
        non_existent_file = (tmp_path / "nonexistent.json").as_posix()
        mock_opik.get_prompt.return_value = mock_opik_prompt

        dataset = OpikPromptDataset(
            filepath=non_existent_file,
            prompt_name="test-prompt",
            prompt_type="chat",
            sync_policy="strict",
            mode="sdk",
            credentials=mock_credentials
        )

        with pytest.raises(DatasetError, match="Strict sync policy.*Missing.*local file"):
            dataset.load()

    def test_sync_strict_mismatch_fails(self, filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset):
        """Test strict sync policy when local and remote differ raises DatasetError."""

        # Create different remote prompt
        mock_remote_prompt = Mock()
        mock_remote_prompt.prompt = [
            {"role": "system", "content": "Different content"},
            {"role": "user", "content": "{input}"}
        ]
        mock_opik.get_prompt.return_value = mock_remote_prompt

        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            sync_policy="strict",
            mode="sdk",
            credentials=mock_credentials
        )

        with pytest.raises(DatasetError, match="Strict sync failed.*differ"):
            dataset.load()

    def test_sync_remote_updates_local(self, filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset):
        """Test remote sync policy updates local file when different."""

        # Remote has different content
        mock_remote_prompt = Mock()
        mock_remote_prompt.prompt = [
            {"role": "system", "content": "Remote version"},
            {"role": "user", "content": "{input}"}
        ]
        mock_opik.get_prompt.return_value = mock_remote_prompt

        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            sync_policy="remote",
            mode="sdk",
            credentials=mock_credentials
        )

        with patch.object(dataset.file_dataset, 'save') as mock_save:
            dataset.load()
            mock_save.assert_called_once()


class TestOpikPromptDatasetFileFormats:
    """Test different file format support."""

    @pytest.mark.parametrize(
        "filepath_fixture,prompt_type",
        [
            ("filepath_json_chat", "chat"),
            ("filepath_yaml_chat", "chat"),
            ("filepath_json_text", "text"),
            ("filepath_yaml_text", "text")
        ]
    )
    def test_file_format_loading(self, request, mock_credentials, mock_opik, mock_opik_dataset, mock_opik_prompt, filepath_fixture, prompt_type):
        """Test loading different file formats."""
        filepath = request.getfixturevalue(filepath_fixture)
        mock_opik.get_prompt.return_value = mock_opik_prompt

        dataset = OpikPromptDataset(
            filepath=filepath,
            prompt_name="test-prompt",
            prompt_type=prompt_type,
            mode="sdk",
            credentials=mock_credentials
        )

        result = dataset.load()
        assert result == mock_opik_prompt


class TestOpikPromptDatasetUtilityMethods:
    """Test utility methods and functions."""

    def test_describe(self, opik_dataset):
        """Test _describe method returns correct information."""
        desc = opik_dataset._describe()
        assert desc["prompt_name"] == "test-prompt"
        assert desc["prompt_type"] == "chat"
        assert desc["sync_policy"] == "local"
        assert desc["mode"] == "langchain"
        assert "filepath" in desc

    @pytest.mark.parametrize(
        "filepath_fixture,expected_class",
        [
            ("filepath_json_chat", "JSONDataset"),
            ("filepath_yaml_chat", "YAMLDataset"),
        ]
    )
    def test_file_dataset_property(self, request, mock_credentials, mock_opik, mock_opik_dataset, filepath_fixture, expected_class):
        """Test file_dataset property returns correct dataset type."""
        filepath = request.getfixturevalue(filepath_fixture)

        dataset = OpikPromptDataset(
            filepath=filepath,
            prompt_name="test-prompt",
            prompt_type="chat",
            credentials=mock_credentials
        )

        file_dataset = dataset.file_dataset
        assert file_dataset.__class__.__name__ == expected_class

    def test_file_dataset_caching(self, opik_dataset):
        """Test that file_dataset is cached."""
        file_dataset1 = opik_dataset.file_dataset
        file_dataset2 = opik_dataset.file_dataset
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
            {"content": "You are helpful"},
            {"content": "Hello"}
        ]
        expected = "You are helpful\nHello"
        assert _get_content(message_content) == expected

    def test_preview_existing_file(self, opik_dataset):
        """Test preview returns JSON string for existing file."""
        preview = opik_dataset.preview()
        parsed = json.loads(str(preview))
        assert isinstance(parsed, list)

    def test_preview_nonexistent_file(self, tmp_path, mock_credentials, mock_opik, mock_opik_dataset):
        """Test preview returns error message for nonexistent file."""
        nonexistent_file = (tmp_path / "nonexistent.json").as_posix()

        dataset = OpikPromptDataset(
            filepath=nonexistent_file,
            prompt_name="test-prompt",
            prompt_type="chat",
            credentials=mock_credentials
        )

        preview = dataset.preview()
        assert "Local prompt file does not exist" in str(preview)

    def test_ensure_dataset_exists_creates_new(self, filepath_json_chat, mock_credentials, mock_opik):
        """Test that _ensure_dataset_exists creates dataset if it doesn't exist."""
        mock_opik.get_dataset.side_effect = Exception("Dataset not found")
        mock_new_dataset = Mock()
        mock_opik.create_dataset.return_value = mock_new_dataset

        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            credentials=mock_credentials
        )

        mock_opik.create_dataset.assert_called_once_with(
            name="prompts-test-prompt",
            description="Prompt versions for test-prompt"
        )
        assert dataset._dataset == mock_new_dataset

    def test_ensure_dataset_exists_uses_existing(self, filepath_json_chat, mock_credentials, mock_opik, mock_opik_dataset):
        """Test that _ensure_dataset_exists uses existing dataset if available."""
        mock_opik.get_dataset.return_value = mock_opik_dataset

        dataset = OpikPromptDataset(
            filepath=filepath_json_chat,
            prompt_name="test-prompt",
            prompt_type="chat",
            credentials=mock_credentials
        )

        mock_opik.get_dataset.assert_called_once_with(name="prompts-test-prompt")
        mock_opik.create_dataset.assert_not_called()
        assert dataset._dataset == mock_opik_dataset

    def test_convert_to_langchain_template_chat(self, opik_dataset):
        """Test conversion of chat messages to ChatPromptTemplate."""
        chat_data = [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "{input}"}
        ]

        with patch("langchain_core.prompts.ChatPromptTemplate") as mock_template:
            mock_template.from_messages.return_value = Mock()
            opik_dataset._convert_to_langchain_template(chat_data)

            mock_template.from_messages.assert_called_once_with([
                ("system", "You are helpful"),
                ("user", "{input}")
            ])

    def test_convert_to_langchain_template_text(self, opik_dataset):
        """Test conversion of text prompt to ChatPromptTemplate."""
        text_data = "Answer: {question}"

        with patch("langchain_core.prompts.ChatPromptTemplate") as mock_template:
            mock_template.from_template.return_value = Mock()
            opik_dataset._convert_to_langchain_template(text_data)

            mock_template.from_template.assert_called_once_with("Answer: {question}")
