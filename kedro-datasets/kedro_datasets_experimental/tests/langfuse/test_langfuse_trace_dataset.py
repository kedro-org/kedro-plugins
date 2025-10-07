"""Unit tests for LangfuseTraceDataset."""

import pytest

from kedro_datasets_experimental.langfuse import LangfuseTraceDataset


class TestLangfuseTraceDataset:
    def test_missing_credentials(self):
        """Test that dataset raises error when credentials are missing."""
        with pytest.raises(ValueError, match="Missing required Langfuse credential"):
            LangfuseTraceDataset(credentials={})

    def test_empty_credentials(self):
        """Test that dataset raises error when credentials are empty."""
        with pytest.raises(ValueError, match="cannot be empty"):
            LangfuseTraceDataset(credentials={"public_key": "", "secret_key": "sk"})

    def test_langchain_mode(self, mocker):
        """Test langchain mode returns CallbackHandler."""
        mocker.patch.dict("os.environ", {}, clear=True)
        dataset = LangfuseTraceDataset(
            credentials={"public_key": "pk_test", "secret_key": "sk_test"},
            mode="langchain"
        )
        # Mock the CallbackHandler import
        mock_handler = mocker.patch("kedro_datasets_experimental.langfuse.langfuse_trace_dataset.CallbackHandler")

        result = dataset.load()
        mock_handler.assert_called_once()
        assert result == mock_handler.return_value
