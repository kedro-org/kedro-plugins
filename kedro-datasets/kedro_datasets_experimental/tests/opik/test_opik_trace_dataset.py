import os
import sys
from unittest.mock import patch

import pytest
from kedro.io import DatasetError

from kedro_datasets_experimental.opik.opik_trace_dataset import OpikTraceDataset


@pytest.fixture
def base_credentials():
    return {"api_key": "test-key", "workspace": "test-workspace"}  # pragma: allowlist secret


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
def test_init_with_valid_credentials(configure_mock, base_credentials):
    """Test that dataset initializes correctly with valid credentials and calls configure."""
    dataset = OpikTraceDataset(base_credentials)
    assert dataset._credentials == base_credentials
    configure_mock.assert_called_once()


def test_missing_required_credentials_raises():
    """Test that missing required credentials raises DatasetError."""
    with pytest.raises(DatasetError, match="Missing required Opik credential"):
        OpikTraceDataset({"workspace": "x"})


def test_empty_optional_credential_raises(base_credentials):
    """Test that empty optional credentials raise DatasetError."""
    creds = base_credentials | {"project_name": " "}
    with pytest.raises(DatasetError, match="cannot be empty"):
        OpikTraceDataset(creds)


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
def test_configure_opik_sets_project_name(configure_mock, base_credentials):
    """Test that configuring Opik sets the project name environment variable."""
    creds = base_credentials | {"project_name": "test-proj"}
    OpikTraceDataset(creds)
    assert os.getenv("OPIK_PROJECT_NAME") == "test-proj"
    configure_mock.assert_called_once()


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
def test_configure_opik_warns_on_project_switch(configure_mock, base_credentials, caplog):
    """Test that configuring Opik warns when switching to a different project."""
    os.environ["OPIK_PROJECT_NAME"] = "existing-proj"
    creds = base_credentials | {"project_name": "new-proj"}
    OpikTraceDataset(creds)
    assert "will be ignored" in caplog.text
    assert os.getenv("OPIK_PROJECT_NAME") == "existing-proj"


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
@patch("kedro_datasets_experimental.opik.opik_trace_dataset.track")
def test_load_sdk_client_returns_wrapper(track_mock, configure_mock, base_credentials):
    """Test that loading SDK mode returns a wrapper client with a track method."""
    dataset = OpikTraceDataset(base_credentials, mode="sdk")
    client = dataset.load()
    assert hasattr(client, "track")
    assert client.track is track_mock


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
@patch("opik.integrations.openai.track_openai")
@patch("openai.OpenAI")
def test_load_openai_client(openai_mock, track_openai_mock, configure_mock, base_credentials):
    """Test that loading OpenAI client returns a tracked OpenAI client."""
    creds = base_credentials | {
        "openai": {"openai_api_key": "sk-test"},  # pragma: allowlist secret
        "project_name": "proj-a",
    }
    dataset = OpikTraceDataset(creds, mode="openai")
    dataset.load()
    openai_mock.assert_called_once()
    track_openai_mock.assert_called_once()


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
def test_openai_missing_credentials_raises(configure_mock, base_credentials):
    """Test that missing OpenAI API key raises DatasetError."""
    creds = base_credentials | {"openai": {}}
    dataset = OpikTraceDataset(creds, mode="openai")
    with pytest.raises(DatasetError, match="Missing or empty OpenAI API key"):
        dataset.load()


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
def test_openai_missing_section_raises(configure_mock, base_credentials):
    """Test that missing OpenAI section raises DatasetError."""
    dataset = OpikTraceDataset(base_credentials, mode="openai")
    with pytest.raises(DatasetError, match="Missing 'openai' section in OpikTraceDataset credentials."):
        dataset.load()


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
@patch("opik.integrations.langchain.OpikTracer")
def test_load_langchain_tracer(opik_tracer_mock, configure_mock, base_credentials):
    """Test that loading LangChain mode returns an OpikTracer instance."""
    dataset = OpikTraceDataset(base_credentials, mode="langchain")
    client = dataset.load()
    opik_tracer_mock.assert_called_once()
    assert client == opik_tracer_mock.return_value


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
def test_langchain_import_error_raises(configure_mock, base_credentials, monkeypatch):
    """Test that ImportError in LangChain integration raises DatasetError."""
    monkeypatch.setitem(sys.modules, "opik.integrations.langchain", None)
    with pytest.raises(DatasetError, match="Opik LangChain integration not available"):
        OpikTraceDataset(base_credentials, mode="langchain").load()


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
@patch("kedro_datasets_experimental.opik.opik_trace_dataset.track")
def test_client_is_cached(track_mock, configure_mock, base_credentials):
    """Test that multiple calls to load() return the cached client instance."""
    dataset = OpikTraceDataset(base_credentials, mode="sdk")
    client1 = dataset.load()
    client2 = dataset.load()
    assert client1 is client2


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
def test_describe_masks_credentials(configure_mock, base_credentials):
    """Test that _describe() masks credential values."""
    dataset = OpikTraceDataset(base_credentials)
    desc = dataset._describe()
    assert all(v == "***" for v in desc["credentials"].values())


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
def test_save_not_implemented(configure_mock, base_credentials):
    """Test that calling save() raises DatasetError because dataset is read-only."""
    dataset = OpikTraceDataset(base_credentials)
    with pytest.raises(DatasetError):
        dataset.save("data")
