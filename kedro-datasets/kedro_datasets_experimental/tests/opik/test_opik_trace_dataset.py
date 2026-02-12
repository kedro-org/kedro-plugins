import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from kedro.io import DatasetError

from kedro_datasets_experimental.opik.opik_trace_dataset import OpikTraceDataset

OPIK_AUTOGEN_ENDPOINT = "https://www.comet.com/opik/api/v1/private/otel/v1/traces"


@pytest.fixture
def base_credentials():
    return {"api_key": "test-key", "workspace": "test-workspace"}  # pragma: allowlist secret


@pytest.fixture
def autogen_credentials(base_credentials):
    return base_credentials | {"endpoint": OPIK_AUTOGEN_ENDPOINT}


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
def test_openai_missing_credentials_raises(configure_mock, base_credentials, mocker):
    """Test that missing OpenAI API key raises DatasetError."""
    # Mock openai and opik.integrations.openai to avoid real imports
    mock_openai = MagicMock()
    mock_opik_openai = MagicMock()
    mocker.patch.dict("sys.modules", {
        "openai": mock_openai,
        "opik.integrations.openai": mock_opik_openai,
    })

    creds = base_credentials | {"openai": {}}
    dataset = OpikTraceDataset(creds, mode="openai")
    with pytest.raises(DatasetError, match="Missing or empty OpenAI API key"):
        dataset.load()


@patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")
def test_openai_missing_section_raises(configure_mock, base_credentials, mocker):
    """Test that missing OpenAI section raises DatasetError."""
    # Mock openai and opik.integrations.openai to avoid real imports
    mock_openai = MagicMock()
    mock_opik_openai = MagicMock()
    mocker.patch.dict("sys.modules", {
        "openai": mock_openai,
        "opik.integrations.openai": mock_opik_openai,
    })

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


# AutoGen mode tests
class TestOpikTraceDatasetAutogenMode:
    """Tests for AutoGen mode in OpikTraceDataset."""

    def test_autogen_mode_returns_tracer(self, mocker, autogen_credentials):
        """Test AutoGen mode returns configured Tracer."""
        mock_tracer = MagicMock()
        mocker.patch(
            "kedro_datasets_experimental.opik.opik_trace_dataset.OpikTraceDataset._build_autogen_tracer",
            return_value=mock_tracer
        )

        dataset = OpikTraceDataset(autogen_credentials, mode="autogen")

        result = dataset.load()
        assert result == mock_tracer

    def test_autogen_mode_caching(self, mocker, autogen_credentials):
        """Test that AutoGen mode caches the tracer."""
        mock_tracer = MagicMock()
        build_tracer_mock = mocker.patch(
            "kedro_datasets_experimental.opik.opik_trace_dataset.OpikTraceDataset._build_autogen_tracer",
            return_value=mock_tracer
        )

        dataset = OpikTraceDataset(autogen_credentials, mode="autogen")

        # Call load twice
        result1 = dataset.load()
        result2 = dataset.load()

        # Should only build tracer once due to caching
        build_tracer_mock.assert_called_once()
        assert result1 is result2

    def test_autogen_mode_skips_opik_configure(self, mocker, autogen_credentials):
        """Test that AutoGen mode does not call Opik SDK configure."""
        configure_mock = mocker.patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")

        # Mock the tracer builder to avoid actual OpenTelemetry imports
        mocker.patch(
            "kedro_datasets_experimental.opik.opik_trace_dataset.OpikTraceDataset._build_autogen_tracer",
            return_value=MagicMock()
        )

        OpikTraceDataset(autogen_credentials, mode="autogen")

        # configure should not be called for autogen mode
        configure_mock.assert_not_called()

    def test_autogen_mode_missing_endpoint(self, base_credentials):
        """Test that autogen mode raises error when endpoint is missing."""
        with pytest.raises(DatasetError, match="AutoGen mode requires 'endpoint'"):
            OpikTraceDataset(base_credentials, mode="autogen")

    def test_autogen_mode_empty_endpoint(self, base_credentials):
        """Test that autogen mode raises error when endpoint is empty."""
        creds = base_credentials | {"endpoint": ""}
        with pytest.raises(DatasetError, match="AutoGen mode requires 'endpoint'"):
            OpikTraceDataset(creds, mode="autogen")

    def test_autogen_mode_endpoint_not_required_for_other_modes(self, mocker, base_credentials):
        """Test that endpoint is not required for non-autogen modes."""
        mocker.patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")

        # Endpoint is only required for autogen mode
        dataset = OpikTraceDataset(base_credentials, mode="sdk")
        assert dataset._mode == "sdk"

    def test_autogen_mode_import_error(self, mocker, autogen_credentials):
        """Test AutoGen mode raises DatasetError when OpenTelemetry not installed."""

        def raise_import_error():
            raise DatasetError(
                "AutoGen mode requires OpenTelemetry. "
                "Install with: pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http"
            )

        mocker.patch(
            "kedro_datasets_experimental.opik.opik_trace_dataset.OpikTraceDataset._build_autogen_tracer",
            side_effect=raise_import_error
        )

        dataset = OpikTraceDataset(autogen_credentials, mode="autogen")

        with pytest.raises(DatasetError, match="AutoGen mode requires OpenTelemetry"):
            dataset.load()

    def test_describe_autogen_mode(self, mocker, autogen_credentials):
        """Test _describe returns correct format for autogen mode."""
        # Mock the tracer builder to avoid actual OpenTelemetry imports
        mocker.patch(
            "kedro_datasets_experimental.opik.opik_trace_dataset.OpikTraceDataset._build_autogen_tracer",
            return_value=MagicMock()
        )

        dataset = OpikTraceDataset(autogen_credentials, mode="autogen")
        desc = dataset._describe()
        assert desc["mode"] == "autogen"
        assert all(v == "***" for v in desc["credentials"].values())


@pytest.fixture(autouse=True)
def clean_env():
    """Clean up environment variables after each test."""
    yield
    if "OPIK_PROJECT_NAME" in os.environ:
        del os.environ["OPIK_PROJECT_NAME"]
