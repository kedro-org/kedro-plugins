import os
import sys
from unittest.mock import patch, MagicMock

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


# AutoGen mode tests
class TestOpikTraceDatasetAutogenMode:
    """Tests for AutoGen mode in OpikTraceDataset."""

    def test_autogen_mode_returns_batch_span_processor(self, mocker, base_credentials):
        """Test AutoGen mode returns BatchSpanProcessor with OTLPSpanExporter."""
        # Create mock OpenTelemetry modules
        mock_batch_span_processor = MagicMock()
        mock_batch_span_processor_instance = MagicMock()
        mock_batch_span_processor.return_value = mock_batch_span_processor_instance

        mock_otlp_exporter = MagicMock()
        mock_otlp_exporter_instance = MagicMock()
        mock_otlp_exporter.return_value = mock_otlp_exporter_instance

        mock_otel_export_module = MagicMock()
        mock_otel_export_module.BatchSpanProcessor = mock_batch_span_processor

        mock_otlp_module = MagicMock()
        mock_otlp_module.OTLPSpanExporter = mock_otlp_exporter

        mocker.patch.dict("sys.modules", {
            "opentelemetry.sdk.trace.export": mock_otel_export_module,
            "opentelemetry.exporter.otlp.proto.http.trace_exporter": mock_otlp_module,
        })

        dataset = OpikTraceDataset(base_credentials, mode="autogen")

        result = dataset.load()

        # Verify OTLPSpanExporter was called with correct endpoint and headers
        mock_otlp_exporter.assert_called_once()
        call_kwargs = mock_otlp_exporter.call_args[1]
        assert "endpoint" in call_kwargs
        assert call_kwargs["endpoint"] == "https://www.comet.com/opik/api/v1/private/otel/v1/traces"
        assert "headers" in call_kwargs
        assert call_kwargs["headers"]["Authorization"] == "test-key"
        assert call_kwargs["headers"]["Comet-Workspace"] == "test-workspace"

        # Verify BatchSpanProcessor was created with the exporter
        mock_batch_span_processor.assert_called_once_with(mock_otlp_exporter_instance)
        assert result == mock_batch_span_processor_instance

    def test_autogen_mode_with_project_name(self, mocker, base_credentials):
        """Test AutoGen mode includes project name in headers."""
        mock_batch_span_processor = MagicMock()
        mock_otlp_exporter = MagicMock()

        mock_otel_export_module = MagicMock()
        mock_otel_export_module.BatchSpanProcessor = mock_batch_span_processor

        mock_otlp_module = MagicMock()
        mock_otlp_module.OTLPSpanExporter = mock_otlp_exporter

        mocker.patch.dict("sys.modules", {
            "opentelemetry.sdk.trace.export": mock_otel_export_module,
            "opentelemetry.exporter.otlp.proto.http.trace_exporter": mock_otlp_module,
        })

        creds = base_credentials | {"project_name": "my-autogen-project"}
        dataset = OpikTraceDataset(creds, mode="autogen")

        dataset.load()

        call_kwargs = mock_otlp_exporter.call_args[1]
        assert call_kwargs["headers"]["projectName"] == "my-autogen-project"

    def test_autogen_mode_with_custom_url(self, mocker, base_credentials):
        """Test AutoGen mode uses custom URL override for endpoint."""
        mock_batch_span_processor = MagicMock()
        mock_otlp_exporter = MagicMock()

        mock_otel_export_module = MagicMock()
        mock_otel_export_module.BatchSpanProcessor = mock_batch_span_processor

        mock_otlp_module = MagicMock()
        mock_otlp_module.OTLPSpanExporter = mock_otlp_exporter

        mocker.patch.dict("sys.modules", {
            "opentelemetry.sdk.trace.export": mock_otel_export_module,
            "opentelemetry.exporter.otlp.proto.http.trace_exporter": mock_otlp_module,
        })

        creds = base_credentials | {"url_override": "https://custom.opik.com"}
        dataset = OpikTraceDataset(creds, mode="autogen")

        dataset.load()

        call_kwargs = mock_otlp_exporter.call_args[1]
        assert call_kwargs["endpoint"] == "https://custom.opik.com/opik/api/v1/private/otel/v1/traces"

    def test_autogen_mode_skips_opik_configure(self, mocker, base_credentials):
        """Test that AutoGen mode does not call Opik SDK configure."""
        configure_mock = mocker.patch("kedro_datasets_experimental.opik.opik_trace_dataset.configure")

        mock_batch_span_processor = MagicMock()
        mock_otlp_exporter = MagicMock()

        mock_otel_export_module = MagicMock()
        mock_otel_export_module.BatchSpanProcessor = mock_batch_span_processor

        mock_otlp_module = MagicMock()
        mock_otlp_module.OTLPSpanExporter = mock_otlp_exporter

        mocker.patch.dict("sys.modules", {
            "opentelemetry.sdk.trace.export": mock_otel_export_module,
            "opentelemetry.exporter.otlp.proto.http.trace_exporter": mock_otlp_module,
        })

        OpikTraceDataset(base_credentials, mode="autogen")

        # configure should not be called for autogen mode
        configure_mock.assert_not_called()

    def test_autogen_mode_caching(self, mocker, base_credentials):
        """Test that AutoGen mode caches the span processor."""
        mock_batch_span_processor = MagicMock()
        mock_batch_span_processor_instance = MagicMock()
        mock_batch_span_processor.return_value = mock_batch_span_processor_instance

        mock_otlp_exporter = MagicMock()

        mock_otel_export_module = MagicMock()
        mock_otel_export_module.BatchSpanProcessor = mock_batch_span_processor

        mock_otlp_module = MagicMock()
        mock_otlp_module.OTLPSpanExporter = mock_otlp_exporter

        mocker.patch.dict("sys.modules", {
            "opentelemetry.sdk.trace.export": mock_otel_export_module,
            "opentelemetry.exporter.otlp.proto.http.trace_exporter": mock_otlp_module,
        })

        dataset = OpikTraceDataset(base_credentials, mode="autogen")

        # Call load twice
        result1 = dataset.load()
        result2 = dataset.load()

        # Should only create span processor once due to caching
        mock_batch_span_processor.assert_called_once()
        assert result1 is result2

    def test_autogen_mode_import_error(self, mocker, base_credentials):
        """Test AutoGen mode raises DatasetError when OpenTelemetry not installed."""
        mocker.patch.dict("sys.modules", {
            "opentelemetry.sdk.trace.export": None,
            "opentelemetry.exporter.otlp.proto.http.trace_exporter": None,
        })

        dataset = OpikTraceDataset(base_credentials, mode="autogen")

        with pytest.raises(DatasetError, match="AutoGen mode requires OpenTelemetry"):
            dataset.load()

    def test_describe_autogen_mode(self, mocker, base_credentials):
        """Test _describe returns correct format for autogen mode."""
        mock_batch_span_processor = MagicMock()
        mock_otlp_exporter = MagicMock()

        mock_otel_export_module = MagicMock()
        mock_otel_export_module.BatchSpanProcessor = mock_batch_span_processor

        mock_otlp_module = MagicMock()
        mock_otlp_module.OTLPSpanExporter = mock_otlp_exporter

        mocker.patch.dict("sys.modules", {
            "opentelemetry.sdk.trace.export": mock_otel_export_module,
            "opentelemetry.exporter.otlp.proto.http.trace_exporter": mock_otlp_module,
        })

        dataset = OpikTraceDataset(base_credentials, mode="autogen")
        desc = dataset._describe()
        assert desc["mode"] == "autogen"
        assert all(v == "***" for v in desc["credentials"].values())


@pytest.fixture(autouse=True)
def clean_env():
    """Clean up environment variables after each test."""
    yield
    if "OPIK_PROJECT_NAME" in os.environ:
        del os.environ["OPIK_PROJECT_NAME"]
