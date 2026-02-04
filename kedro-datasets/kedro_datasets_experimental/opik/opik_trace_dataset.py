import logging
import os
from typing import Any, Literal

from kedro.io import AbstractDataset, DatasetError
from opik import configure, track

logger = logging.getLogger(__name__)

REQUIRED_OPIK_CREDENTIALS = {"api_key", "workspace"}
OPTIONAL_OPIK_CREDENTIALS = {"project_name", "url_override"}


class OpikTraceDataset(AbstractDataset):
    """Kedro dataset for managing Opik tracing clients and callbacks.

    This dataset provides Opik tracing integrations for various AI frameworks or direct SDK usage.
    During initialization, the dataset automatically configures the Opik environment and credentials
    to ensure that subsequent traces are correctly logged to the specified workspace and project.

    **Modes:**

    - ``sdk``: Returns a simple namespace-like client exposing the ``track`` decorator for manual tracing.
    - ``openai``: Returns an OpenAI client automatically wrapped for Opik tracing.
    - ``langchain``: Returns an ``OpikTracer`` callback handler for LangChain integration.
    - ``autogen``: Returns a configured ``Tracer`` for AutoGen integration via OTLP.

    **Examples**

    Using catalog YAML configuration:

    .. code-block:: yaml

        opik_trace:
          type: kedro_datasets_experimental.opik.OpikTraceDataset
          credentials: opik_credentials
          mode: openai

    Using Python API:

    .. code-block:: python

        from kedro_datasets_experimental.opik import OpikTraceDataset

        # Example: OpenAI mode (traced completions)
        dataset = OpikTraceDataset(
            credentials={
                "api_key": "opik_api_key",  # pragma: allowlist secret
                "workspace": "my-workspace",
                "project_name": "kedro-demo",
                "openai": {
                    "openai_api_key": "sk-...",  # pragma: allowlist secret
                    "openai_api_base": "https://api.openai.com/v1",
                },
            },
            mode="openai",
        )
        client = dataset.load()
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Summarize Kedro in one sentence."},
            ],
        )

        # Example: SDK mode (manual tracing via decorator)
        dataset = OpikTraceDataset(
            credentials={
                "api_key": "opik_api_key",  # pragma: allowlist secret
                "workspace": "my-workspace",
                "project_name": "kedro-sdk-demo",
            },
            mode="sdk",
        )
        client = dataset.load()


        @client.track(name="demo_workflow")
        def multiply(x: int, y: int) -> int:
            return x * y


        print(multiply(3, 4))

        # Example: LangChain mode
        dataset = OpikTraceDataset(
            credentials={
                "api_key": "opik_api_key",  # pragma: allowlist secret
                "workspace": "my-workspace",
            },
            mode="langchain",
        )
        tracer = dataset.load()
        # Use tracer in your LangChain Runnable or chain.run(callbacks=[tracer])

        # Example: AutoGen mode (agent tracing via OpenTelemetry)
        dataset = OpikTraceDataset(
            credentials={
                "api_key": "opik_api_key",  # pragma: allowlist secret
                "workspace": "my-workspace",
                "project_name": "autogen-demo",
                "url_override": "https://www.comet.com",
            },
            mode="autogen",
        )
        tracer = dataset.load()  # Returns configured Tracer, ready to use

        # Option 1: Automatic tracing (LLM calls traced automatically)
        agent.invoke(context)  # Traces sent to Opik

        # Option 2: Add custom spans with business context (recommended)
        with tracer.start_as_current_span("response_generation") as span:
            span.set_attribute("intent", "claim_new")
            span.set_attribute("user_id", "123")
            agent.invoke(context)  # Child spans nested under "response_generation"

    **Notes**

    - Opik configuration is global within the Python process.
      Using multiple ``OpikTraceDataset`` instances with different projects in the same session
      may cause all traces to log to the first configured project.
    - To switch projects, restart the Python process or reload the Opik module.
    """

    def __init__(
        self,
        credentials: dict[str, Any],
        mode: Literal["sdk", "openai", "langchain", "autogen"] = "sdk",
        **trace_kwargs: Any,
    ):
        self._credentials = credentials
        self._mode = mode
        self._trace_kwargs = trace_kwargs
        self._cached_client = None

        self._validate_opik_credentials()
        # Skip Opik SDK configuration for autogen mode (uses OTLP directly)
        if self._mode != "autogen":
            self._configure_opik()

    def _validate_opik_credentials(self) -> None:
        """Validate Opik credentials before configuring the environment."""
        for key in REQUIRED_OPIK_CREDENTIALS:
            if key not in self._credentials or not str(self._credentials[key]).strip():
                raise DatasetError(f"Missing required Opik credential: '{key}'")

        for key in OPTIONAL_OPIK_CREDENTIALS:
            if key in self._credentials and not str(self._credentials[key]).strip():
                raise DatasetError(f"Optional Opik credential '{key}' cannot be empty if provided")

        # AutoGen mode requires 'url_override' to construct the OTLP endpoint
        if self._mode == "autogen" and not self._credentials.get("url_override"):
            raise DatasetError(
                "AutoGen mode requires 'url_override' in credentials (e.g. 'https://www.comet.com'). "
                "This is needed to construct the OTLP endpoint for trace export."
            )

    def _configure_opik(self) -> None:
        """Initialize Opik global configuration with awareness of project switching.

        This function ensures that the Opik SDK is configured using the provided credentials.
        If an existing configuration is detected (from a prior dataset instance),
        a warning is emitted since the active project cannot be changed dynamically.
        """
        project_name = self._credentials.get("project_name")

        # Detect an existing configuration and warn if switching projects
        existing_project = os.getenv("OPIK_PROJECT_NAME")
        if existing_project and project_name and project_name != existing_project:
            logger.warning(
                f"Opik is already configured for project '{existing_project}', "
                f"as defined by the environment variable OPIK_PROJECT_NAME. "
                f"The active project cannot be changed dynamically — the new project "
                f"'{project_name}' will be ignored, and all traces will continue "
                f"to be logged under '{existing_project}'.\n"
                f"To log traces to a different project, unset the environment variable "
                f"`OPIK_PROJECT_NAME` before running your pipeline or in the interactive session."
            )
        # Set or update the environment variable (used by Opik SDK)
        elif project_name:
            os.environ["OPIK_PROJECT_NAME"] = project_name

        # Configure Opik (repeated calls are safe but project name won't change)
        configure(
            api_key=self._credentials["api_key"],
            workspace=self._credentials["workspace"],
            url=self._credentials.get("url_override"),
            force=True,
        )

    def _build_openai_client_params(self) -> dict[str, str]:
        """Validate and construct OpenAI client parameters from credentials."""
        if "openai" not in self._credentials:
            raise DatasetError(
                "Missing 'openai' section in OpikTraceDataset credentials. "
                "For OpenAI mode, include an 'openai' block inside your credentials."
            )

        openai_creds = self._credentials["openai"]

        api_key = str(openai_creds.get("openai_api_key", "")).strip()
        if not api_key:
            raise DatasetError("Missing or empty OpenAI API key")

        params = {"api_key": api_key}

        base_url = openai_creds.get("openai_api_base")
        if base_url and str(base_url).strip():
            params["base_url"] = str(base_url).strip()

        return params

    def _build_autogen_tracer(self) -> Any:
        """Build and return a configured Tracer for AutoGen integration with Opik.

        Sets up OpenTelemetry TracerProvider with OTLP exporter to Opik,
        configures it as the global provider, and returns a ready-to-use Tracer.

        Returns:
            Tracer configured to export traces to Opik.

        Raises:
            DatasetError: If required OpenTelemetry dependencies are not installed.
        """
        try:
            from opentelemetry import trace  # noqa: PLC0415
            from opentelemetry.sdk.trace import TracerProvider  # noqa: PLC0415
            from opentelemetry.sdk.trace.export import BatchSpanProcessor  # noqa: PLC0415
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter  # noqa: PLC0415
        except ImportError as exc:
            raise DatasetError(
                "AutoGen mode requires OpenTelemetry. "
                "Install with: pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-http"
            ) from exc

        # Build headers for Opik authentication
        headers = {
            "Authorization": self._credentials["api_key"],
            "Comet-Workspace": self._credentials["workspace"],
        }

        # Add project name if specified
        project_name = self._credentials.get("project_name")
        if project_name:
            headers["projectName"] = project_name

        # Use Opik's OTLP endpoint (url_override validated in _validate_opik_credentials)
        base_url = self._credentials["url_override"]
        endpoint = f"{base_url}/opik/api/v1/private/otel/v1/traces"

        exporter = OTLPSpanExporter(
            endpoint=endpoint,
            headers=headers
        )

        processor = BatchSpanProcessor(exporter)

        # Use existing provider if already set, otherwise create a new one.
        existing_provider = trace.get_tracer_provider()
        if hasattr(existing_provider, "add_span_processor"):
            existing_provider.add_span_processor(processor)
        else:
            provider = TracerProvider()
            provider.add_span_processor(processor)
            trace.set_tracer_provider(provider)

        return trace.get_tracer("opik.autogen")

    def _describe(self) -> dict[str, Any]:
        """Describe dataset configuration with credentials redacted."""
        creds = self._credentials.copy()
        if "openai" in creds:
            creds["openai"] = {k: "***" for k in creds["openai"].keys()}

        return {
            "mode": self._mode,
            "credentials": {k: "***" for k in creds.keys()},
        }

    def load(self) -> Any:
        """Load the appropriate tracing client based on the configured mode.

        Returns:
            Tracing client object based on mode:
            - sdk mode: SDKClient wrapper exposing the track decorator
            - openai mode: Wrapped OpenAI client with automatic tracing
            - langchain mode: OpikTracer callback handler
            - autogen mode: Configured Tracer for OpenTelemetry integration

        Raises:
            DatasetError: If mode-specific dependencies are missing or credentials are invalid.
        """
        if self._cached_client is not None:
            return self._cached_client

        if self._mode == "sdk":
            self._cached_client = self._load_sdk_client()
        elif self._mode == "openai":
            self._cached_client = self._load_openai_client()
        elif self._mode == "langchain":
            self._cached_client = self._load_langchain_tracer()
        elif self._mode == "autogen":
            self._cached_client = self._build_autogen_tracer()
        else:
            raise DatasetError(f"Unsupported mode '{self._mode}' for OpikTraceDataset")

        return self._cached_client

    def _load_sdk_client(self) -> Any:
        """Return a simple SDK client exposing the `track` decorator.

        The Opik SDK does not provide a formal client object for direct usage;
        instead, the `track` decorator is imported at the module level.
        This wrapper mimics a client interface for consistency across modes.
        """

        # Simple namespace-like wrapper to mimic a "client"
        class SDKClient:
            track = staticmethod(track)

        return SDKClient()

    def _load_openai_client(self) -> Any:
        """Return an OpenAI client wrapped with Opik tracing integration."""
        try:
            import openai  # noqa: PLC0415
            from opik.integrations.openai import track_openai  # noqa: PLC0415
        except ImportError as e:
            raise DatasetError(
                "OpenAI or Opik OpenAI integration not available. "
                "Ensure you have installed the required dependencies: "
                "pip install openai opik"
            ) from e

        params = self._build_openai_client_params()
        client = openai.OpenAI(**params)

        project_name = self._trace_kwargs.get("project_name")
        env_project = os.getenv("OPIK_PROJECT_NAME")
        if project_name and env_project and project_name != env_project:
            logger.warning(
                f"Project name mismatch detected: trace_kwargs specifies '{project_name}', "
                f"but environment variable OPIK_PROJECT_NAME is set to '{env_project}'. "
                f"The environment value will take precedence."
            )

        return track_openai(client, project_name=project_name) if project_name else track_openai(client)

    def _load_langchain_tracer(self) -> Any:
        """Return an OpikTracer callback for LangChain integration."""
        try:
            from opik.integrations.langchain import OpikTracer  # noqa: PLC0415
        except ImportError as e:
            raise DatasetError("Opik LangChain integration not available.") from e

        return OpikTracer(**self._trace_kwargs)

    def save(self, data: Any) -> None:
        """Saving traces manually is not supported; OpikTraceDataset is read-only."""
        raise NotImplementedError("OpikTraceDataset is read-only.")
