import logging
import os
from typing import Any, Literal

from kedro.io import AbstractDataset, DatasetError
from opik import configure, track


logger = logging.getLogger(__name__)

REQUIRED_OPIK_CREDENTIALS = {"api_key", "workspace"}
OPTIONAL_OPIK_CREDENTIALS = {"project_name", "url_override"}


class OpikTraceDataset(AbstractDataset):
    """Kedro dataset for managing Opik tracing clients and callbacks."""

    def __init__(
        self,
        credentials: dict[str, Any],
        mode: Literal["sdk", "openai", "langchain"] = "sdk",
        **trace_kwargs: Any,
    ):
        self._credentials = credentials
        self._mode = mode
        self._trace_kwargs = trace_kwargs
        self._cached_client = None

        self._validate_opik_credentials()
        self._configure_opik()

    def _validate_opik_credentials(self) -> None:
        """Validate Opik credentials before setting environment variables."""
        for key in REQUIRED_OPIK_CREDENTIALS:
            if key not in self._credentials or not str(self._credentials[key]).strip():
                raise DatasetError(f"Missing required Opik credential: '{key}'")

        for key in OPTIONAL_OPIK_CREDENTIALS:
            if key in self._credentials and not str(self._credentials[key]).strip():
                raise DatasetError(f"Optional Opik credential '{key}' cannot be empty if provided")

    def _configure_opik(self) -> None:
        """Initialize Opik global configuration with awareness of project switching."""
        project_name = self._credentials.get("project_name")

        # Try to detect an existing configuration and warn if switching projects
        existing_project = os.getenv("OPIK_PROJECT_NAME")
        if existing_project and project_name and project_name != existing_project:
            logger.warning(
                f"Opik is already configured for project '{existing_project}'. "
                f"New project '{project_name}' will be ignored — traces will still "
                f"be logged to the first configured project. "
                f"Restart the Python process or reload the 'opik' module to switch projects.",
            )

        # Set or update the environment variable
        if project_name:
            os.environ["OPIK_PROJECT_NAME"] = project_name

        # Configure Opik (repeated calls are safe but project name won't change)
        configure(
            api_key=self._credentials["api_key"],
            workspace=self._credentials["workspace"],
            url=self._credentials.get("url_override"),
            force=True,
        )

    def _build_openai_client_params(self) -> dict[str, str]:
        """Validate and build OpenAI client parameters from credentials.

        Ensures presence of required OpenAI credentials in the 'openai' section.

        Returns:
            Dictionary with validated OpenAI client parameters.
        Raises:
            DatasetError: If OpenAI credentials are missing or invalid.
        """
        if "openai" not in self._credentials:
            raise DatasetError("OpenAI mode requires an 'openai' section in credentials")

        openai_creds = self._credentials["openai"]

        # Validate API key
        if "openai_api_key" not in openai_creds:
            raise DatasetError("Missing required OpenAI credential: 'openai_api_key'")

        api_key = str(openai_creds["openai_api_key"]).strip()
        if not api_key:
            raise DatasetError("OpenAI API key cannot be empty")

        client_params = {"api_key": api_key}

        # Optional base URL
        base_url = openai_creds.get("openai_api_base")
        if base_url and str(base_url).strip():
            client_params["base_url"] = str(base_url).strip()

        return client_params

    def _describe(self) -> dict[str, Any]:
        return {
            "mode": self._mode,
            "credentials": {k: "***" for k in self._credentials.keys()},
        }

    def load(self) -> Any:
        """Load the appropriate tracing client based on mode."""
        if self._cached_client is not None:
            return self._cached_client

        if self._mode == "sdk":
            self._cached_client = self._load_sdk_client()
        elif self._mode == "openai":
            self._cached_client = self._load_openai_client()
        elif self._mode == "langchain":
            self._cached_client = self._load_langchain_tracer()
        else:
            raise DatasetError(f"Unsupported mode '{self._mode}' for OpikTraceDataset")

        return self._cached_client

    def _load_sdk_client(self):
        """Return a simple SDK client object for tracing via decorators."""
        try:
            from opik import track

            # Simple namespace-like wrapper to mimic a "client"
            # Opik SDK does not provide a client object with .track;
            # instead, we import the track decorator (and flush if needed) directly.
            # This wrapper exposes them as attributes so the dataset interface
            # is consistent with OpenAI and LangChain modes.
            class SDKClient:
                pass

            SDKClient.track = staticmethod(track)

            return SDKClient()
        except ImportError as e:
            raise DatasetError("Opik SDK client not available.") from e

    def _load_openai_client(self):
        """Return an OpenAI client wrapped with Opik tracing."""
        try:
            import openai
            from opik.integrations.openai import track_openai
        except ImportError as e:
            raise DatasetError("OpenAI or Opik OpenAI integration not available") from e

        params = self._build_openai_client_params()
        client = openai.OpenAI(**params)

        # Only project_name is supported as a forward param to track_openai
        project_name = self._trace_kwargs.get("project_name")
        if project_name:
            return track_openai(client, project_name=project_name)
        return track_openai(client)

    def _load_langchain_tracer(self):
        """Return OpikTracer callback for LangChain LCEL or chains."""
        try:
            from opik.integrations.langchain import OpikTracer
        except ImportError as e:
            raise DatasetError("Opik LangChain integration not available") from e

        return OpikTracer(**self._trace_kwargs)

    def save(self, data: Any) -> None:
        raise NotImplementedError("OpikTraceDataset is read-only.")
