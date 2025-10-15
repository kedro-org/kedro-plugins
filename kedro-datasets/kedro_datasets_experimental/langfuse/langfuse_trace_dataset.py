import os
from typing import Any, Literal

from kedro.io import AbstractDataset, DatasetError
from langfuse import Langfuse

REQUIRED_LANGFUSE_CREDENTIALS = {"public_key", "secret_key"}
OPTIONAL_LANGFUSE_CREDENTIALS = {"host"}


class LangfuseTraceDataset(AbstractDataset):
    """Kedro dataset for managing Langfuse tracing clients and callbacks.

    This dataset provides appropriate tracing objects based on mode configuration,
    enabling seamless integration with different AI frameworks and direct SDK usage.
    Environment variables are automatically configured during initialization.

    Modes:
    - langchain: Returns CallbackHandler for LangChain integration
    - openai: Returns wrapped OpenAI client with automatic tracing
    - sdk: Returns raw Langfuse client for manual tracing

    Examples:
        Using catalog YAML configuration:

        ```yaml
        langfuse_trace:
          type: langfuse.LangfuseTraceDataset
          credentials: langfuse_credentials
          mode: openai
        ```

        Using Python API:

        ```python
        from kedro_datasets_experimental.langfuse import LangfuseTraceDataset

        # Basic usage (using default Langfuse cloud)
        dataset = LangfuseTraceDataset(
            credentials={
                "public_key": "pk_...",
                "secret_key": "sk_...", # pragma: allowlist secret
                "openai": {"openai_api_key": "sk-..."} # pragma: allowlist secret
            },
            mode="openai"
        )

        # With custom host
        dataset = LangfuseTraceDataset(
            credentials={
                "public_key": "pk_...",
                "secret_key": "sk_...", # pragma: allowlist secret
                "host": "https://custom.langfuse.com",
                "openai": {"openai_api_key": "sk-..."} # pragma: allowlist secret
            },
            mode="openai"
        )

        # Load tracing client
        client = dataset.load()
        response = client.chat.completions.create(...)  # Automatically traced
        ```
    """

    def __init__(
            self,
            credentials: dict[str, Any],
            mode: Literal["langchain", "openai", "sdk"] = "sdk",
            **trace_kwargs: Any
    ):
        """Initialize LangfuseTraceDataset and configure environment variables.

        Validates credentials and sets up appropriate environment variables for
        Langfuse tracing integration. Environment variables are set immediately
        during initialization for use by all tracing modes.

        Args:
            credentials: Dictionary with Langfuse credentials. Required keys:
                {public_key, secret_key}. Optional keys: {host} (defaults to
                Langfuse cloud if not provided). For OpenAI mode, may also include
                openai section with {openai_api_key, openai_api_base}.
            mode: Tracing mode - "langchain", "openai", or "sdk" (default).
            **trace_kwargs: Additional kwargs passed to the tracing client.

        Raises:
            DatasetError: If required Langfuse credentials are missing or empty.

        Examples:
            # Basic SDK mode (using default Langfuse cloud)
                dataset = LangfuseTraceDataset(
            ...     credentials={"public_key": "pk_...", "secret_key": "sk_..."}  # pragma: allowlist secret
            ... )

            # With custom host
                dataset = LangfuseTraceDataset(
            ...     credentials={
            ...         "public_key": "pk_...", "secret_key": "sk_...",  # pragma: allowlist secret
            ...         "host": "https://custom.langfuse.com"
            ...     }
            ... )

            # OpenAI mode with API key
                dataset = LangfuseTraceDataset(
            ...     credentials={
            ...         "public_key": "pk_...", "secret_key": "sk_...",  # pragma: allowlist secret
            ...         "openai": {"openai_api_key": "sk-...", "openai_api_base": "..."} # pragma: allowlist secret
            ...     },
            ...     mode="openai"
            ... )

        Note:
            Sets LANGFUSE_SECRET_KEY, LANGFUSE_PUBLIC_KEY, and LANGFUSE_HOST
            environment variables from the provided credentials. Also sets
            OPENAI_API_KEY if provided for OpenAI mode compatibility.
        """
        self._credentials = credentials
        self._mode = mode
        self._trace_kwargs = trace_kwargs
        self._cached_client = None

        # Validate Langfuse credentials before setting environment variables
        self._validate_langfuse_credentials()

        # Set Langfuse environment variables from credentials
        os.environ["LANGFUSE_SECRET_KEY"] = self._credentials["secret_key"]
        os.environ["LANGFUSE_PUBLIC_KEY"] = self._credentials["public_key"]

        if "host" in self._credentials:
            os.environ["LANGFUSE_HOST"] = self._credentials["host"]

    def _validate_langfuse_credentials(self) -> None:
        """Validate Langfuse credentials before setting environment variables.

        Raises:
            DatasetError: If Langfuse credentials are missing or invalid.
        """
        # Validate required keys
        for key in REQUIRED_LANGFUSE_CREDENTIALS:
            if key not in self._credentials:
                raise DatasetError(f"Missing required Langfuse credential: '{key}'")

            # Validate that credential is not empty
            if not self._credentials[key] or not str(self._credentials[key]).strip():
                raise DatasetError(f"Langfuse credential '{key}' cannot be empty")

        # Validate optional keys if present
        for key in OPTIONAL_LANGFUSE_CREDENTIALS:
            if key in self._credentials:
                # If host is provided, it cannot be empty
                if not self._credentials[key] or not str(self._credentials[key]).strip():
                    raise DatasetError(f"Langfuse credential '{key}' cannot be empty if provided")

    def _describe(self) -> dict[str, Any]:
        """Return a description of the dataset for Kedro's internal use.

        Returns:
            Dictionary containing dataset description with mode and masked credentials.
        """
        return {"mode": self._mode, "credentials": "***"}

    def _build_openai_client_params(self) -> dict[str, str]:
        """Validate and build OpenAI client parameters from credentials.

        Validates the presence and content of required OpenAI credentials,
        then constructs parameters dictionary for OpenAI client initialization.

        Returns:
            Dictionary with validated OpenAI client parameters. Always includes
            'api_key', optionally includes 'base_url' if provided.

        Raises:
            DatasetError: If OpenAI credentials are missing or invalid.

        Examples:
            # With API key only
                params = self._build_openai_client_params()
                # Returns: {"api_key": "sk-..."}  # pragma: allowlist secret

            # With API key and custom base URL
                params = self._build_openai_client_params()
                # Returns: {"api_key": "sk-...", "base_url": "https://api.custom.com"}  # pragma: allowlist secret
        """
        # Check if openai section exists
        if "openai" not in self._credentials:
            raise DatasetError("OpenAI mode requires 'openai' section in credentials")

        openai_creds = self._credentials["openai"]

        # Check for required API key
        if "openai_api_key" not in openai_creds:
            raise DatasetError("Missing required OpenAI credential: 'openai_api_key'")

        # Validate that API key is not empty
        if not openai_creds["openai_api_key"] or not openai_creds["openai_api_key"].strip():
            raise DatasetError("OpenAI API key cannot be empty")

        # Build validated client parameters
        client_params = {"api_key": openai_creds["openai_api_key"]}

        # Add base_url if provided (optional)
        if "openai_api_base" in openai_creds and openai_creds["openai_api_base"]:
            client_params["base_url"] = openai_creds["openai_api_base"]

        return client_params

    def load(self) -> Any:
        """Load appropriate tracing client based on configured mode.

        Creates and returns the appropriate tracing client for the specified mode.
        The client is cached after first load to avoid repeated initialisation.
        All clients use environment variables set during initialisation for authentication.

        Returns:
            Tracing client object based on mode:
            - langchain mode: CallbackHandler for LangChain integration
            - openai mode: Wrapped OpenAI client with automatic tracing
            - sdk mode: Raw Langfuse client for manual tracing

        Raises:
            DatasetError: If OpenAI mode is used but OpenAI credentials are missing or invalid.

        Examples:
            # LangChain mode
                dataset = LangfuseTraceDataset(credentials=creds, mode="langchain")
                callback = dataset.load()
                chain.invoke(input, config={"callbacks": [callback]})

            # OpenAI mode
                dataset = LangfuseTraceDataset(credentials=creds, mode="openai")
                client = dataset.load()
                response = client.chat.completions.create(model="gpt-4", messages=[...])

            # SDK mode
                dataset = LangfuseTraceDataset(credentials=creds, mode="sdk")
                langfuse = dataset.load()
                trace = langfuse.trace(name="my-trace")
        """
        # Return cached client if available
        if self._cached_client is not None:
            return self._cached_client

        # Create and cache the appropriate client
        if self._mode == "langchain":
            from langfuse.langchain import CallbackHandler  # noqa PLC0415
            self._cached_client = CallbackHandler()
        elif self._mode == "openai":
            from langfuse.openai import OpenAI  # noqa PLC0415
            client_params = self._build_openai_client_params()
            self._cached_client = OpenAI(**client_params)
        else:
            try:
                from langfuse import get_client  # noqa PLC0415
                self._cached_client = get_client()
            except ImportError:
                from langfuse import Langfuse  # noqa PLC0415
                self._cached_client = Langfuse()

        return self._cached_client

    def save(self, data: Any) -> None:
        """Save operation is not supported for tracing datasets.

        Args:
            data: Data to save (not used).

        Raises:
            NotImplementedError: Always raised as tracing datasets are read-only.

        Note:
            LangfuseTraceDataset is designed for providing tracing clients,
            not for data storage. Use the returned tracing clients to automatically
            log traces, spans, and generations to Langfuse.
        """
        raise NotImplementedError("LangfuseTraceDataset is read-only - it provides tracing clients, not data storage")
