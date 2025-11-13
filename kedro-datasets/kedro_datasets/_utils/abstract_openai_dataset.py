"""Defines an interface to common OpenAI models."""

from abc import abstractmethod
from typing import Any, Generic, NoReturn, TypeVar

from kedro.io import AbstractDataset, DatasetError

OPENAI_TYPE = TypeVar("OPENAI_TYPE")


class AbstractOpenAIDataset(AbstractDataset[None, OPENAI_TYPE], Generic[OPENAI_TYPE]):
    """OpenAI dataset used to access credentials at runtime."""

    @property
    @abstractmethod
    def constructor(self) -> OPENAI_TYPE:
        """Return the OpenAI class to construct in the _load method."""

    def __init__(
        self, credentials: dict[str, str] = None, kwargs: dict[str, Any] = None
    ):
        """Constructor.

        Args:
            credentials (Optional): contains `api_key` and `base_url`.
                If not provided, will use environment variables OPENAI_API_KEY and OPENAI_API_BASE.
            kwargs: keyword arguments passed to the underlying constructor.
        """
        self.credentials = credentials or {}
        self.kwargs = kwargs or {}

    def _describe(self) -> dict[str, Any]:
        """Returns a description of the dataset.

        Returns:
            dict[str, Any]: Dictionary containing the kwargs passed to the OpenAI constructor.
        """
        credentials = (
            {k: "***" for k in self.credentials.keys()} if self.credentials else {}
        )
        return {**credentials, **self.kwargs}

    def save(self, data: None) -> NoReturn:
        """Save operation is not supported for OpenAI datasets.

        Raises:
            DatasetError: Always raised as this dataset is read-only.
        """
        raise DatasetError(f"{self.__class__.__name__} is a read only dataset type")

    def load(self) -> OPENAI_TYPE:
        """Load and return an OpenAI model instance.

        Constructs an OpenAI instance using the provided kwargs and optional
        credentials. If credentials are not provided, the OpenAI instance
        will automatically use environment variables OPENAI_API_KEY and
        OPENAI_API_BASE for authentication.

        Returns:
            OPENAI_TYPE: A configured OpenAI model instance.
        """
        return self.constructor(**self.credentials, **self.kwargs)
