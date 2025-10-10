"""``PDFDataset`` loads data from PDF files using an underlying
filesystem (e.g.: local, S3, GCS). It uses pypdf to read and extract text from PDF files.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, NoReturn

import fsspec
import pypdf
from kedro.io.core import (
    AbstractDataset,
    DatasetError,
    get_filepath_str,
    get_protocol_and_path,
)


class PDFDataset(AbstractDataset[NoReturn, list[str]]):
    """``PDFDataset`` loads data from PDF files using an underlying
    filesystem (e.g.: local, S3, GCS). It uses pypdf to read and extract text from PDF files.

    This is a read-only dataset - saving is not supported.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        my_pdf_document:
          type: pypdf.PDFDataset
          filepath: data/01_raw/document.pdf

        password_protected_pdf:
          type: pypdf.PDFDataset
          filepath: data/01_raw/protected.pdf
          load_args:
            password: "pass123"  # pragma: allowlist secret

        s3_pdf:
          type: pypdf.PDFDataset
          filepath: s3://your_bucket/document.pdf
          credentials: dev_s3
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from kedro_datasets_experimental.pypdf import PDFDataset
        >>>
        >>> dataset = PDFDataset(filepath="data/document.pdf")
        >>> pages = dataset.load()
        >>> # pages is a list of strings, one per page
        >>> assert isinstance(pages, list)
        >>> assert all(isinstance(page, str) for page in pages)

    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {"strict": False}

    def __init__(
        self,
        *,
        filepath: str,
        load_args: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``PDFDataset`` pointing to a concrete PDF file
        on a specific filesystem.

        Args:
            filepath: Filepath in POSIX format to a PDF file prefixed with a protocol like `s3://`.
                If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
            load_args: Pypdf options for loading PDF files (arguments passed
                into ``pypdf.PdfReader``). Here you can find all available arguments:
                https://pypdf.readthedocs.io/en/stable/modules/PdfReader.html
                All defaults are preserved, except "strict", which is set to False.
                Common options include:
                - password (str): Password for encrypted PDFs
                - strict (bool): Whether to raise errors on malformed PDFs (default: False)
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``), as well as
                to pass to the filesystem's `open` method through nested keys
                `open_args_load` and `open_args_save`.
                Here you can find all available arguments for `open`:
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.open
                All defaults are preserved.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _credentials = deepcopy(credentials) or {}

        super().__init__()

        protocol, path = get_protocol_and_path(filepath)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)
        self._filepath = PurePosixPath(path)
        self.metadata = metadata

        # Handle default load and fs arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._fs_open_args_load = _fs_open_args_load or {}

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "load_args": self._load_args,
        }

    def load(self) -> list[str]:
        """Loads data from a PDF file.

        Returns:
            list[str]: A list of strings, where each string contains the text extracted from one page.
        """
        load_path = get_filepath_str(self._filepath, self._protocol)

        with self._fs.open(load_path, mode="rb", **self._fs_open_args_load) as fs_file:
            pdf_reader = pypdf.PdfReader(stream=fs_file, **self._load_args)
            pages = []
            for page in pdf_reader.pages:
                pages.append(page.extract_text())
            return pages

    def save(self, data: NoReturn) -> None:
        """Saving to PDFDataset is not supported.

        Args:
            data: Data to save.

        Raises:
            DatasetError: Always raised as saving is not supported.
        """
        raise DatasetError("Saving to PDFDataset is not supported.")

    def _exists(self) -> bool:
        """Check if the PDF file exists.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        load_path = get_filepath_str(self._filepath, self._protocol)
        return self._fs.exists(load_path)

    def _release(self) -> None:
        """Release any cached filesystem information."""
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
