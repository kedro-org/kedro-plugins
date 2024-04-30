"""``EmailMessageDataset`` loads/saves an email message from/to a file
using an underlying filesystem (e.g.: local, S3, GCS). It uses the
``email`` package in the standard library to manage email messages.
"""
from __future__ import annotations

from copy import deepcopy
from email.generator import Generator
from email.message import Message
from email.parser import Parser
from email.policy import default
from pathlib import PurePosixPath
from typing import Any

import fsspec
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)


class EmailMessageDataset(AbstractVersionedDataset[Message, Message]):
    """``EmailMessageDataset`` loads/saves an email message from/to a file
    using an underlying filesystem (e.g.: local, S3, GCS). It uses the
    ``email`` package in the standard library to manage email messages.

    Note that ``EmailMessageDataset`` doesn't handle sending email messages.

    Example:

    .. code-block:: pycon

        >>> from email.message import EmailMessage
        >>>
        >>> from kedro_datasets.email import EmailMessageDataset
        >>>
        >>> string_to_write = "what would you do if you were invisable for one day????"
        >>>
        >>> # Create a text/plain message
        >>> msg = EmailMessage()
        >>> msg.set_content(string_to_write)
        >>> msg["Subject"] = "invisibility"
        >>> msg["From"] = '"sin studly17"'
        >>> msg["To"] = '"strong bad"'
        >>>
        >>> dataset = EmailMessageDataset(filepath=tmp_path / "test")
        >>> dataset.save(msg)
        >>> reloaded = dataset.load()
        >>> assert msg.__dict__ == reloaded.__dict__

    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``EmailMessageDataset`` pointing to a concrete text file
        on a specific filesystem.

        Args:
            filepath: Filepath in POSIX format to a text file prefixed with a protocol like `s3://`.
                If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            load_args: ``email`` options for parsing email messages (arguments passed
                into ``email.parser.Parser.parse``). Here you can find all available arguments:
                https://docs.python.org/3/library/email.parser.html#email.parser.Parser.parse
                If you would like to specify options for the `Parser`,
                you can include them under the "parser" key. Here you can
                find all available arguments:
                https://docs.python.org/3/library/email.parser.html#email.parser.Parser
                All defaults are preserved, but "policy", which is set to ``email.policy.default``.
            save_args: ``email`` options for generating MIME documents (arguments passed into
                ``email.generator.Generator.flatten``). Here you can find all available arguments:
                https://docs.python.org/3/library/email.generator.html#email.generator.Generator.flatten
                If you would like to specify options for the `Generator`,
                you can include them under the "generator" key. Here you can
                find all available arguments:
                https://docs.python.org/3/library/email.generator.html#email.generator.Generator
                All defaults are preserved.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``), as well as
                to pass to the filesystem's `open` method through nested keys
                `open_args_load` and `open_args_save`.
                Here you can find all available arguments for `open`:
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.open
                All defaults are preserved, except `mode`, which is set to `r` when loading
                and to `w` when saving.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)

        self._protocol = protocol
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

        self.metadata = metadata

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        # Handle default load arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._parser_args = self._load_args.pop("parser", {"policy": default})

        # Handle default save arguments
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)
        self._generator_args = self._save_args.pop("generator", {})

        _fs_open_args_load.setdefault("mode", "r")
        _fs_open_args_save.setdefault("mode", "w")
        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "parser_args": self._parser_args,
            "save_args": self._save_args,
            "generator_args": self._generator_args,
            "version": self._version,
        }

    def _load(self) -> Message:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
            return Parser(**self._parser_args).parse(fs_file, **self._load_args)

    def _save(self, data: Message) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            Generator(fs_file, **self._generator_args).flatten(data, **self._save_args)

        self._invalidate_cache()

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False

        return self._fs.exists(load_path)

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
