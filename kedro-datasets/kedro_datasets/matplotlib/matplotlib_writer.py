"""``MatplotlibWriter`` saves one or more Matplotlib objects as image
files to an underlying filesystem (e.g. local, S3, GCS)."""

import io
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, Dict, List, NoReturn, Union
from warnings import warn

import fsspec
import matplotlib.pyplot as plt
from kedro.io.core import (
    AbstractVersionedDataSet,
    DataSetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)


class MatplotlibWriter(
    AbstractVersionedDataSet[
        Union[plt.figure, List[plt.figure], Dict[str, plt.figure]], NoReturn
    ]
):
    """``MatplotlibWriter`` saves one or more Matplotlib objects as
    image files to an underlying filesystem (e.g. local, S3, GCS).

    Example adding a catalog entry with the `YAML API
    <https://kedro.readthedocs.io/en/stable/data/\
        data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

    .. code-block:: yaml

        >>> output_plot:
        >>>   type: matplotlib.MatplotlibWriter
        >>>   filepath: data/08_reporting/output_plot.png
        >>>   save_args:
        >>>     format: png
        >>>

    Example using the Python API:

    ::

        >>> import matplotlib.pyplot as plt
        >>> from kedro_datasets.matplotlib import MatplotlibWriter
        >>>
        >>> fig = plt.figure()
        >>> plt.plot([1, 2, 3])
        >>> plot_writer = MatplotlibWriter(
        >>>     filepath="data/08_reporting/output_plot.png"
        >>> )
        >>> plt.close()
        >>> plot_writer.save(fig)

    Example saving a plot as a PDF file:

    ::

        >>> import matplotlib.pyplot as plt
        >>> from kedro_datasets.matplotlib import MatplotlibWriter
        >>>
        >>> fig = plt.figure()
        >>> plt.plot([1, 2, 3])
        >>> pdf_plot_writer = MatplotlibWriter(
        >>>     filepath="data/08_reporting/output_plot.pdf",
        >>>     save_args={"format": "pdf"},
        >>> )
        >>> plt.close()
        >>> pdf_plot_writer.save(fig)


    Example saving multiple plots in a folder, using a dictionary:

    ::

        >>> import matplotlib.pyplot as plt
        >>> from kedro_datasets.matplotlib import MatplotlibWriter
        >>>
        >>> plots_dict = dict()
        >>> for colour in ["blue", "green", "red"]:
        >>>     plots_dict[f"{colour}.png"] = plt.figure()
        >>>     plt.plot([1, 2, 3], color=colour)
        >>>
        >>> plt.close("all")
        >>> dict_plot_writer = MatplotlibWriter(
        >>>     filepath="data/08_reporting/plots"
        >>> )
        >>> dict_plot_writer.save(plots_dict)

    Example saving multiple plots in a folder, using a list:

    ::

        >>> import matplotlib.pyplot as plt
        >>> from kedro_datasets.matplotlib import MatplotlibWriter
        >>>
        >>> plots_list = []
        >>> for i in range(5):
        >>>     plots_list.append(plt.figure())
        >>>     plt.plot([i, i + 1, i + 2])
        >>> plt.close("all")
        >>> list_plot_writer = MatplotlibWriter(
        >>>     filepath="data/08_reporting/plots"
        >>> )
        >>> list_plot_writer.save(plots_list)

    """

    DEFAULT_SAVE_ARGS = {}  # type: Dict[str, Any]

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        filepath: str,
        fs_args: Dict[str, Any] = None,
        credentials: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None,
        overwrite: bool = False,
    ) -> None:
        """Creates a new instance of ``MatplotlibWriter``.

        Args:
            filepath: Filepath in POSIX format to save Matplotlib objects to, prefixed with a
                protocol like `s3://`. If prefix is not provided, `file` protocol (local filesystem)
                will be used. The prefix should be any protocol supported by ``fsspec``.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``), as well as
                to pass to the filesystem's `open` method through nested key `open_args_save`.
                Here you can find all available arguments for `open`:
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.open
                All defaults are preserved, except `mode`, which is set to `wb` when saving.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``S3FileSystem`` it should look like:
                `{'key': '<id>', 'secret': '<key>'}}`
            save_args: Save args passed to `plt.savefig`. See
                https://matplotlib.org/api/_as_gen/matplotlib.pyplot.savefig.html
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            overwrite: If True, any existing image files will be removed.
                Only relevant when saving multiple Matplotlib objects at
                once.
        """
        _credentials = deepcopy(credentials) or {}
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _fs_open_args_save.setdefault("mode", "wb")

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        self._fs_open_args_save = _fs_open_args_save

        # Handle default save arguments
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        if overwrite and version is not None:
            warn(
                "Setting 'overwrite=True' is ineffective if versioning "
                "is enabled, since the versioned path must not already "
                "exist; overriding flag with 'overwrite=False' instead."
            )
            overwrite = False
        self._overwrite = overwrite

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
            protocol=self._protocol,
            save_args=self._save_args,
            version=self._version,
        )

    def _load(self) -> NoReturn:
        raise DataSetError(f"Loading not supported for '{self.__class__.__name__}'")

    def _save(
        self, data: Union[plt.figure, List[plt.figure], Dict[str, plt.figure]]
    ) -> None:
        save_path = self._get_save_path()

        if isinstance(data, (list, dict)) and self._overwrite and self._exists():
            self._fs.rm(get_filepath_str(save_path, self._protocol), recursive=True)

        if isinstance(data, list):
            for index, plot in enumerate(data):
                full_key_path = get_filepath_str(
                    save_path / f"{index}.png", self._protocol
                )
                self._save_to_fs(full_key_path=full_key_path, plot=plot)
        elif isinstance(data, dict):
            for plot_name, plot in data.items():
                full_key_path = get_filepath_str(save_path / plot_name, self._protocol)
                self._save_to_fs(full_key_path=full_key_path, plot=plot)
        else:
            full_key_path = get_filepath_str(save_path, self._protocol)
            self._save_to_fs(full_key_path=full_key_path, plot=data)

        plt.close("all")

        self._invalidate_cache()

    def _save_to_fs(self, full_key_path: str, plot: plt.figure):
        bytes_buffer = io.BytesIO()
        plot.savefig(bytes_buffer, **self._save_args)

        with self._fs.open(full_key_path, **self._fs_open_args_save) as fs_file:
            fs_file.write(bytes_buffer.getvalue())

    def _exists(self) -> bool:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        return self._fs.exists(load_path)

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
