"""``PlotlyDataSet`` generates a plot from a pandas DataFrame and saves it to a JSON
file using an underlying filesystem (e.g.: local, S3, GCS). It loads the JSON into a
plotly figure.
"""
from copy import deepcopy
from typing import Any, Dict

import pandas as pd
import plotly.express as px
from kedro.io.core import Version
from plotly import graph_objects as go

from .json_dataset import JSONDataSet


class PlotlyDataSet(JSONDataSet):
    """``PlotlyDataSet`` generates a plot from a pandas DataFrame and saves it to a JSON
    file using an underlying filesystem (e.g.: local, S3, GCS). It loads the JSON into a
    plotly figure.

    ``PlotlyDataSet`` is a convenience wrapper for ``plotly.JSONDataSet``. It generates
    the JSON file directly from a pandas DataFrame through ``plotly_args``.

    Example adding a catalog entry with YAML API:

    .. code-block:: yaml

        >>> bar_plot:
        >>>   type: plotly.PlotlyDataSet
        >>>   filepath: data/08_reporting/bar_plot.json
        >>>   plotly_args:
        >>>     type: bar
        >>>     fig:
        >>>         x: features
        >>>         y: importance
        >>>         orientation: h
        >>>     layout:
        >>>         xaxis_title: x
        >>>         yaxis_title: y
        >>>         title: Title

    Example using Python API:
    ::

        >>> from kedro_datasets.plotly import PlotlyDataSet
        >>> import plotly.express as px
        >>> import pandas as pd
        >>>
        >>> df_data = pd.DataFrame([[0, 1], [1, 0]], columns=('x1', 'x2'))
        >>>
        >>> data_set = PlotlyDataSet(
        >>>     filepath='scatter_plot.json',
        >>>     plotly_args={
        >>>         'type': 'scatter',
        >>>         'fig': {'x': 'x1', 'y': 'x2'},
        >>>     }
        >>> )
        >>> data_set.save(df_data)
        >>> reloaded = data_set.load()
        >>> assert px.scatter(df_data, x='x1', y='x2') == reloaded

    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        filepath: str,
        plotly_args: Dict[str, Any],
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None,
        credentials: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``PlotlyDataSet`` pointing to a concrete JSON file
        on a specific filesystem.

        Args:
            filepath: Filepath in POSIX format to a JSON file prefixed with a protocol like `s3://`.
                If prefix is not provided `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            plotly_args: Plotly configuration for generating a plotly figure from the
                dataframe. Keys are `type` (plotly express function, e.g. bar,
                line, scatter), `fig` (kwargs passed to the plotting function), theme
                (defaults to `plotly`), `layout`.
            load_args: Plotly options for loading JSON files.
                Here you can find all available arguments:
                https://plotly.com/python-api-reference/generated/plotly.io.from_json.html#plotly.io.from_json
                All defaults are preserved.
            save_args: Plotly options for saving JSON files.
                Here you can find all available arguments:
                https://plotly.com/python-api-reference/generated/plotly.io.write_json.html
                All defaults are preserved.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{'token': None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``), as well as
                to pass to the filesystem's `open` method through nested keys
                `open_args_load` and `open_args_save`.
                Here you can find all available arguments for `open`:
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.open
                All defaults are preserved, except `mode`, which is set to `w` when saving.
        """
        super().__init__(filepath, load_args, save_args, version, credentials, fs_args)
        self._plotly_args = plotly_args

        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _fs_open_args_save.setdefault("mode", "w")

        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

    def _describe(self) -> Dict[str, Any]:
        return {**super()._describe(), "plotly_args": self._plotly_args}

    def _save(self, data: pd.DataFrame) -> None:
        fig = self._plot_dataframe(data)
        super()._save(fig)

    def _plot_dataframe(self, data: pd.DataFrame) -> go.Figure:
        plot_type = self._plotly_args.get("type")
        fig_params = self._plotly_args.get("fig", {})
        fig = getattr(px, plot_type)(data, **fig_params)  # type: ignore
        fig.update_layout(template=self._plotly_args.get("theme", "plotly"))
        fig.update_layout(self._plotly_args.get("layout", {}))
        return fig
