"""MLRun DataFrame dataset for Kedro."""
from typing import Any

from . import MLRunAbstractDataset


class MLRunDataframeDataset(MLRunAbstractDataset):
    """Dataset for saving/loading pandas DataFrames via MLRun.

    Uses MLRun's
    `log_artifact <https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_artifact>`_
    and
    `get_artifact <https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.get_artifact>`_.
    ``load_args`` and ``save_args`` accept any arguments supported by the corresponding
    MLRun API for your MLRun version; see the MLRun docs.

    Examples:
        Using the
        `YAML API <https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/>`_:

        .. code-block:: yaml

            user_data:
              type: kedro_datasets_experimental.mlrun.MLRunDataframeDataset
              key: generate-data-main_user_data

        Using the
        `Python API <https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/>`_:

        .. code-block:: python

            from kedro_datasets_experimental.mlrun import MLRunDataframeDataset
            import pandas as pd

            dataset = MLRunDataframeDataset(key="processed_df")
            dataset.save(pd.DataFrame({"a": [1, 2], "b": [3, 4]}))
            loaded_df = dataset.load()

    Args:
        key: Artifact key for MLRun (defaults to catalog dataset name).
        load_args: Passed to MLRun when loading; see MLRun docs for your version.
        save_args: Passed to log_artifact; see MLRun docs for your version.
    """

    def __init__(self,
                 key,
                 load_args: dict[str, Any] | None = None,
                 save_args: dict[str, Any] | None = None, ) -> None:
        super().__init__(key=key, save_args=save_args, load_args=load_args)

    def load(self):
        artifact = super().load()
        if not artifact:
            return None
        return artifact.to_dataitem().as_df()
