"""MLRun result dataset for Kedro."""
from typing import Any

from .abstract_artifact import MLRunAbstractDataset


class MLRunResult(MLRunAbstractDataset):
    """Dataset for saving/loading scalar results (metrics) via MLRun.

    Uses MLRun's
    `log_result <https://docs.mlrun.org/en/latest/api/mlrun.execution/index.html#mlrun.execution.MLClientCtx.log_result>`_;
    results are read from context.results.
    ``load_args`` and ``save_args`` accept any arguments supported by the corresponding
    MLRun API for your MLRun version; see the MLRun docs.

    Examples:
        Using the
        `YAML API <https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/>`_:

        .. code-block:: yaml

            training_metrics:
              type: kedro_datasets_experimental.mlrun.MLRunResult
              key: metrics
              flatten: true

        Using the
        `Python API <https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/>`_:

        .. code-block:: python

            from kedro_datasets_experimental.mlrun import MLRunResult

            dataset = MLRunResult(key="metrics", flatten=True)
            dataset.save({"accuracy": 0.95, "loss": 0.05})
            loaded = dataset.load()

    Args:
        key: Result key for MLRun (defaults to catalog dataset name).
        flatten: If True, flatten nested dicts to dot-notation keys. When True,
            each key is stored as a separate MLRun result; load per key
            (e.g. from context.results for each key).
        load_args: Passed to MLRun when loading; see MLRun docs for your version.
        save_args: Passed to log_result; see MLRun docs for your version.
    """

    def __init__(
        self,
        key: str | None = None,
        flatten: bool = False,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(key=key, save_args=save_args, load_args=load_args)
        self._flatten = flatten

    def _flatten_dict(self, d: dict, prefix: str = "") -> dict:
        """Flatten nested dict into dot-notation keys."""
        items = {}
        for k, v in d.items():
            new_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                items.update(self._flatten_dict(v, new_key))
            else:
                items[new_key] = v
        return items

    def save(self, data: Any) -> None:
        if self._flatten and isinstance(data, dict):
            # Each flattened key is logged as its own MLRun result (no single key holds
            # the full dict). When loading, read each key from context.results.
            flat = self._flatten_dict(data)
            for k, v in flat.items():
                self._ctx_manager.context.log_result(k, v)
        else:
            self._ctx_manager.context.log_result(self.key, data)

    def load(self) -> Any:
        # When flatten=True, results were saved per key; use context.results per key.
        return self._ctx_manager.context.results.get(self.key)
