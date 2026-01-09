"""MLRun result dataset for Kedro."""
from typing import Any

from .abstract_artifact import MLRunAbstractDataset


class MLRunResult(MLRunAbstractDataset):
    """Dataset for saving/loading results via MLRun."""

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
            flat = self._flatten_dict(data)
            for k, v in flat.items():
                self._ctx_manager.context.log_result(k, v)
        else:
            self._ctx_manager.context.log_result(self._key, data)

    def load(self) -> Any:
        return self._ctx_manager.context.results.get(self._key)
