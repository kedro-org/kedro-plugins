from typing import Any

from . import MLRunAbstractDataset


class MLRunDataframeDataset(MLRunAbstractDataset):

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
