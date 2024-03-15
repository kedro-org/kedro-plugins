from typing import Any
from kedro.io import AbstractDataset
from transformers import AutoTokenizer, AutoModel

import logging
import importlib

from collections import namedtuple


TransformerModel = namedtuple("TransformerModel", ["model", "tokenizer"])


logger = logging.getLogger(__file__)


class HFTransformer(AbstractDataset):
    def __init__(
        self,
        checkpoint: str,
        model_type: str = None,
        tokenizer_kwargs: dict = None,
        model_kwargs: dict = None,
    ):
        self.checkpoint = checkpoint

        if model_type is not None:
            try:
                self.model = importlib.import_module(model_type, package='transformers')
            except ImportError as e:
                logger.info(
                    f"Given model type={model_type} doesn't exist in transformers"
                )
                raise e
        else:
            self.model = AutoModel

        self.tokenizer_kwargs = tokenizer_kwargs
        self.model_kwargs = model_kwargs

    def _load(self) -> TransformerModel:
        model = self.model.from_pretrained(self.checkpoint, **self.model_kwargs)
        tokenizer = AutoTokenizer.from_pretrained(self.checkpoint, **self.tokenizer_kwargs)

        return TransformerModel(model=model, tokenizer=tokenizer)

    def _save(self, data) -> None:
        raise NotImplementedError("Pretrained models don't support saving for now")

    def _describe(self) -> dict[str, Any]:
        return {
            "checkpoint": self.checkpoint,
            "model_type": self.model,
            "tokenizer_kwargs": self.tokenizer_kwargs,
            "model_kwargs": self.model_kwargs,
        }