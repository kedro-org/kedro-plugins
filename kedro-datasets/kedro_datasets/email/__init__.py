"""``AbstractDataset`` implementations for managing email messages."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
EmailMessageDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"message_dataset": ["EmailMessageDataset"]},
)
