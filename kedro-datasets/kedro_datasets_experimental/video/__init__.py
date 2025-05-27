"""Dataset implementation to load/save data from/to a video file."""

from typing import Any

try:
    from .video_dataset import VideoDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    VideoDataset: Any

import lazy_loader as lazy

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"video_dataset": ["VideoDataset"]}
)
