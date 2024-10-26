import os


def parse_glob_pattern(pattern: str) -> str:
    special = ("*", "?", "[")
    clean = []
    for part in pattern.split("/"):
        if any(char in part for char in special):
            break
        clean.append(part)
    return "/".join(clean)


def split_filepath(filepath: str | os.PathLike) -> tuple[str, str]:
    split_ = str(filepath).split("://", 1)
    if len(split_) == 2:  # noqa: PLR2004
        return split_[0] + "://", split_[1]
    return "", split_[0]