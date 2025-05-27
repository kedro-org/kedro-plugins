from collections import defaultdict
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING

import setuptools
import toml
from jsonschema import validate

from . import build_system, project, tool

if TYPE_CHECKING:
    from typing import Dict, Literal

SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "build-system": build_system.__schema__,
        "project": project.__schema__,
        "tool": tool.__schema__,
    },
}

README_CONTENT_TYPES = {
    ".md": "text/markdown",
    ".rst": "text/x-rst",
    ".txt": "text/plain",
    "": "text/plain",
}


def format_author(author: "Dict[Literal['name', 'email'], str]"):
    if "email" in author:
        return (
            "{name} <{email}>".format(**author)
            if "name" in author
            else author["email"],
            "_email",
        )
    elif "name" in author:
        return author["name"], ""
    else:
        raise ValueError("")


def setup_decorator(origin_setup):
    @wraps(origin_setup)
    def new_setup(**attrs):
        pyproject_toml = Path("pyproject.toml")
        if pyproject_toml.exists():
            pyproject = toml.loads(pyproject_toml.read_text(encoding="utf-8"))
            validate(pyproject, SCHEMA)
            for k, v in pyproject.get("project", {}).items():
                attrs[k] = v
            for k, v in pyproject.get("tool", {}).get("pyproject-toml", {}).items():
                attrs[k] = v
        else:
            return origin_setup(**attrs)

        if "readme" in attrs:
            readme = attrs.pop("readme")
            if isinstance(readme, str):
                readme = Path(readme)
            elif "file" in readme:
                readme = Path(readme["file"])
            else:
                attrs["long_description"] = readme["text"]
            if isinstance(readme, Path):
                try:
                    attrs["long_description_content_type"] = README_CONTENT_TYPES[
                        readme.suffix
                    ]
                except KeyError:
                    raise TypeError("Content type of {} is not supported".format(readme))
                attrs["long_description"] = readme.read_text(encoding="utf-8")

        if "requires-python" in attrs:
            attrs["python_requires"] = attrs.pop("requires-python")

        if "license" in attrs:
            license_ = attrs.pop("license")
            if isinstance(license_, str):
                raise NotImplementedError(
                    "No PEP supports SPDX at 2021-03-07 when I write, see "
                    "https://www.python.org/dev/peps/pep-0621/#id88"
                )
            if "text" in license_:
                attrs["license"] = license_["text"]
            elif "file" in license_:
                attrs["license_file"] = license_["file"]

        authors = defaultdict(list)
        for type_, people in ("author", attrs.pop("authors", [])), (
            "maintainer",
            attrs.pop("maintainers", []),
        ):
            for person in people:
                value, postfix = format_author(person)
                authors[type_ + postfix].append(value)

        for k, v in authors.items():
            attrs[k] = ",".join(v)

        if "urls" in attrs:
            urls = attrs.pop("urls")
            attrs["url"] = urls.pop(
                "home-page", urls.pop("homepage", urls.pop("url", None))
            )
            attrs["download_url"] = urls.pop(
                "download-url", urls.pop("download_url", urls.pop("download", None))
            )
            attrs["project_urls"] = urls

        attrs["install_requires"] = attrs.pop("dependencies", None)

        origin_setup(**attrs)

    return new_setup


setup = setup_decorator(setuptools.setup)
