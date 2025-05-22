__schema__ = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "patter": r"^([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9])$",
        },
        "version": {"type": "string"},
        "description": {"type": "string"},
        "readme": {
            "anyOf": [
                {"type": "string"},
                {
                    "type": "object",
                    "properties": {"file": {"type": "string"}},
                    "required": ["file"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"],
                    "additionalProperties": False,
                },
            ]
        },
        "requires-python": {"type": "string"},
        "license": {
            "anyOf": [
                {
                    "type": "object",
                    "properties": {"file": {"type": "string"}},
                    "required": ["file"],
                    "additionalProperties": False,
                },
                {
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"],
                    "additionalProperties": False,
                },
            ]
        },
        "authors": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                },
            },
        },
        "maintainers": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                },
            },
        },
        "keywords": {
            "$ref": "http://json-schema.org/draft-07/schema#/definitions/stringArray"
        },
        "classifiers": {
            "$ref": "http://json-schema.org/draft-07/schema#/definitions/stringArray"
        },
        "urls": {"type": "object"},
        "scripts": {"type": "object"},
        "gui-scripts": {"type": "object"},
        "entry-points": {"type": "object"},
        "dependencies": {
            "$ref": "http://json-schema.org/draft-07/schema#/definitions/stringArray"
        },
        "optional-dependencies": {
            "type": "object",
            "patternProperties": {
                ".+": {
                    "$ref": "http://json-schema.org/draft-07/schema#/definitions/stringArray"
                }
            },
        },
        "dynamic": {
            "$ref": "http://json-schema.org/draft-07/schema#/definitions/stringArray"
        },
    },
    "required": ["name", "version"],
}
