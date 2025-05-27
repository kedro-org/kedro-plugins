__schema__ = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "requires": {
            "$ref": "http://json-schema.org/draft-07/schema#/definitions/stringArray"
        },
        "build-backend": {
            "type": "string",
            "pattern": r"^(\w[\w\d]*)(\.\w[\w\d]*)*(:(\w[\w\d]*)(\.\w[\w\d]*))?$",
        },
        "backend-path": {
            "$ref": "http://json-schema.org/draft-07/schema#/definitions/stringArray"
        },
    },
    "required": ["requires"],
}
