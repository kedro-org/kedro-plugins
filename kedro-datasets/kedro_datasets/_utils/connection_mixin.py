from collections.abc import Hashable
from typing import Any, ClassVar


class ConnectionMixin:
    _connections: ClassVar[dict[Hashable, Any]] = {}

    @property
    def _connection(self) -> Any:
        def hashable(value: Any) -> Hashable:
            """Return a hashable key for a potentially-nested object."""
            if isinstance(value, dict):
                return tuple((k, hashable(v)) for k, v in sorted(value.items()))
            if isinstance(value, list):
                return tuple(hashable(x) for x in value)
            return value

        cls = type(self)
        key = self._CONNECTION_GROUP, hashable(self._connection_config)
        if key not in cls._connections:
            cls._connections[key] = self._connect()

        return cls._connections[key]
