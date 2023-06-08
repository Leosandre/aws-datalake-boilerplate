from typing import (
    Any,
    Dict,
)


class StackCache:
    def __init__(self) -> None:
        self._cache: Dict[str, Any] = {}

    def add(self, name: str, resource: Any) -> None:
        self._cache[name] = resource

    def get(self, name: str) -> Any:
        return self._cache[name]
