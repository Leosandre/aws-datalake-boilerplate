from dataclasses import (
    dataclass,
    field,
)
from typing import (
    List,
    Tuple,
)


@dataclass
class Tags:
    items: List[Tuple[str, str]] = field(default_factory=list)

    @staticmethod
    def __kebab(value: str) -> str:
        value = value.replace("_", "-")
        value = value.replace(" ", "-")
        value = value.lower()
        return value

    def add(self, key: str, value: str) -> None:
        key = self.__kebab(key)
        self.items.append((key, value))
