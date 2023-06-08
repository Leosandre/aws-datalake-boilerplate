from dataclasses import dataclass
from re import sub

from builder.model.property.environment import Environment


@dataclass
class Name:
    name: str
    env: Environment

    def __post_init__(self) -> None:
        if not isinstance(self.env, Environment):
            raise ValueError("Invalid environment")

    def _kebab(self) -> None:
        self.name = sub("(.)([A-Z][a-z]+)", r"\1_\2", self.name)
        self.name = sub("([a-z0-9])([A-Z])", r"\1_\2", self.name)
        self.name = self.name.replace("_", "-")
        self.name = self.name.replace(" ", "-")
        self.name = self.name.lower()

    def add_prefix(self, prefix: str) -> "Name":
        return Name(f"{prefix}-{self.name}", self.env)

    def add_suffix(self, suffix: str) -> "Name":
        return Name(f"{self.name}-{suffix}", self.env)

    @property
    def value(self) -> str:
        self._kebab()
        return f"{self.name}-{self.env.value}"
