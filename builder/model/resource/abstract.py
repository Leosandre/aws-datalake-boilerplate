from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass

from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.utils.stack_cache import StackCache


@dataclass
class Resource(ABC):
    name: Name
    tags: Tags

    @staticmethod
    @abstractmethod
    def from_pydict(name: Name, tags: Tags, pydict: dict) -> "Resource":
        pass

    @abstractmethod
    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        pass
