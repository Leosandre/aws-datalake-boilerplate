from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass


@dataclass
class Package(ABC):
    @abstractmethod
    def build(self) -> "Package":
        pass
