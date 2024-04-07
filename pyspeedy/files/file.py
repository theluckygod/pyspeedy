from abc import ABC, abstractmethod
from beartype.typing import Any


class File(ABC):
    """
    The Abstract File interface declares the operations that all concrete
    files must implement.
    """

    @abstractmethod
    def read(self) -> str:
        pass

    @abstractmethod
    def write(self, data: Any, path: str) -> None:
        pass
