from abc import ABC, abstractmethod


class File(ABC):
    """
    The Abstract File interface declares the operations that all concrete
    files must implement.
    """

    @abstractmethod
    def read(self) -> str:
        pass

    @abstractmethod
    def write(self, path: str) -> None:
        pass
