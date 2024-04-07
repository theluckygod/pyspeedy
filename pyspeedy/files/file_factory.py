from pyspeedy.files.file import File
from pyspeedy.files.csv import CSV


class FileFactory:
    """
    The Abstract Factory interface declares a set of methods that return
    different abstract files.
    """

    def create(self, ext: str) -> File:
        """
        The Abstract Factory interface declares a set of methods that return
        different abstract files.
        """
        if ext == "csv":
            return CSV()

        raise ValueError(f"Unknown file extension: {ext}")
