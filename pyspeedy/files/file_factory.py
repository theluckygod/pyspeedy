from pyspeedy.files.file_concrete.csv import CSV
from pyspeedy.files.file_concrete.file import File
from pyspeedy.files.file_concrete.json import JSON, JSONL
from pyspeedy.files.file_concrete.txt import TXT
from pyspeedy.files.file_concrete.xlsx import XLSX


class FileFactory:
    """
    The Abstract Factory interface declares a set of methods that return
    different abstract files.
    """

    _handlers = {
        "csv": CSV(),
        "xlsx": XLSX(),
        "json": JSON(),
        "jsonl": JSONL(),
        "txt": TXT(),
        "log": TXT(),
    }

    def create(self, ext: str) -> File:
        """
        The Abstract Factory interface declares a set of methods that return
        different abstract files.
        """
        handler = self._handlers.get(ext, None)
        if not handler:
            raise ValueError(f"Unknown file extension: {ext}")

        return handler
