from pyspeedy.files import *


class FileFactory:
    """
    The Abstract Factory interface declares a set of methods that return
    different abstract files.
    """

    _handlers = {
        "csv": CSV(),
        "json": JSON(),
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
