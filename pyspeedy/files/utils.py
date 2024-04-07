from beartype.typing import Any

from pyspeedy.files.file import File
from pyspeedy.files.file_factory import FileFactory


def load_file_by_ext(fname: str, **kwargs) -> Any:
    try:
        ext: str = fname.split(".")[-1]
    except IndexError:
        raise ValueError(f"File {fname} does not have an extension.")

    f: File = FileFactory().create(ext=ext)
    return f.read(path=fname, **kwargs)
