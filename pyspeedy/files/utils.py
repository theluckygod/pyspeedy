import os
from glob import glob
from beartype import beartype
from beartype.typing import Any, Union

import pandas as pd

from pyspeedy.files.file import File
from pyspeedy.files.file_factory import FileFactory


@beartype
def load_file_by_ext(fname: str, **kwargs) -> Any:
    """Load file by extension.

    Args:
        fname (str): File name.
        **kwargs: Keyword arguments.

    Returns:
        Any: The content of the loaded file. It can be a dictionary, a list, a pandas DataFrame, or a single object.
    """

    if not os.path.exists(fname):
        raise FileNotFoundError(f"File {fname} not found.")

    try:
        ext: str = fname.split(".")[-1]
    except IndexError:
        raise ValueError(f"File {fname} does not have an extension.")

    f: File = FileFactory().create(ext=ext)
    return f.read(path=fname, **kwargs)


def mergeable(files: list[any]) -> bool:
    return (
        all([isinstance(f, dict) for f in files])
        or all([isinstance(f, list) for f in files])
        or all([isinstance(f, pd.DataFrame) for f in files])
    )


@beartype
def load_by_ext(fname: Union[str, list], try_to_merge: bool = False, **kwargs) -> Any:
    """Loads one or multiple files based on their extensions.

    Args:
        fname (Union[str, list]): The name or list of names of the files to be loaded. If a list is given, all files in the list will be loaded.
        Examples:
            - ["file1.txt", "file2.txt"]
            - "file.txt"
            - "data/*.csv"

        try_to_merge (bool): If True, tries to merge the loaded files into a single object. Default is False.
        **kwargs: Additional keyword arguments.

    Returns:
        Any: The content of the loaded file(s). It can be a dictionary, a list, a pandas DataFrame, or a single object.
        Usually, the return the dictionary of the file name and its content.
    """

    if isinstance(fname, str):
        if "*" in fname:
            files = glob(fname)
            return load_by_ext(fname=files, **kwargs)

        return load_file_by_ext(fname=fname, **kwargs)

    if isinstance(fname, list):
        files = [load_file_by_ext(fname=f, **kwargs) for f in fname]

        if try_to_merge:
            assert mergeable(files), "Files are not mergeable."

            if all([isinstance(f, dict) for f in files]):
                return {k: v for f in files for k, v in f.items()}
            elif all([isinstance(f, list) for f in files]):
                return [item for sublist in files for item in sublist]
            elif all([isinstance(f, pd.DataFrame) for f in files]):
                return pd.concat(files, axis=0, ignore_index=True, sort=False)

        return {f: file for f, file in zip(fname, files)}
