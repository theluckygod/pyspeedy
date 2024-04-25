import datetime
import os
from glob import glob

import dateutil
import pandas as pd
from beartype import beartype
from beartype.typing import Any, Dict, List, Union
from loguru import logger

from pyspeedy.files.file_concrete.file import File
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


def mergeable(files: List[any]) -> bool:
    return (
        all([isinstance(f, dict) for f in files])
        or all([isinstance(f, list) for f in files])
        or all([isinstance(f, pd.DataFrame) for f in files])
    )


@beartype
def load_by_ext(fname: Union[str, List], try_to_merge: bool = False, **kwargs) -> Any:
    """Loads one or multiple files based on their extensions.

    Args:
        fname (Union[str, List]): The name or list of names of the files to be loaded. If a list is given, all files in the list will be loaded.
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
            logger.info(f"Found {len(files)} files in {fname}. Loading...")
            return load_by_ext(fname=files, try_to_merge=try_to_merge, **kwargs)

        return load_file_by_ext(fname=fname, **kwargs)

    if isinstance(fname, list):
        files = [load_file_by_ext(fname=f, **kwargs) for f in fname]

        if try_to_merge:
            assert mergeable(files), "Files are not mergeable."

            if all([isinstance(f, dict) for f in files]):
                logger.info("Merging all files into a single dictionary.")
                return {k: v for f in files for k, v in f.items()}
            elif all([isinstance(f, list) for f in files]):
                logger.info("Merging all files into a single list.")
                return [item for sublist in files for item in sublist]
            elif all([isinstance(f, pd.DataFrame) for f in files]):
                logger.info("Merging all files into a single DataFrame.")
                return pd.concat(files, axis=0, ignore_index=True, sort=False)
            else:
                raise ValueError("Files are not mergeable.")

        return {f: file for f, file in zip(fname, files)}


@beartype
def write_by_ext(
    data: Union[pd.DataFrame, List, Dict],
    fname: str,
    to_makedir: bool = True,
    to_overwrite: bool = False,
    to_add_date_tag: bool = False,
    tag_date_format: str = "_v%y.%m.%d",
    tz: str = "Asia/Ho_Chi_Minh",
    **kwargs,
) -> str:
    """Writes data to a file based on its extension.

    Args:
        data (Union[pd.DataFrame, list, dict]): The data to be written.
        fname (str): The name of the file to be written.
        to_makedir (bool): If True, creates the folder if it does not exist. Default is True.
        to_overwrite (bool): If True, overwrites the file if it already exists. Default is False.
        to_add_date_tag (bool): If True, adds the current date to the file name. Default is False.
        tag_date_format (str): The format of the date tag. Default is "_v%y.%m.%d". Only used if to_add_date_tag is True.
        tz (str): The time zone to be used. Default is "Asia/Ho_Chi_Minh". Only used if to_add_date_tag is True.
        **kwargs: Additional keyword arguments.

    Returns:
        str: The path of the written file.
    """

    folder = os.path.dirname(fname)

    try:
        ext: str = fname.split(".")[-1]
    except IndexError:
        raise ValueError(f"File {fname} does not have an extension.")

    if folder and not os.path.exists(folder):
        if not to_makedir:
            raise FileNotFoundError(f"Folder {folder} not found.")
        logger.warning(f"Folder {folder} not found. Creating...")
        os.makedirs(folder, exist_ok=True)

    if to_add_date_tag:
        tag_date = datetime.datetime.now(tz=dateutil.tz.gettz(tz)).strftime(
            tag_date_format
        )
        fname = f"{fname[: -len(ext) - 1]}{tag_date}.{ext}"

    if os.path.exists(fname):
        if not to_overwrite:
            raise FileExistsError(
                f"File {fname} already exists. Use 'to_overwrite=True' to overwrite it."
            )
        logger.warning(f"Overwriting {fname}...")

    f: File = FileFactory().create(ext=ext)
    f.write(data=data, path=fname, **kwargs)

    return fname
