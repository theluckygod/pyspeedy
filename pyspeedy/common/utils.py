import os
import shutil

import pkg_resources
from beartype import beartype

from pyspeedy.common.logging import logger


@beartype
def dump_template(
    source_path: str,
    destination_path: str,
    to_overwrite: bool = False,
    test_mode: bool = False,
):
    """Copy the template folder to the specified destination path.

    Args:
        source_path (str): The directory path of the template, relative to the repository root.
        destination_path (str): The path where the tests folder will be copied. Defaults to "tests".
        to_overwrite (bool): If True, overwrites the destination path if it already exists. Defaults to False.
        test_mode (bool): If True, the function will not copy the tests folder. Defaults to False.
    """

    if os.path.exists(destination_path):
        if not to_overwrite:
            raise FileExistsError(
                f"'{destination_path}' already exists. Use 'to_overwrite=True' to overwrite it."
            )
        logger.warning(f"Overwriting '{destination_path}'...")
        shutil.rmtree(destination_path)

    package_path: str = pkg_resources.resource_filename(
        __name__, ""
    )  # pyspeedy/pyspeedy/common
    repo_path: str = os.path.dirname(os.path.dirname(package_path))  # pyspeedy/
    source_path: str = os.path.join(repo_path, source_path)  # pyspeedy/ + $source_path
    assert os.path.exists(source_path), f"Source path '{source_path}' does not exist."

    if test_mode:
        logger.warning(
            f"Test mode is on. Would copy '{source_path}' to '{destination_path}'."
        )
        return

    logger.info(f"Copying '{source_path}' to '{destination_path}'...")
    shutil.copytree(source_path, destination_path, dirs_exist_ok=True)
    logger.info("Done.")


# https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook
def is_notebook() -> bool:
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter
