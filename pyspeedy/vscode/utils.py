import os
import shutil

import pkg_resources
from beartype import beartype
from loguru import logger


@beartype
def dump_settings(
    destination_path: str = ".vscode",
    to_overwrite: bool = False,
    test_mode: bool = False,
):
    """Copy the well-configured .vscode folder to the specified destination path.

    Args:
        destination_path (str): The path where the .vscode folder will be copied. Defaults to ".vscode".
        to_overwrite (bool): If True, overwrites the destination path if it already exists. Defaults to False.
        test_mode (bool): If True, the function will not copy the .vscode folder. Defaults to False.
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
    )  # pyspeedy/pyspeedy/vscode
    repo_path: str = os.path.dirname(os.path.dirname(package_path))  # pyspeedy/pyspeedy
    source_path: str = os.path.join(repo_path, ".vscode")  # pyspeedy/.vscode
    assert os.path.exists(
        source_path
    ), f"Source path '{source_path}' does not exist. Make sure the .vscode folder is in the root of the repository."

    if test_mode:
        logger.warning(
            f"Test mode is on. Would copy '{source_path}' to '{destination_path}'."
        )
        return

    logger.info(f"Copying '{source_path}' to '{destination_path}'...")
    shutil.copytree(source_path, destination_path, dirs_exist_ok=True)
    logger.info("Done.")
