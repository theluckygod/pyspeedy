import os
import shutil

import pkg_resources
from beartype import beartype
from loguru import logger


@beartype
def dump_template(
    destination_path: str = "tests",
    to_overwrite: bool = False,
    test_mode: bool = False,
):
    """Copy the pytest template folder to the specified destination path.

    Args:
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
        __name__, "template"
    )  # pyspeedy/pyspeedy/pytest/template
    assert os.path.exists(package_path), f"Source path '{package_path}' does not exist."

    if test_mode:
        logger.warning(
            f"Test mode is on. Would copy '{package_path}' to '{destination_path}'."
        )
        return

    logger.info(f"Copying '{package_path}' to '{destination_path}'...")
    shutil.copytree(package_path, destination_path, dirs_exist_ok=True)
    logger.info("Done.")
