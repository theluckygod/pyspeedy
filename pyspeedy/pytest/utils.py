import pyspeedy.common.utils as utils


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

    return utils.dump_template(
        source_path="templates/pytest",
        destination_path=destination_path,
        to_overwrite=to_overwrite,
        test_mode=test_mode,
    )
