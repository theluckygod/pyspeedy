import pyspeedy.common.utils as utils


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

    return utils.dump_template(
        source_path=".vscode",
        destination_path=destination_path,
        to_overwrite=to_overwrite,
        test_mode=test_mode,
    )
