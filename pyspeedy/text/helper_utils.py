import re

import pyspeedy.text.constants as constants
from beartype.typing import Dict


def _correct_outside_quotes(text: str, replace_dict: Dict[str, str]) -> str:
    """Correct the text outside quotes

    Args:
        text (str): text to be corrected
        replace_dict (dict[str, str]): replace dictionary

    Returns:
        str: corrected text
    """

    for k, v in replace_dict.items():
        p = re.compile(rf"(?<!\\\\){k}")
        text = p.sub(v, text)

    return text


def _json_loads_format_console_print(text: str) -> str:
    """Format the text to be printed in console"""
    return _correct_outside_quotes(text, constants.JSON_REPLACE_DICT_CONSOLE_PRINT)


def _json_loads_format_excel(text: str) -> str:
    """Format the text to be printed in excel"""
    return _correct_outside_quotes(text, constants.JSON_REPLACE_DICT_EXCEL)


def _is_python_dict_format(text: str) -> bool:
    return "\\\\n" not in text and '\\"' not in text
