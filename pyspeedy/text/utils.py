import json
import re
from json import JSONDecodeError

from beartype.typing import Dict, Union

import pyspeedy.text.constants as constants
from pyspeedy.common.logging import logger


def teen_code_decode(
    text: str, teen_code_dict: Dict[str, str] = constants.TEEN_CODE_DICT
) -> str:
    """Replace teen codes with formal ones

    Args:
        text (str): text to be decoded
        teen_code_dict (dict[str, str], optional): teen code dictionary. Defaults to constants.TEEN_CODE_DICT.

    Returns:
        str: decoded text
    """

    rep_ = dict((r"\b{}\b".format(k), v) for k, v in teen_code_dict.items())
    pattern = re.compile("|".join(rep_.keys()), flags=re.I)

    result = pattern.sub(lambda m: teen_code_dict[re.escape(m.group(0)).lower()], text)

    return result


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


def json_loads_corrector(text: str, return_dict: bool = True) -> Union[Dict, str]:
    """Correct the string that can not be loaded by json.loads

    Args:
        text (str): text to be corrected
        return_dict (bool, optional): return dict or not. Defaults to True.

    Returns:
        Union[Dict, str]: dict or corrected text
    """

    dict_obj = None

    try:
        eval_text = re.sub(r"\\*n", r"\\n", text)
        dict_obj = eval(eval_text)
        logger.info("Corrected the text with eval method")
    except Exception as e:
        logger.warning("Failed to correct the text with eval method")

    if dict_obj is None:
        try:
            dict_obj = json.loads(_json_loads_format_console_print(text), strict=False)
            logger.info("Corrected the text with console print format")
        except JSONDecodeError:
            logger.warning("Failed to correct the text with console print format")

    if dict_obj is None:
        try:
            dict_obj = json.loads(_json_loads_format_excel(text), strict=False)
            logger.info("Corrected the text with excel format")
        except JSONDecodeError:
            logger.warning("Failed to correct the text with excel format")

    if dict_obj is None:
        raise JSONDecodeError("Failed to correct the text")

    if not return_dict:
        return json.dumps(text, ensure_ascii=False)

    return dict_obj
