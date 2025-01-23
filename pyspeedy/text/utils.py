import json
import re
from json import JSONDecodeError

from beartype.typing import Dict, Union

import pyspeedy.text.constants as constants
import pyspeedy.text.helper_utils as helper_utils


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


def json_loads_corrector(text: str, return_dict: bool = True) -> Union[Dict, str]:
    """Correct the string that can not be loaded by json.loads

    Args:
        text (str): text to be corrected
        return_dict (bool, optional): return dict or not. Defaults to True.

    Returns:
        Union[Dict, str]: dict or corrected text
    """

    dict_obj = None
    error = None

    if helper_utils._is_python_dict_format(text):
        try:
            dict_obj = eval(text)
        except Exception as e:
            error = e

    if dict_obj is None:
        try:
            dict_obj = json.loads(
                helper_utils._json_loads_format_console_print(text), strict=False
            )
        except JSONDecodeError as e:
            error = e

    if dict_obj is None:
        try:
            dict_obj = json.loads(
                helper_utils._json_loads_format_excel(text), strict=False
            )
        except JSONDecodeError as e:
            error = e

    if dict_obj is None:
        raise error

    if not return_dict:
        return json.dumps(dict_obj, ensure_ascii=False)

    return dict_obj
