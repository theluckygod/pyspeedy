import re

from beartype.typing import Dict

import pyspeedy.text.constants as constants


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


def json_loads_corrector(text: str) -> str:
    """Correct the string that can not be loaded by json.loads

    Args:
        text (str): text to be corrected

    Returns:
        str: corrected text
    """

    return text.replace("\\n", "n").replace('\\"', '"').replace('\\"', '"')
