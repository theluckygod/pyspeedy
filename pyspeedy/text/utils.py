import re

import pyspeedy.text.constants as constants


def teen_code_decode(text: str) -> str:
    """Replace teen codes with formal ones

    Args:
        text (str): text to be decoded

    Returns:
        str: decoded text
    """

    rep_ = dict((r"\b{}\b".format(k), v) for k, v in constants.TEEN_CODE_DICT.items())
    pattern = re.compile("|".join(rep_.keys()), flags=re.I)

    result = pattern.sub(
        lambda m: constants.TEEN_CODE_DICT[re.escape(m.group(0)).lower()], text
    )

    return result
