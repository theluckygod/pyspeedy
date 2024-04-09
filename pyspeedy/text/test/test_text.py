from pyspeedy.text import *


def test_teen_code_decode():
    assert (
        teen_code_decode("a trả e bn tiền trà sữa, 15k được k?")
        == "anh trả em bao nhiêu tiền trà sữa, 15k được không?"
    )
