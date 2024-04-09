from beartype import beartype
from beartype.typing import Callable, Dict, List, Optional, Union


def dummy_result_func(result=None):
    return result


def wrapped_print(text: str, *args, **kwargs):
    print(text.format(*args))

    return kwargs.get("keep_result", None)


if __name__ == "__main__":
    wrapped_print("example: {} {}", ["arg1", "arg2"], {"keep_result": 100})
