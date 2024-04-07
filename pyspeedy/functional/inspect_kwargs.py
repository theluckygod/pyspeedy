import inspect
import functools

from beartype import beartype
from beartype.typing import Callable


@beartype
def ignore_unmatched_kwargs(f: Callable) -> Callable:
    """Make function ignore unmatched kwargs. If the function already has the catch all **kwargs, do nothing.

    Args:
        f (Callable): The function to be wrapped.

    Returns:
        Callable: The wrapped function.
    """
    if contains_var_kwarg(f):
        return f

    @functools.wraps(f)
    def inner(*args, **kwargs):
        filtered_kwargs = {
            key: value for key, value in kwargs.items() if is_kwarg_of(key, f)
        }
        return f(*args, **filtered_kwargs)

    return inner


@beartype
def contains_var_kwarg(f: Callable) -> bool:
    """Check if function f contains a catch all **kwargs.

    Args:
        f (Callable): The function to check.

    Returns:
        bool: True if f contains a catch all **kwargs, False otherwise.
    """

    return any(
        param.kind == inspect.Parameter.VAR_KEYWORD
        for param in inspect.signature(f).parameters.values()
    )


@beartype
def is_kwarg_of(key: str, f: Callable) -> bool:
    """Check if key is a kwarg of function f.

    Args:
        key (str): The key to check.
        f (Callable): The function to check.

    Returns:
        bool: True if key is a kwarg of f, False otherwise.
    """

    param = inspect.signature(f).parameters.get(key, False)
    return param and (
        param.kind is inspect.Parameter.KEYWORD_ONLY
        or param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    )
