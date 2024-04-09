from __future__ import annotations

import traceback
from enum import Enum
from typing import Optional

from beartype import beartype
from beartype.typing import Callable, Dict, List, Tuple, Union
from loguru import logger


class State(Enum):
    INIT = 0
    RUNNING = 1
    DONE = 2
    ERROR = 3
    WAITING = 4


class Result:
    @beartype
    def __init__(
        self, state: State = State.INIT, result=None, traceback: Optional[str] = None
    ) -> None:
        self.state = state
        self.result = result
        self.traceback = traceback

    def __str__(self) -> str:
        return f"Result(state={self.state}, result={self.result}, traceback={self.traceback})"

    def __repr__(self):
        return str(self)

    def __eq__(self, other: Result) -> bool:
        if not isinstance(other, Result):
            return False

        return (
            self.state == other.state
            and self.result == other.result
            and self.traceback == other.traceback
        )

    def error(self):
        self.state = State.ERROR

    def running(self):
        self.state = State.RUNNING

    def done(self):
        self.state = State.DONE

    def is_done(self):
        return self.state == State.DONE

    def is_error(self):
        return self.state == State.ERROR

    def is_init(self):
        return self.state == State.INIT

    def is_running(self):
        return self.state == State.RUNNING


class Command:
    @beartype
    def __init__(self, func: Callable, *args, **kwargs) -> None:
        if func.__name__ == "<lambda>":
            logger.warn(
                f"Command function is lambda: {func}, that may cause problem when merge with other Task"
            )

        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self) -> Result:
        logger.debug(f"Run command: {self}")
        try:
            result = None
            result = self.func(*self.args, **self.kwargs)
            return Result(State.DONE, result, None)
        except Exception as e:
            logger.error(f"Run command failure: {self}")
            traceback_str = "".join(
                traceback.format_exception(type(e), e, e.__traceback__)
            )
            logger.error(traceback_str)
            return Result(State.ERROR, None, traceback_str)

    def __str__(self) -> str:
        return f"Command(func={self.func}, args={self.args}, kwargs={self.kwargs})"

    def __repr__(self):
        return str(self)

    def __eq__(self, other: Command) -> bool:
        if not isinstance(other, Command):
            return False

        return (
            self.func == other.func
            and self.args == other.args
            and self.kwargs == other.kwargs
        )


WAITING_RESULT = Result(State.WAITING, None, "Waiting for upstream node result...")


if __name__ == "__main__":
    cmd1 = Command(lambda text: print(text), text="test keyword params")
    cmd2 = Command(print, "test arg params")
    cmd1.run()
    cmd2.run()
