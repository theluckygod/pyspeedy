from beartype import beartype
from beartype.typing import Callable
from common_package.process_logs.commands import WAITING_RESULT, Command, Result
from loguru import logger


class ProcessLogsCommand(Command):
    """
    ProcessLogsCommand is a Command that will be used to process logs.
    This command always add WAITING_RESULT as the first argument.
    """

    @beartype
    def __init__(self, func: Callable, *args, **kwargs) -> None:
        super().__init__(func, *([WAITING_RESULT] + list(args)), **kwargs)
