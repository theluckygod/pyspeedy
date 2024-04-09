from __future__ import annotations

import pandas as pd
from beartype import beartype
from beartype.typing import List, Optional, Tuple, Union
from common_package.process_logs.commands import *
from common_package.process_logs.process_node import ProccessNode, RootProccessNode
from loguru import logger

from pyspeedy.patterns.observer import Observer, Subject


class Task(Observer):

    @beartype
    def __init__(
        self,
        anotherTask: Optional["Task"] = None,
        command: Optional["Command"] = None,
        result: Optional["Result"] = None,
        failure_handler: Optional["Command"] = None,
    ):
        if isinstance(anotherTask, Task):
            self.commands = anotherTask.commands
            self.result = anotherTask.result
            self.failure_handler = anotherTask.failure_handler
            return

        self.commands = list()
        if isinstance(command, Command):
            self.commands.append(command)

        self.result = Result()
        if isinstance(result, Result):
            self.result = result

        self.failure_handler = None
        if isinstance(failure_handler, Command):
            self.failure_handler = failure_handler

    def is_done(self) -> bool:
        return self.result.is_done()

    def is_error(self) -> bool:
        return self.result.is_error()

    def __str__(self) -> str:
        return (
            f"Task(commands={self.commands}, "
            f"result={self.result}, "
            f"failure_handler={self.failure_handler})"
        )

    def __repr__(self):
        return str(self)

    def values(self):
        if self.is_done():
            return self.result
        elif self.is_error():
            raise Exception(self.result.traceback)
        elif self.result.is_init():
            raise Exception(
                "You must add Task to Kernel and call Kernel().run() first!!!"
            )
        elif self.result.is_running():
            raise Exception(
                "I dont know why this Task is still running! Help me check it please!!!"
            )

    @beartype
    def update(self, subject: Subject) -> None:
        self.update_result(subject.result)

    @beartype
    def update_result(self, result: Result) -> None:
        self.result = result

        if self.is_error() and self.failure_handler is not None:
            logger.debug(
                f"Task {self} is error. Trying to handle with error FailureHandler..."
            )
            self.result = self.failure_handler.run()

    @beartype
    def add_command(self, func: Union[Callable, Command], *args, **kwargs):
        if isinstance(func, Command):
            assert (
                len(args) == 0 and len(kwargs) == 0
            ), "You must not pass any args or kwargs when func is Command"
            command = func
        else:
            command = Command(func, *args, **kwargs)
        self.commands.append(command)
        return self

    @beartype
    def add_commands(self, command_lst: List[Command]):
        self.commands = self.commands + command_lst
        return self

    @beartype
    def append_task(self, task: Task) -> Task:
        self.commands.extend(task.commands)
        return self

    @beartype
    def set_failure_handler(self, func: Union[Callable, Command], *args, **kwargs):
        if isinstance(func, Command):
            assert (
                len(args) == 0 and len(kwargs) == 0
            ), "You must not pass any args or kwargs when func is Command"
            command = func
        else:
            command = Command(func, *args, **kwargs)
        self.failure_handler = command
        return self

    def build_process_node(self) -> ProccessNode:
        head = RootProccessNode()
        tail = head
        for idx, command in enumerate(self.commands):
            observer = None
            if idx == len(self.commands) - 1:
                observer = self
            node = ProccessNode(command, observer=observer)

            tail.next.append(node)
            node.rank = tail.rank + 1

            tail = node
        return head

    def run(self) -> Result:
        self.build_process_node().run()
        return self.result


if __name__ == "__main__":
    from common_package.process_logs.process_logs_command import ProcessLogsCommand
    from common_package.process_logs_utils import filt_by_intents, filt_by_logmode

    file_path = "crawl_daily_logs/outputs/logs_date_2023-02-26.csv"
    task = (
        Task()
        .add_command(pd.read_csv, file_path)
        .add_command(filt_by_intents, WAITING_RESULT, "music.ask_music.ask_play_lyrics")
        .add_command(ProcessLogsCommand(filt_by_logmode, "lyrics_from_zmp3"))
        .add_command(Command(print, "Done!"))
    )
    result = task.run()
    print("result", result)

    # test failure handler
    task2 = (
        Task()
        .add_command(pd.read_csv, "no_file_path")
        .add_command(filt_by_intents, WAITING_RESULT, "music.ask_music.ask_play_lyrics")
        .add_command(ProcessLogsCommand(filt_by_logmode, "lyrics_from_zmp3"))
        .add_command(Command(print, "Done!"))
        .set_failure_handler(Command(print, "Error!"))
    )
    result2 = task2.run()
    print("result2", result2)
