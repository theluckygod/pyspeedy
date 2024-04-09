import common_package.process_logs.utils as utils
from beartype import beartype
from beartype.typing import Callable, Optional, Union
from common_package.process_logs.commands import Command, Result
from common_package.process_logs.task import WAITING_RESULT, Task
from loguru import logger


class PostProcessingTask(Task):
    """
    PostProcessingTask is a Task that will be used to process logs after the main task.
    The post_commands will be executed after the main task (commands and failure_handler).
    The post_failure_handler will be executed on post_commands failure.
    """

    def __init__(
        self,
        anotherTask: Optional[Task] = None,
        command: Optional[Command] = None,
        result: Optional[Result] = None,
        failure_handler: Optional[Command] = None,
        post_command: Optional[Command] = None,
        post_failure_handler: Optional[Command] = None,
    ):
        super().__init__(anotherTask, command, result, failure_handler)

        if isinstance(anotherTask, PostProcessingTask):
            self.post_commands = anotherTask.post_commands
            self.post_failure_handler = anotherTask.post_failure_handler
            return

        self.post_commands = list()
        if isinstance(post_command, Command):
            self.post_commands.append(command)

        self.post_failure_handler = None
        if isinstance(post_failure_handler, Command):
            self.post_failure_handler = post_failure_handler

    @beartype
    def add_post_command(self, func: Union[Callable, Command], *args, **kwargs):
        if isinstance(func, Command):
            assert (
                len(args) == 0 and len(kwargs) == 0
            ), "You must not pass any args or kwargs when func is Command"
            command = func
        else:
            command = Command(func, *args, **kwargs)
        self.post_commands.append(command)
        return self

    @beartype
    def set_post_failure_handler(self, func: Union[Callable, Command], *args, **kwargs):
        if isinstance(func, Command):
            assert (
                len(args) == 0 and len(kwargs) == 0
            ), "You must not pass any args or kwargs when func is Command"
            command = func
        else:
            command = Command(func, *args, **kwargs)
        self.post_failure_handler = command
        return self

    @beartype
    def update_result_post_processing(self, result: Result) -> None:
        self.result = result

        if self.is_error() and self.post_failure_handler is not None:
            logger.debug(
                f"Task {self} is error. Trying to handle with error PostProcessingFailureHandler..."
            )
            self.result = self.post_failure_handler.run()

    @staticmethod
    @beartype
    def __create_dummy_task(result=None) -> Task:
        """create_dummy_task creates a dummy task with first command is return result
        Args:
            result (any):
        Returns:
            Task
        """
        return Task().add_command(utils.dummy_result_func, result)

    @beartype
    def update_result(self, result: Result) -> None:
        super().update_result(result)

        if self.post_commands and self.is_done():
            post_task = PostProcessingTask.__create_dummy_task(
                self.result.result
            ).add_commands(self.post_commands)
            post_task.build_process_node().run()
            self.update_result_post_processing(post_task.result)


if __name__ == "__main__":
    import pandas as pd
    from common_package.process_logs_utils import filt_by_intents, filt_by_logmode

    # file_path = "crawl_daily_logs/outputs/logs_date_2023-02-26.csv"
    file_path = "no_file_path"
    task = (
        PostProcessingTask()
        .add_command(pd.read_csv, file_path)
        .add_command(filt_by_intents, WAITING_RESULT, "music.ask_music.ask_play_lyrics")
        .add_command(filt_by_logmode, WAITING_RESULT, "lyrics_from_zmp3")
        .add_command(len, WAITING_RESULT)
        .set_failure_handler(lambda: 0)
        .add_post_command(print, WAITING_RESULT)
    )

    result = task.run()
    print("result", result)
