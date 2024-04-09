from beartype import beartype
from beartype.typing import Callable, Union
from common_package.process_logs.process_logs_command import ProcessLogsCommand
from common_package.process_logs.task import Task


class ProcessLogsTask(Task):
    @beartype
    def add_process_logs_command(
        self, func: Union[Callable, ProcessLogsCommand], *args, **kwargs
    ):
        if isinstance(func, ProcessLogsCommand):
            assert (
                len(args) == 0 and len(kwargs) == 0
            ), "You must not pass any args or kwargs when func is ProcessLogsCommand"
            command = func
        else:
            command = ProcessLogsCommand(func, *args, **kwargs)
        self.commands.append(command)
        return self


if __name__ == "__main__":
    import pandas as pd
    from common_package.process_logs_utils import filt_by_intents, filt_by_logmode

    file_path = "crawl_daily_logs/outputs/logs_date_2023-02-26.csv"
    task = (
        ProcessLogsTask()
        .add_command(pd.read_csv, file_path)
        .add_process_logs_command(filt_by_intents, "music.ask_music.ask_play_lyrics")
        .add_process_logs_command(filt_by_logmode, "lyrics_from_zmp3")
    )

    result = task.run()
    print("result", result)
