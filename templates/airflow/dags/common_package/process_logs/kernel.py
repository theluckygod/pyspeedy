from __future__ import annotations

import time
from threading import Lock, Thread

import common_package.process_logs.utils as utils
from beartype import beartype
from common_package.process_logs.commands import WAITING_RESULT
from common_package.process_logs.process_node import ProccessNode, RootProccessNode
from common_package.process_logs.task import Task
from loguru import logger

from pyspeedy.patterns.singleton import SingletonMeta


class Kernel(metaclass=SingletonMeta):
    tree: ProccessNode = None

    def __init__(self) -> None:
        self._run_lock: Lock = Lock()
        self.tree = RootProccessNode()

    @beartype
    def add_task(self, task: Task) -> None:
        self.tree.merge(task.build_process_node())

    def run(self) -> None:
        logger.debug("===========================================")
        logger.debug("Kernel tree:")
        logger.debug(self.tree)
        logger.debug("===========================================")

        start_time = time.time()
        with self._run_lock:
            self.tree.run()
            self.tree = RootProccessNode()
        logger.info(f"Kernel run in {round(time.time() - start_time, 2)}s")


if __name__ == "__main__":
    import time

    import pandas as pd
    from common_package.process_logs_utils import filt_by_intents, filt_by_logmode

    start_time = time.time()

    file_path = "crawl_daily_logs/outputs/logs_date_2023-02-26.csv"
    wrap_df1 = (
        Task()
        .add_command(pd.read_csv, file_path)
        .add_command(
            filt_by_intents, df=WAITING_RESULT, intent="music.ask_music.ask_play_lyrics"
        )
        .add_command(filt_by_logmode, df=WAITING_RESULT, log_mode="lyrics_from_zmp3")
        .add_command(
            pd.DataFrame.to_csv,
            WAITING_RESULT,
            "common_package/process_logs/outputs/lyrics_from_zmp3.csv",
        )
    )
    wrap_df2 = (
        Task()
        .add_command(pd.read_csv, file_path)
        .add_command(
            filt_by_intents, WAITING_RESULT, "music.ask_music.ask_song_mp3_app"
        )
        .add_command(
            lambda df, logmode: df[df.actions.str.contains(logmode)],
            WAITING_RESULT,
            "blocked",
        )
        .add_command(
            pd.DataFrame.to_csv,
            WAITING_RESULT,
            "common_package/process_logs/outputs/ask_song_mp3_app.csv",
        )
    )
    wrap_df3 = (
        Task()
        .add_command(pd.read_csv, file_path)
        .add_command(
            lambda df, device_type: df[df.device_type.str.contains(device_type)],
            df=WAITING_RESULT,
            device_type="ZING",
        )
        .add_command(
            filt_by_intents, WAITING_RESULT, "music.ask_music.ask_song_mp3_app"
        )
        .add_command(filt_by_logmode, WAITING_RESULT, "highlight_api_play")
        .add_command(len, WAITING_RESULT)
        .add_command(utils.wrapped_print, "Result: {}", WAITING_RESULT)
    )

    # process1 = Thread(target=Kernel().add_task, args=(wrap_df1,))
    # process2 = Thread(target=Kernel().add_task, args=(wrap_df2,))
    # process3 = Thread(target=Kernel().add_task, args=(wrap_df3,))
    # process1.start()
    # process2.start()
    # process3.start()

    Kernel().add_task(wrap_df1)
    Kernel().add_task(wrap_df2)
    Kernel().add_task(wrap_df3)

    Kernel().run()

    process_time = time.time() - start_time
    print(f"--- {process_time} seconds ---")
    assert process_time < 15, "process_time should be less than 15 seconds"
