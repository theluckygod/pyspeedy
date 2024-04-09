from __future__ import annotations

import json

import common_package.constants as constants
import common_package.file_utils as file_utils
import common_package.kiki_logs_constants as kiki_logs_constants
import common_package.process_logs.utils as utils
import common_package.process_logs_utils as process_logs_utils
import daily_logs_analysis_v2.filters as filters
from beartype import beartype
from beartype.typing import Dict, List
from common_package.process_logs.commands import WAITING_RESULT, Command, Result
from common_package.process_logs.kernel import Kernel
from common_package.process_logs.process_logs_command import ProcessLogsCommand
from common_package.process_logs.task import Task
from daily_logs_analysis_v2.analysis.music_tracking import MusicTrackingAnalysis
from daily_logs_analysis_v2.utils.db_updater import DBUpdater
from daily_logs_analysis_v2.utils.utils import LogsTrackingTask, UpdateState
from loguru import logger

TABLE_NAME = "logics_tracking_v2"
APP_TYPES = [kiki_logs_constants.APP.ALL_APPS, kiki_logs_constants.APP.APP_ZINGS]
FILTERS: List[filters.Filter] = [
    filters.AskAlbumFilter,
    filters.AskMedleyFilter,
    filters.AskSongFilter,
    filters.BlockContentFilter,
    filters.GenreFilter,
    filters.LyricsFilter,
    filters.NearlyMatchFilter,
    filters.ZMP3MusicFilter,
    filters.SearchHighlightFilter,
    filters.MappedMusicFilter,
]


class LogicsTrackingAnalysis(MusicTrackingAnalysis):

    @staticmethod
    @beartype
    def analysis(date: str, deploy_type: str, **kwargs: Dict) -> None:
        dbupdater = DBUpdater(deploy_type=deploy_type, table_name=TABLE_NAME)
        kernel = Kernel()
        load_logs_result = Command(file_utils.load_logs_data, date).run()
        assert load_logs_result.is_done(), f"Failed to load logs data {date}!!!"
        df = load_logs_result.result

        for app_type in APP_TYPES:
            filt_app_type = ProcessLogsCommand(
                process_logs_utils.filt_by_app_type, app_type
            )

            for backend in df["backend"].dropna().unique().tolist() + ["ALL"]:
                if backend == "ALL":
                    filt_backend = ProcessLogsCommand(utils.dummy_result_func)
                else:
                    filt_backend = ProcessLogsCommand(
                        process_logs_utils.filt_by_backend, backend
                    )

                key_kwargs = {
                    "device_type": json.dumps(app_type),
                    "date": date,
                    "backend": backend,
                }

                for filter in FILTERS:
                    instance = filter()
                    task = (
                        LogsTrackingTask.create_dummy_task(df)
                        .add_command(filt_app_type)
                        .add_command(filt_backend)
                        .add_commands(instance.build_filting_task().commands)
                        .add_process_logs_command(__class__.get_extracting_info_task())
                        .add_process_logs_command(
                            __class__.get_set_key_task(
                                logic=instance.get_logic_name(),
                                state=UpdateState.OK,
                                **key_kwargs,
                            )
                        )
                        .set_failure_handler(
                            __class__.get_default_failure_task(
                                logic=instance.get_logic_name(),
                                state=UpdateState.FAIL,
                                **key_kwargs,
                            )
                        )
                        .add_post_command(
                            Command(
                                utils.wrapped_print,
                                "Result task: {}",
                                WAITING_RESULT,
                                keep_result=WAITING_RESULT,
                            )
                        )
                        .add_post_command(ProcessLogsCommand(dbupdater.add_values))
                    )
                    kernel.add_task(task)

        kernel.run()
        dbupdater.do_upsert()


if __name__ == "__main__":

    date = "2023-02-26"
    LogicsTrackingAnalysis.analysis(date=date)
