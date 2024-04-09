from __future__ import annotations

import json

import common_package.constants as constants
import common_package.file_utils as file_utils
import common_package.process_logs.utils as utils
import common_package.process_logs_utils as process_logs_utils
import pandas as pd
from beartype import beartype
from beartype.typing import Dict, List
from common_package.process_logs.commands import WAITING_RESULT, Command, Result
from common_package.process_logs.kernel import Kernel
from common_package.process_logs.process_logs_command import ProcessLogsCommand
from daily_logs_analysis_v2.analysis.music_tracking import MusicTrackingAnalysis
from daily_logs_analysis_v2.utils.db_updater import DBUpdater
from daily_logs_analysis_v2.utils.utils import (
    LogsTrackingTask,
    UpdateState,
    WrappedDataFrame,
)
from loguru import logger

TABLE_NAME = "logics_tracking_v2"
DESCRIPTION = "intent automatic monitoring"


class IntentTrackingAnalysis(MusicTrackingAnalysis):
    @staticmethod
    @beartype
    def analysis(date: str, deploy_type: str, **kwargs: Dict) -> None:
        dbupdater = DBUpdater(deploy_type=deploy_type, table_name=TABLE_NAME)

        load_logs_result = Command(file_utils.load_logs_data, date).run()
        assert load_logs_result.is_done(), f"Failed to load logs data {date}!!!"
        df = load_logs_result.result

        for device_type in df["device_type"].dropna().unique().tolist():
            filt_device_df = process_logs_utils.filt_by_device_type(df, device_type)

            for backend in df["backend"].dropna().unique().tolist():
                filted_df = process_logs_utils.filt_by_backend(filt_device_df, backend)

                intent_lst = filted_df["intent"].dropna().unique().tolist()
                for intent in sorted(intent_lst, reverse=True):
                    key_kwargs = {
                        "device_type": device_type,
                        "date": date,
                        "backend": backend,
                        "logic": intent,
                        "description": DESCRIPTION,
                    }

                    task = (
                        LogsTrackingTask.create_dummy_task(WrappedDataFrame(filted_df))
                        .add_process_logs_command(
                            process_logs_utils.filt_by_intents, intent
                        )
                        .add_process_logs_command(__class__.get_extracting_info_task())
                        .add_process_logs_command(
                            __class__.get_set_key_task(
                                state=UpdateState.OK, **key_kwargs
                            )
                        )
                        .set_failure_handler(
                            __class__.get_default_failure_task(
                                state=UpdateState.FAIL, **key_kwargs
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
                    task.run()

        dbupdater.clean_rows(lambda row: row["num"] == 0)
        dbupdater.do_upsert()


if __name__ == "__main__":

    date = "2023-02-26"
    IntentTrackingAnalysis.analysis(date)
