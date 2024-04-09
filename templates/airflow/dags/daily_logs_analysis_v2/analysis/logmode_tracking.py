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
DESCRIPTION = "logmode automatic monitoring"
LOGMODE_REGEX = r"\"mode\":\"\[([^\"]+)\]\""


def clean_logmode(logmode: str) -> str:
    # remove logmode's suffix
    if "@" in logmode:
        logmode = logmode.split("@")[0]
    return logmode


def parse_logmode(df: pd.DataFrame, regex=LOGMODE_REGEX) -> pd.DataFrame:
    # extract logmode from actions column with regex pattern
    return (
        df["actions"]
        .str.extract(regex, expand=False)
        .apply(
            lambda logmode: (
                '["' + logmode.replace(", ", '","') + '"]'
                if isinstance(logmode, str)
                else "[]"
            )
        )
    )


def filt_by_extracted_logmode(df: pd.DataFrame, logmode: str) -> pd.DataFrame:
    return df[df["logmode"].str.contains(f'"{logmode}["|@]')]


class LogmodeTrackingAnalysis(MusicTrackingAnalysis):

    @staticmethod
    @beartype
    def analysis(date: str, deploy_type: str, **kwargs: Dict) -> None:
        dbupdater = DBUpdater(deploy_type=deploy_type, table_name=TABLE_NAME)

        load_logs_result = Command(file_utils.load_logs_data, date).run()
        assert load_logs_result.is_done(), f"Failed to load logs data {date}!!!"
        df = load_logs_result.result
        df["logmode"] = parse_logmode(df)

        for device_type in df["device_type"].dropna().unique().tolist():
            filt_device_df = process_logs_utils.filt_by_device_type(df, device_type)

            for backend in df["backend"].dropna().unique().tolist():
                filted_df = process_logs_utils.filt_by_backend(filt_device_df, backend)

                logmode_lst = []
                for logmodes in filted_df["logmode"].dropna().tolist():
                    tmp_lst = json.loads(logmodes)
                    tmp_lst = list(map(clean_logmode, tmp_lst))
                    logmode_lst.extend(tmp_lst)
                logmode_lst = list(set(logmode_lst))

                key_kwargs = {
                    "device_type": device_type,
                    "date": date,
                    "backend": backend,
                    "description": DESCRIPTION,
                }

                for logmode in sorted(logmode_lst, reverse=True):
                    task = (
                        LogsTrackingTask.create_dummy_task(WrappedDataFrame(filted_df))
                        .add_process_logs_command(filt_by_extracted_logmode, logmode)
                        .add_process_logs_command(__class__.get_extracting_info_task())
                        .add_process_logs_command(
                            __class__.get_set_key_task(
                                logic=logmode, state=UpdateState.OK, **key_kwargs
                            )
                        )
                        .set_failure_handler(
                            __class__.get_default_failure_task(
                                logic=logmode, state=UpdateState.FAIL, **key_kwargs
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
    LogmodeTrackingAnalysis.analysis(date)

    # actions = """
    # [{"action_code":"DisplayCard","action_id":"7b5a5275-85f2-4427-ab39-a91df023a58a","required":[],"payload":{"cancel_enabled":false,"text":"Đang mở bài hát Ngàn Lý Do Anh Đặt Ra","render":"SimpleCardTemplate"},"mode":"[highlight_api_play, zingmp3_search_song.matching_a_single_best]"},{"action_code":"SpeechSynthesizer","action_id":"ad32e032-5ae0-4a4f-ba92-d4f954a1e553","required":[],"payload":{"utter_text":"Đang mở bài hát Ngàn Lý Do Anh Đặt Ra","cached_url":"https://audiostream-ailab.tts.zalo.ai/m3u8/809c8109114bf815a15a.m3u8?kiki-server-request=1","cacheable":true,"cached_audio_link":"https://chunk-ailab.tts.zalo.ai/637276ce6c8e85d0dc9f?requestId=128743457","cached_audio_extension":"mp3"},"mode":""},{"action_code":"SpeechSynthesizerOffline","action_id":"5943818c-98f7-494e-a258-7e998d2f1eb9","required":["ad32e032-5ae0-4a4f-ba92-d4f954a1e553|fail"],"payload":{"type":"PLAY_MP3_SUCCESS"},"mode":""},{"action_code":"PlayerMP3","action_id":"6150ec23-8779-4c7f-b2b6-1dfa3b67a1ee","required":[],"payload":{"idEncode":"Z6WDB00B","artist":"Sơn Ca, Quang Hà","name":"Ngàn Lý Do Anh Đặt Ra","thumb":"https://photo-resize-zmp3.zmdcdn.me/w240_r1x1_jpeg/avatars/7/0/70ef265944844e1372ab125ba6da34c9_1511598604.jpg","link":"https://zingmp3.vn/bai-hat/Ngan-Ly-Do-Anh-Dat-Ra-Son-Ca-Quang-Ha/Z6WDB00B.html","direct_link":"","timer_timeout":0,"type":"song","command":"start","request_playing_in_order":false},"mode":""},{"action_code":"DisplayCard","action_id":"0210d8cf-d04b-4292-90b9-854bf6d08348","required":["6150ec23-8779-4c7f-b2b6-1dfa3b67a1ee|fail"],"payload":{"cancel_enabled":false,"text":"Mở nhạc thất bại","render":"SimpleCardTemplate"},"mode":""},{"action_code":"SpeechSynthesizer","action_id":"5ec95598-0627-43c3-9b1e-6ef6ef13a952","required":["6150ec23-8779-4c7f-b2b6-1dfa3b67a1ee|fail"],"payload":{"utter_text":"Mở nhạc thất bại","cacheable":true},"mode":""},{"action_code":"DisplayCard","action_id":"e432487a-1c2f-4f10-aaeb-a9d17e5a5238","required":["6150ec23-8779-4c7f-b2b6-1dfa3b67a1ee|timeout"],"payload":{"cancel_enabled":false,"text":"Rất tiếc, không nhận được phản hồi","render":"SimpleCardTemplate"},"mode":""},{"action_code":"SpeechSynthesizer","action_id":"69ca0da4-63af-4bc3-8b6c-d26a89630d48","required":["6150ec23-8779-4c7f-b2b6-1dfa3b67a1ee|timeout"],"payload":{"utter_text":"Rất tiếc, không nhận được phản hồi","cacheable":true}}]
    # """

    # df = pd.DataFrame([{"actions": actions}])
    # df["logmode"] = parse_logmode(df)
    # print(df.logmode.loc[0])
