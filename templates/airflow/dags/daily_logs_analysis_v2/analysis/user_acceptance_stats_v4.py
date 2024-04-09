from __future__ import annotations

import datetime
import gc
import glob

import common_package.constants as constants
import common_package.file_utils as file_utils
import common_package.kiki_logs_constants as kiki_logs_constants
import common_package.process_logs_utils as process_logs_utils
import pandas as pd
from beartype import beartype
from beartype.typing import Dict, List
from common_package.process_logs.commands import Command
from crawl_daily_logs.crawler import (
    KIKI_MP3_DURATION_STATS_DATE_FORMAT,
    KIKI_MP3_DURATION_STATS_HEADERS,
    KIKI_MP3_DURATION_STATS_PATH,
    KiKiLogsCrawler,
)
from daily_logs_analysis_v2.analysis.analysis import Analysis
from daily_logs_analysis_v2.utils.db_updater import DBUpdater
from daily_logs_analysis_v2.utils.utils import LogsTrackingTask, UpdateState

TABLE_NAME = "kiki_user_acceptance_stats_v4"
ACTION_CODE = "suggestionmp3"
DAYS_BACK_LEN = 14


def load_mp3_song_duration(date):
    str_date = date.strftime(KIKI_MP3_DURATION_STATS_DATE_FORMAT)
    file_paths = glob.glob(KIKI_MP3_DURATION_STATS_PATH.format(str_date))
    assert (
        len(file_paths) == 1
    ), f"Found {len(file_paths)} files with date {str_date}!!!"
    file_path = file_paths[0]
    mp3_song_duration_df = pd.read_csv(file_path, sep=",")
    mp3_song_duration_df = mp3_song_duration_df.rename(
        columns={
            "source_id": "id",
            "duration_max": "duration",
            "song_length_max": "song_length",
        }
    )

    crawler = KiKiLogsCrawler()
    start_ts = int(date.timestamp() * 1000)
    process_mp3_logs_df = crawler.process_duration_data(
        mp3_song_duration_df, _type="zingmp3", start_ts=start_ts
    )
    assert len(process_mp3_logs_df) == len(mp3_song_duration_df), (
        f"Length of processed mp3 logs (len={len(process_mp3_logs_df)}) "
        f"is not equal to length of mp3 logs (len={len(mp3_song_duration_df)})!!!"
    )
    # extend all columns in process_mp3_logs_df to mp3_song_duration_df
    mp3_song_duration_df = mp3_song_duration_df.assign(**process_mp3_logs_df)

    return mp3_song_duration_df


class UserAcceptanceStatsV4Analysis(Analysis):
    """Summary of user acceptance stats v4
    Different from user_acceptance_stats_v3, this version removed the suggestion logs

    Parameters:
        Analysis (_type_): _description_

    Returns:
        _type_: _description_
    """

    @staticmethod
    @beartype
    def analysis(date: str, deploy_type: str, **kwargs: Dict) -> None:
        dbupdater = DBUpdater(deploy_type=deploy_type, table_name=TABLE_NAME)
        date = datetime.datetime.strptime(date, constants.DATE_FORMAT)

        mp3_song_duration_df = load_mp3_song_duration(date)
        mp3_song_duration_df["is_suggestion"] = False

        # trace days back
        for i in range(DAYS_BACK_LEN):
            gc.collect()

            trace_date = date.date() - datetime.timedelta(days=i)
            date_str = trace_date.strftime(constants.DATE_FORMAT)
            suggestion_df_result = (
                LogsTrackingTask(
                    command=Command(
                        file_utils.load_logs_data,
                        date_str,
                        columns=["id", "device_type", "actions"],
                    )
                )
                .add_process_logs_command(
                    process_logs_utils.filt_by_app_type,
                    kiki_logs_constants.APP.APP_ZINGS,
                )
                .add_process_logs_command(
                    process_logs_utils.filt_actions_by_trace, ACTION_CODE
                )
                .run()
            )
            assert (
                suggestion_df_result.is_done()
            ), f"{suggestion_df_result.traceback}\nFailed to parse logs data {date_str}!!!"
            suggestion_df = suggestion_df_result.result
            suggestion_df["id"] = suggestion_df["id"].apply(
                lambda id: id if id.startswith("1conn") else id[-12:]
            )

            filter = (~mp3_song_duration_df["is_suggestion"]) & (
                mp3_song_duration_df["id"].isin(suggestion_df["id"])
            )
            mp3_song_duration_df.loc[filter, "is_suggestion"] = True

        mp3_song_duration_df = mp3_song_duration_df[
            mp3_song_duration_df["is_suggestion"] == False
        ]
        key_values = __class__.get_default_key(
            date.date().strftime(constants.DATE_FORMAT)
        )
        values = __class__.extract_info(mp3_song_duration_df)
        values.update(key_values)
        dbupdater.add_values(values)
        dbupdater.do_upsert()

    @staticmethod
    def extract_info(df: pd.DataFrame, *args, **kwargs) -> dict:
        info = {}

        assert process_logs_utils.count_intent(
            df, is_dur=True
        ) == process_logs_utils.count_intent(
            df
        ), "Total intent is not equal to total intent with duration!!!"
        info["total"] = process_logs_utils.count_intent(df)
        info["count_happy"] = process_logs_utils.count_accepted_intent(df)
        info["count_short_songs"] = process_logs_utils.count_short_songs(df)

        return info

    @staticmethod
    @beartype
    def get_default_key(date: str) -> dict:
        key_info = {
            "date": date,
        }
        return key_info


if __name__ == "__main__":
    from loguru import logger

    date = "2023-05-29"
    UserAcceptanceStatsV4Analysis.analysis(date=date, deploy_type="prod")
