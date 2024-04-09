from __future__ import annotations

import json

import common_package.file_utils as file_utils
import common_package.kiki_logs_constants as kiki_logs_constants
import common_package.process_logs.utils as utils
import common_package.process_logs_utils as process_logs_utils
import crawl_daily_logs.crawl_tools as crawl_tools
import pandas as pd
from beartype import beartype
from beartype.typing import Dict, List, Union
from common_package.process_logs.commands import WAITING_RESULT, Command, Result
from common_package.process_logs.process_logs_command import ProcessLogsCommand
from daily_logs_analysis_v2.analysis.analysis import Analysis
from daily_logs_analysis_v2.utils.db_updater import DBUpdater
from daily_logs_analysis_v2.utils.utils import (
    LogsTrackingTask,
    UpdateState,
    WrappedDataFrame,
)
from loguru import logger

TABLE_NAME = "suggestion_tracking"
SUGGESTION_ACTION_CODE = "SuggestionMP3"
NO_ACTION_ACTION_CODE_LIST = [
    SUGGESTION_ACTION_CODE,
    "DisplayCard",
    "SpeechSynthesizer",
    "SpeechSynthesizerOffline",
    "ResponseDisplayCard",
    "ResponseSpeechSynthesizer",
]
NO_SUGGESTION_HASH = "no_suggestion"


def join_action_logs(client_logs_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Join action logs with client logs

    Parameters
    ----------
    client_logs_df : pd.DataFrame
        Client logs dataframe
    df : pd.DataFrame
        Action logs dataframe

    Returns
    -------
    pd.DataFrame
        Joined dataframe
    """

    df.index = df.index.map(lambda x: x if x.startswith("1conn") else x[-12:])
    df = df.merge(client_logs_df, left_index=True, right_index=True, how="outer")
    return df


def parse_json_suggested_items(df: pd.DataFrame) -> pd.DataFrame:
    """Parse json suggested items from actions column that contains SuggestionMP3 action code
        and assign to suggested_items column

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """

    def select_suggested_items(actions: List[Dict]) -> Dict:
        for action in actions:
            if action["action_code"] == SUGGESTION_ACTION_CODE:
                return action["payload"]["items"]
        return None

    def dummy_suggested_item_index(items: List[Dict]) -> List[int]:
        return list(range(len(items)))

    df["suggested_items"] = None
    suggestion_filter = (df["actions"].notnull()) & (
        df["actions"].str.contains(SUGGESTION_ACTION_CODE)
    )
    df.loc[suggestion_filter.index, "suggested_items"] = df[suggestion_filter][
        "actions"
    ].apply(lambda actions: select_suggested_items(json.loads(actions)))
    df.loc[suggestion_filter.index, "suggested_item_index"] = df[suggestion_filter][
        "suggested_items"
    ].apply(lambda items: dummy_suggested_item_index(items))
    return df


def process_suggestion_logs(df: pd.DataFrame) -> pd.DataFrame:
    """Process suggestion logs: split suggested items into rows and then create a hash column with value "suggester|item:index" """
    df = df.explode(["suggested_items", "suggested_item_index"], ignore_index=True)
    suggestion_filter = df["suggested_items"].notnull()
    df.loc[suggestion_filter, "suggested_item_hash"] = df[suggestion_filter][
        ["suggested_items", "suggested_item_index"]
    ].apply(
        lambda row: f"{row['suggested_items']['suggester']}|item:{row['suggested_item_index']}",
        axis=1,
    )
    df.loc[~suggestion_filter, "suggested_item_hash"] = NO_SUGGESTION_HASH
    return df


def filt_no_result(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out no result suggestion logs"""

    def doesContainOnlyDisplayCardsAndSpeechSynthesizerActions(
        actions: List[Dict],
    ) -> bool:
        for action in actions:
            if action["action_code"] not in NO_ACTION_ACTION_CODE_LIST:
                return False
        return True

    filter = (df["actions"].notnull()) & (
        df["actions"].apply(
            lambda actions: doesContainOnlyDisplayCardsAndSpeechSynthesizerActions(
                json.loads(actions)
            )
        )
    )
    return df[filter]


class SuggestionTrackingAnalysis(Analysis):
    @staticmethod
    @beartype
    def analysis(date: str, deploy_type: str, **kwargs: Dict) -> None:
        dbupdater = DBUpdater(deploy_type=deploy_type, table_name=TABLE_NAME)

        suggestion_df_result = (
            LogsTrackingTask(command=Command(file_utils.load_logs_data, date))
            .add_process_logs_command(
                lambda df: df[(df.id.notnull()) & (df.actions.notnull())]
            )
            .add_process_logs_command(lambda df: df.set_index("id"))
            .add_process_logs_command(
                process_logs_utils.filt_by_app_type, kiki_logs_constants.APP.APP_ZINGS
            )
            .run()
        )
        assert (
            suggestion_df_result.is_done()
        ), f"Failed to load/process logs data {date}!!!"
        action_df = suggestion_df_result.result

        df_result = (
            LogsTrackingTask(
                command=Command(crawl_tools.crawl_daily_suggestion_mp3_logs, date)
            )
            .add_process_logs_command(join_action_logs, action_df)
            .add_process_logs_command(parse_json_suggested_items)
            .run()
        )
        assert (
            df_result.is_done()
        ), f"Failed to load/process suggestion client logs data {date}!!!"
        df = df_result.result

        for device_type in df["device_type"].dropna().unique().tolist():
            filt_device_df = process_logs_utils.filt_by_device_type(df, device_type)

            for backend in filt_device_df["backend"].dropna().unique().tolist():
                filt_backend_df = process_logs_utils.filt_by_backend(
                    filt_device_df, backend
                )

                for sdk_version in (
                    filt_backend_df["version_code"].dropna().unique().tolist()
                ):
                    filt_sdk_df = filt_backend_df[
                        filt_backend_df["version_code"] == sdk_version
                    ]
                    key_kwargs = {
                        "date": date,
                        "device_type": device_type,
                        "backend": backend,
                        "sdk_version": sdk_version,
                    }
                    num_no_result = len(filt_no_result(filt_sdk_df))
                    num_suggested = len(
                        filt_sdk_df[filt_sdk_df["suggested_items"].notnull()]
                    )

                    filt_sdk_df = process_suggestion_logs(filt_sdk_df)
                    for hashkey in (
                        filt_sdk_df["suggested_item_hash"].dropna().unique().tolist()
                    ):
                        filted_df = filt_sdk_df[
                            filt_sdk_df["suggested_item_hash"] == hashkey
                        ]
                        task = (
                            LogsTrackingTask.create_dummy_task(
                                WrappedDataFrame(filted_df)
                            )
                            .add_process_logs_command(
                                __class__.get_extracting_info_task(
                                    suggestion_type=hashkey,
                                    num_no_result=num_no_result,
                                    num_suggested=num_suggested,
                                )
                            )
                            .add_process_logs_command(
                                __class__.get_set_key_task(**key_kwargs)
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
                            .run()
                        )
                        assert (
                            task.is_done()
                        ), f"{task.traceback}\nFailed to run task!!!"

        dbupdater.do_upsert()

    @staticmethod
    def extract_info(
        df: pd.DataFrame,
        suggestion_type: str,
        num_no_result: int,
        num_suggested: int,
        *args,
        **kwargs,
    ) -> dict:
        info = {}
        info["num_no_result"] = num_no_result
        info["num_suggested"] = num_suggested

        info["suggestion_type"] = suggestion_type

        play_filter = (
            (df["item_index"].notnull())
            & (df["suggested_item_index"] == df["item_index"])
            & (
                ((df["action_type"] == "item") & (df["item_type"] == "song"))
                | (df["action_type"] == "play")
            )
        )
        info["num_direct_play"] = len(df[play_filter])
        info["num_dur_direct_play"] = len(df[play_filter & (df["duration"].notnull())])
        info["num_happy_direct_play"] = len(df[play_filter & (df["is_accepted"])])

        indirect_play_filter = (
            (df["item_index"].notnull())
            & (df["suggested_item_index"] == df["item_index"])
            & (df["action_type"] == "item")
            & (df["item_type"].isin(["artist", "playlist", "album"]))
        )
        info["num_indirect_play"] = len(df[indirect_play_filter])
        info["num_dur_indirect_play"] = len(
            df[indirect_play_filter & (df["duration"].notnull())]
        )
        info["num_happy_indirect_play"] = len(
            df[indirect_play_filter & (df["is_accepted"])]
        )

        return info

    @staticmethod
    def get_extracting_info_task(*args, **kwargs):
        return ProcessLogsCommand(__class__.extract_info, *args, **kwargs)

    @staticmethod
    @beartype
    def set_key(info: Dict, *args, **kwargs) -> dict:
        key_info = __class__.get_default_key(*args, **kwargs)
        info.update(key_info)
        return info

    @staticmethod
    @beartype
    def get_default_key(
        date: str, device_type: str, backend: str, sdk_version: Union[int, float]
    ) -> dict:
        key_info = {
            "date": date,
            "device_type": device_type,
            "backend": backend,
            "sdk_version": int(sdk_version),
        }

        return key_info

    @staticmethod
    @beartype
    def get_set_key_task(*args, **kwargs):
        return ProcessLogsCommand(__class__.set_key, *args, **kwargs)

    @staticmethod
    @beartype
    def get_default_failure_task(*args, **kwargs):
        return Command(__class__.get_default_key, *args, **kwargs)


if __name__ == "__main__":

    date = "2023-06-06"
    SuggestionTrackingAnalysis.analysis(date, "prod")
