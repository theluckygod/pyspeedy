import os

import common_package.file_utils as file_utils
import common_package.kiki_logs_constants as kiki_logs_constants
import common_package.process_logs_utils as process_logs_utils
import pandas as pd
import statistic_uar_by_query.constants as constants
from loguru import logger
from statistic_uar_by_query.tools.data_processing import DataProcessingTools

OUTPUT_FOLDER = "{}_outputs"


def get_first_valid_str_value(x: pd.Series):
    valid_x = x[x.str.len() > 0]
    if len(valid_x) == 0:
        return ""
    return valid_x.iloc[0]


class AllTestcasesRowDataProcessing:
    def process_confirm_intent_helper(df):
        df = df.sort_values("time", ascending=False)

        lyrics_df = process_logs_utils.filt_by_intents(
            df, kiki_logs_constants.INTENT.LYRICS_INTENT
        )
        lyrics_df = lyrics_df[lyrics_df["is_reask"] == True]
        confirm_intent_df = process_logs_utils.filt_by_intents(
            df, kiki_logs_constants.INTENT.CONFIRM_INTENTS
        )

        drop_list = []
        for idx, row in confirm_intent_df.iterrows():
            userid = row["userid"]

            sub_df = lyrics_df[
                (lyrics_df["userid"] == userid)
                & (lyrics_df["time"].astype(int) <= int(row["time"]))
            ]
            if len(sub_df) == 0:
                drop_list.append(idx)
                print("remove action confirm by not found lyrics reask:", row["id"])
                continue

            for sub_idx, sub_row in sub_df.iterrows():
                if int(row["time"]) - int(sub_row["time"]) > 60000:
                    drop_list.append(idx)
                    print("remove action confirm by reasking overtime:", row["id"])
                    break

                if "question" in df.keys():
                    df.at[idx, "question"] = sub_row["question"]
                if "logmode" in df.keys():
                    df.at[idx, "logmode"] = sub_row["logmode"]

                if row["intent"] == kiki_logs_constants.INTENT.CONFIRM_INTENT_NO:
                    df.at[idx, "is_accepted"] = False
                    if "action_verbose" in df.keys():
                        df.at[idx, "action_verbose"] = sub_row["action_verbose"]

                drop_list.append(sub_idx)
                break

        df = df.drop(index=drop_list)
        df = df.reset_index()
        return df

    def process_confirm_intent(df):
        print("Processing confirmation intents")
        df = AllTestcasesRowDataProcessing.process_confirm_intent_helper(df)
        df = df[
            [
                "device",
                "action",
                "action_verbose",
                "is_accepted",
                "question",
                "userid",
                "time",
                "intent",
                "backend",
                "logmode",
            ]
        ]
        return df

    def merge_daily_statistic_uar_data(df_list):
        if not df_list:
            raise ValueError("Daily data is empty!")

        df = df_list[0]
        interesting_columns = list(df.columns.values)

        for daily_df in df_list[1:]:
            df = AllTestcasesRowDataProcessing._merge_helper(df, daily_df)
        return df[interesting_columns]

    def _merge_helper(df, ano_df):
        df = pd.concat([df, ano_df], ignore_index=True, sort=False)

        df = (
            df.groupby(by=["question", "backend", "action", "intent"], dropna=False)
            .agg(
                action_verbose=(
                    "action_verbose",
                    lambda x: get_first_valid_str_value(x),
                ),
                logmode=("logmode", lambda x: get_first_valid_str_value(x)),
                frequency=("frequency", sum),
                duration_num=("duration_num", sum),
                accepted_num=("accepted_num", sum),
            )
            .reset_index()
        )

        df["uar"] = df["accepted_num"] / df["duration_num"].fillna(1)
        df["type"] = df["action"].str.split("_", expand=True)[0].fillna("song")
        df["label"] = df.apply(
            lambda x: DataProcessingTools.is_satisfied_uar_criteria(
                x.frequency, x.uar, x.type
            ),
            axis=1,
        )

        df = df[
            [
                "question",
                "action_verbose",
                "action",
                "uar",
                "frequency",
                "duration_num",
                "accepted_num",
                "label",
                "intent",
                "backend",
                "logmode",
            ]
        ]
        return df

    def extract_testcases(data: pd.DataFrame):
        data["media_id"] = data["media_id"].fillna("")
        data["media_title"] = data["media_title"].fillna("")

        data["device"] = data["device_type"]
        data["action"] = data.apply(
            lambda row: DataProcessingTools.get_song_album_ID(row), axis=1
        )
        data["action_verbose"] = data.apply(
            lambda row: DataProcessingTools.get_action(row), axis=1
        )
        data["question"] = data.apply(
            lambda row: DataProcessingTools.get_question(row), axis=1
        )
        data["userid"] = data.apply(
            lambda row: DataProcessingTools.get_user_id(row), axis=1
        )
        data["is_reask"] = data["actions"].apply(
            lambda action: process_logs_utils.extract_info_from_re(
                action, kiki_logs_constants.PATTERN.REASK_PATTERN
            )
            is not None
        )
        data["time"] = data.apply(lambda row: DataProcessingTools.get_time(row), axis=1)
        data["logmode"] = data.apply(
            lambda row: DataProcessingTools.get_logmode(row), axis=1
        )
        data = data[
            [
                "id",
                "device",
                "action",
                "action_verbose",
                "is_accepted",
                "question",
                "userid",
                "time",
                "intent",
                "is_reask",
                "backend",
                "logmode",
            ]
        ]
        return data

    def merge_data_by_song(df):
        queries_list = []
        response_list = []
        verbose_res_list = []
        labels_list = []
        uar_list = []
        freq_list = []
        duration_num_list = []
        accepted_num_list = []
        intent_list = []
        backend_list = []
        logmode_list = []

        groupby_df = df.groupby(
            ["backend", "question", "intent", "action"], observed=True
        )

        # merge by backend
        for key, sub_df in groupby_df:
            backend, ques, intent, songid = key

            song_name = get_first_valid_str_value(sub_df["action_verbose"])
            logmode = get_first_valid_str_value(sub_df["logmode"])

            freq = len(sub_df)
            if freq == 0:
                continue

            if songid:
                song_type = songid.split("_")[0]
            else:
                song_type = "song"

            filted_df = sub_df[sub_df["is_accepted"].notnull()]
            duration_num = len(filted_df)
            accepted_num = len(filted_df[filted_df["is_accepted"] == True])
            uar = 0
            if duration_num > 0:
                uar = accepted_num / duration_num

            if (
                not songid
                and accepted_num == 0
                and intent not in constants.INTERESTING_INTENTS_WHICH_NO_DURATION
            ):
                continue

            if song_type == "song":
                label = DataProcessingTools.process_song(sub_df, uar)
            elif song_type == "album":
                label = DataProcessingTools.process_album(sub_df, uar)
            elif song_type == "video":
                label = DataProcessingTools.process_video(sub_df, uar)
            elif song_type == "movie":
                # TODO: unsupport
                pass
            else:
                logger.info(f"Unsupport song type: {song_type}")
                continue

            queries_list.append(ques)
            response_list.append(songid)
            verbose_res_list.append(song_name)
            uar_list.append(uar)
            freq_list.append(freq)
            duration_num_list.append(duration_num)
            accepted_num_list.append(accepted_num)
            labels_list.append(label)
            intent_list.append(intent)
            backend_list.append(backend)
            logmode_list.append(logmode)

        data = pd.DataFrame()
        data["question"] = queries_list
        data["action_verbose"] = verbose_res_list
        data["action"] = response_list
        data["uar"] = uar_list
        data["frequency"] = freq_list
        data["duration_num"] = duration_num_list
        data["accepted_num"] = accepted_num_list
        data["label"] = labels_list
        data["intent"] = intent_list
        data["backend"] = backend_list
        data["logmode"] = logmode_list
        return data


def generate_testcase_all(data: pd.DataFrame, date: str, app_name: str):
    data = AllTestcasesRowDataProcessing.extract_testcases(data)
    output_folder = OUTPUT_FOLDER.format(app_name)

    if not os.path.isdir(output_folder):
        os.makedirs(output_folder)

    merged_data = AllTestcasesRowDataProcessing.process_confirm_intent(data)
    merged_data = AllTestcasesRowDataProcessing.merge_data_by_song(merged_data)
    merged_data = merged_data.sort_values(by=["frequency"], ascending=False)

    res_path = file_utils.write_data_to_dags_folder(
        merged_data, file_utils.get_testcases_statistic_uar_name(date), output_folder
    )

    return res_path


def generate_testcase_all_a_week(date_lst: list, yst_date: str, app_name: str):
    """gen testcases for all this week

    Args:
        date_lst (list[str]): all dates in week
        yst_date (str): sunday in str

    Returns:
        res_path: path of result
    """
    logger.info("Loading data all...")
    list_df = []
    date_lst = sorted(date_lst, reverse=True)
    output_folder = OUTPUT_FOLDER.format(app_name)
    for date in date_lst:
        logger.info(f"----Loading data {date}...")

        if file_utils.is_testcases_weekly_data_existing(date, output_folder):
            week_df = file_utils.load_weekly_statistic_uar_data(date, output_folder)
            list_df.append(week_df)
            break
        else:
            date_df = file_utils.load_daily_statistic_uar_data(date, output_folder)
            list_df.append(date_df)
    list_df = list_df[::-1]
    merged_data = AllTestcasesRowDataProcessing.merge_daily_statistic_uar_data(list_df)
    merged_data = merged_data.sort_values(by=["frequency"], ascending=False)

    res_path = file_utils.write_data_to_dags_folder(
        merged_data,
        file_utils.get_testcases_statistic_uar_weekly_name(yst_date),
        output_folder,
    )

    return res_path


if __name__ == "__main__":
    import time

    s = time.time()

    date = "2022-12-06"

    # data = file_utils.load_logs_data(date, data_path=kiki_logs_constants.DATA_PATH)
    # data = process_logs_utils.filt_by_app_type(data, app_types=kiki_logs_constants.APP_ZINGS)
    # generate_testcase_all(data, date, "zing_apps")
    generate_testcase_all_a_week(
        date_lst=["2022-12-04", "2022-12-05", "2022-12-06"],
        yst_date=date,
        app_name="zing_apps",
    )

    print("Runtime:", time.time() - s)
