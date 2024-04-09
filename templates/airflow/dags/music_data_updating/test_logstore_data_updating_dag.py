import os
import sys

DAGS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(DAGS_FOLDER)
sys.path.append(os.path.join(DAGS_FOLDER, "daily_logs_analysis"))
os.chdir(DAGS_FOLDER)

import csv
import datetime
import json

import daily_logs_analysis.utils.constants as constants
import daily_logs_analysis.utils.export_music_duration
import daily_logs_analysis.utils.utils
import requests
from loguru import logger

DATA_FOLDER = "/home/lap14351/KiKi/dists/query_log_store/Kiki-history"
DATA_NAME_FORMAT = "kiki_mp3_song_duration_stats_{}.csv"
DATA_RANGE = 30
UPDATE_API = "http://10.30.78.101:8004/api/update_log_store"


def prune_old_data(keep_date_list):
    print("keep_date_list", keep_date_list)
    keep_list = []
    for date in keep_date_list:
        print("date", date, DATA_NAME_FORMAT, DATA_NAME_FORMAT.format(date))
        keep_list.append(DATA_NAME_FORMAT.format(date))
    # keep_list = [DATA_NAME_FORMAT.format(date) for date in keep_date_list]
    print("keep_list", keep_list)
    old_files = os.listdir(DATA_FOLDER)
    for old_file in old_files:
        if old_file not in keep_list:
            os.remove(os.path.join(DATA_FOLDER, old_file))


def export_song_duration(date: str, app, out_path):
    df = daily_logs_analysis.utils.utils.load_data(date, data_path=constants.DATA_PATH)
    df = daily_logs_analysis.utils.export_music_duration.export_song_duration_helper(
        df, app
    )
    path = os.path.join(out_path, f"kiki_mp3_song_duration_stats_{date}.csv")
    df.to_csv(path, sep=";", index=False, quoting=csv.QUOTE_ALL, encoding="utf-8")
    return path


def extract_new_logs(keep_date_list, app=constants.APP.APP_ZINGS):
    for date in keep_date_list:
        if os.path.isfile(os.path.join(DATA_FOLDER, DATA_NAME_FORMAT.format(date))):
            continue
        export_song_duration(date, app, DATA_FOLDER)


def update_data_logstore(date):
    dt = datetime.datetime.strptime(date, constants.DATE_FORMAT)
    keep_date_list = [
        (dt - datetime.timedelta(i)).strftime(constants.DATE_FORMAT)
        for i in range(DATA_RANGE)
    ]

    if not os.path.isdir(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    prune_old_data(keep_date_list)
    extract_new_logs(keep_date_list)
    logger.info(f"Export song duration at {DATA_FOLDER}")


def call_api_updating():
    response = requests.post(url=UPDATE_API)
    logger.info(f"Response: {response.text}")
    if json.loads(response.text)["code"] == 0:
        raise ValueError("call_api_updating failure!!!")


if __name__ == "__main__":
    date = "2023-01-03"
    update_data_logstore(date)
    call_api_updating()
