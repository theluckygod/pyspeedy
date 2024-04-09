import os

DAGS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(DAGS_FOLDER)

import csv
import datetime
import json

import common_package.constants as constants
import common_package.file_utils as file_utils
import common_package.kiki_logs_constants as kiki_logs_constants
import daily_logs_analysis.utils.export_music_duration
import requests
from airflow import DAG
from airflow.decorators import task
from loguru import logger

DATA_FOLDER = "/home/zdeploy/KiKi/dists/query_log_store/Kiki-history"
DATA_NAME_FORMAT = "kiki_mp3_song_duration_stats_{}.csv"
DATA_RANGE = 30
UPDATE_API = "http://10.30.78.101:8004/api/update_log_store"


@task.python(provide_context=True)
def determine_date(ti=None, **kwargs):
    return datetime.datetime.fromtimestamp(kwargs["ts"]) \
        .astimezone(constants.LOCAL_TIMEZONE) \
        .strftime(constants.DATE_FORMAT)


def prune_old_data(keep_date_list):
    keep_list = [DATA_NAME_FORMAT.format(date) for date in keep_date_list]
    old_files = os.listdir(DATA_FOLDER)
    for old_file in old_files:
        if old_file not in keep_list:
            logger.info(f"Prunning new logs date: {old_file}")
            os.remove(os.path.join(DATA_FOLDER, old_file))


def export_song_duration(date: str, app, out_path):
    df = file_utils.load_logs_data(date)
    df = daily_logs_analysis.utils.export_music_duration.export_song_duration_helper(df, app)
    path = os.path.join(out_path, f"kiki_mp3_song_duration_stats_{date}.csv")
    df.to_csv(path, sep=";", index=False,
              quoting=csv.QUOTE_ALL, encoding="utf-8")
    return path


def extract_new_logs(keep_date_list, app=kiki_logs_constants.APP.APP_ZINGS):
    for date in keep_date_list:
        logger.info(f"Extracting new logs date: {date}")

        if os.path.isfile(os.path.join(DATA_FOLDER, DATA_NAME_FORMAT.format(date))):
            continue
        export_song_duration(date, app, DATA_FOLDER)


@task.python
def update_data_logstore(date, ti=None):
    dt = datetime.datetime.strptime(date, constants.DATE_FORMAT)
    keep_date_list = [(dt - datetime.timedelta(i)).strftime(constants.DATE_FORMAT)
                      for i in range(DATA_RANGE)]

    if not os.path.isdir(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    prune_old_data(keep_date_list)
    extract_new_logs(keep_date_list)
    logger.info(f"Export song duration at {DATA_FOLDER}")


@task.python
def call_api_updating(ti=None):
    response = requests.post(url=UPDATE_API)
    logger.info(f"Response: {response.text}")
    if json.loads(response.text)["code"] != 0:
        raise ValueError("call_api_updating failure!!!")


with DAG(
    dag_id="logstore_data_updating_dag",
    start_date=datetime.datetime(2023, 1, 1, tzinfo=constants.LOCAL_TIMEZONE),
    end_date=None,
    catchup=False,
    schedule_interval="30 05 * * *",
    tags=["updater"]
) as dag:
    """run trigger w/ config: {"date": "YYYY-mm-dd"}
    """
    date = determine_date()
    update_data_logstore(date) >> call_api_updating()
