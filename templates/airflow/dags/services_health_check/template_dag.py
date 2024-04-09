import os
import sys

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURRENT_FOLDER)
os.chdir(CURRENT_FOLDER)

import datetime

import dateutil
import requests
from airflow import DAG
from airflow.decorators import task
from loguru import logger
from requests.exceptions import RequestException

SERVICE_NAME = "template_service"
DAG_ID = f"{SERVICE_NAME}_health_check_dag"
SCHEDULE_INTERVAL = "0 * * * *"

default_args = {
    "owner": "thieunv",
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=5),
    "on_failure_callback": utils.get_function_report_failure(
        f"[AIRFLOW] Execute dag_id={DAG_ID} failure!!!"
    ),
}


@task.python
def health_check_func():
    HOST = "0.0.0.0"
    URL = f"http://{HOST}:9111/match_song"
    TIMEOUT = 1  # 1 second
    ques = "em gái mua"
    label = "em gái mưa"
    params = {"text": ques, "commonId": "1887448340672368128"}
    response = requests.post(URL, json=params, timeout=TIMEOUT)
    if response.status_code != 200:
        raise RequestException("Request failure!!!")
    else:
        logger.info("Request successfully!")

        name = response.json()["data"]["name"]
        if label in name.lower():
            logger.info("Response is matching!")
        else:
            raise ValueError("Response is not matching!!!")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime.datetime(
        2023, 1, 1, tzinfo=dateutil.tz.gettz("Asia/Ho_Chi_Minh")
    ),
    end_date=None,
    catchup=False,
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["health_check"],
) as dag:
    health_check_func()
