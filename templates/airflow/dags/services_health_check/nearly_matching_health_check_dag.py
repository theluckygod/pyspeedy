import os

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(CURRENT_FOLDER)

import datetime

import common_package.constants as constants
import common_package.little_dog_libs as little_dog_libs
import requests
from airflow import DAG
from airflow.decorators import task
from loguru import logger
from requests.exceptions import RequestException

SERVICE_NAME = "nearly_matching"
DAG_ID = f"{SERVICE_NAME}_health_check_dag"
SCHEDULE_INTERVAL = "0 * * * *"

default_args = {
    "owner": "thieunv",
    "retries": 5,
    "retry_delay": datetime.timedelta(seconds=30),
    "on_failure_callback": little_dog_libs.get_function_report_failure(
        little_dog_libs.get_failure_message(DAG_ID)
    ),
}


def health_check_func(host, port):
    URL = f"http://{host}:{port}/match_song"
    TIMEOUT = 1  # 1 second
    ques = b"em g\xc3\xa1i mua".decode("utf-8")
    label = b"em g\xc3\xa1i m\xc6\xb0a".decode("utf-8")
    params = {"text": ques, "commonId": "1887448340672368128"}

    logger.info(f"Request URL: {URL}")
    logger.info(f"Request params: {params}")

    try:
        response = requests.post(URL, json=params, timeout=TIMEOUT)
    except:
        raise RequestException("Request failure!!!")

    if response.status_code != 200:
        raise RequestException("Request failure!!!")
    else:
        logger.info("Request successfully!")

        name = response.json()["data"]["name"]
        if label in name.lower():
            logger.info("Response is matching!")
        else:
            raise ValueError("Response is not matching!!!")


@task.python
def nearly_match_prod_alpha():
    health_check_func(host="10.30.78.60", port=9110)


@task.python
def nearly_match_prod_beta():
    health_check_func(host="10.30.78.60", port=9111)


@task.python
def nearly_match_stg():
    health_check_func(host="10.30.78.101", port=9110)


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime.datetime(2023, 1, 1, tzinfo=constants.LOCAL_TIMEZONE),
    end_date=None,
    catchup=False,
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["health_check"],
) as dag:
    nearly_match_stg()
    nearly_match_prod_alpha()
    nearly_match_prod_beta()
