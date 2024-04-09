import os

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(CURRENT_FOLDER)

import datetime
from copy import deepcopy

import common_package.constants as constants
import common_package.dags_libs as dags_libs
import common_package.little_dog_libs as report_libs
import crawl_daily_logs.crawl_tools as crawl_tools
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from loguru import logger

DAGS_ID = "crawl_daily_logs_dag"
TRIGGER_DOWNSTREAM_FILENAME = "trigger_downstream_dags.txt"
TRIGGER_DOWNSTREAM_DAG_ID_PATH = os.path.join(constants.DAGS_PATH, constants.DAG.CRAWL_DAILY_LOGS_DAG, TRIGGER_DOWNSTREAM_FILENAME)


@task.python
def crawl_logs(date, **kwargs):
    _kwargs = deepcopy(kwargs["dag_run"].conf)
    _kwargs["date"] = date
    if _kwargs.get("is_from_date", False):
        is_successful = crawl_tools.update_database_from_date(
            _kwargs.get("date"), _kwargs.get("is_forced", False))
        return is_successful
    else:
        path = crawl_tools.crawl_daily_logs(
            _kwargs.get("date"), _kwargs.get("is_forced", False))
        return path


def gen_trigger_task_list(date):
    with open(TRIGGER_DOWNSTREAM_DAG_ID_PATH, "r", encoding="utf-8") as f:
        dag_id_lst = [line.strip() for line in f.readlines()]

    tasks = []
    for dag_id in dag_id_lst:
        trigger_task = TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
            conf={"date": str(date)},
            wait_for_completion=False,
        )
        tasks.append(trigger_task)
    return tasks


default_args = {
    "owner": "thieunv",
    "retries": 10,
    "retry_delay": datetime.timedelta(minutes=15),
    "on_failure_callback": report_libs.get_function_report_failure(report_libs.get_failure_message(DAGS_ID)),
}

with DAG(
    dag_id=DAGS_ID,
    default_args=default_args,
    start_date=datetime.datetime(
        2022, 5, 25, tzinfo=constants.LOCAL_TIMEZONE),
    end_date=None,
    catchup=False,
    schedule_interval="00 05 * * *",
    tags=["tracking", "crawl_logs"],
) as dag:
    """run trigger w/ config: {"date": "YYYY-mm-dd"}
    """
    date = dags_libs.determine_date()
    crawl_logs_task = crawl_logs(date)
    trigger_task_list = gen_trigger_task_list(date)
    trigger_task_list << crawl_logs_task << date
