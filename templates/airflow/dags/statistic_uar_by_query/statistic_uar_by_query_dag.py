import os

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(CURRENT_FOLDER)

import datetime

import common_package.constants as common_constants
import common_package.dags_libs as dags_libs
import common_package.file_utils as file_utils
import common_package.kiki_logs_constants as kiki_logs_constants
import common_package.process_logs_utils as process_logs_utils
import statistic_uar_by_query.constants as constants
from airflow import DAG
from airflow.decorators import task
from loguru import logger
from statistic_uar_by_query.generate_testcase_all import (
    generate_testcase_all, generate_testcase_all_a_week)
from statistic_uar_by_query.monitor_by_id import (load_whitelist_id,
                                                  monitoring_by_id)


def generate_all_testcase(date, app):
    data = file_utils.load_logs_data(date)
    logger.info("Load data successfully!")
    data = process_logs_utils.filt_by_app_type(data, app_types=app)
    path = generate_testcase_all(data, date, process_logs_utils.get_app_name(app))
    return path


@task.python
def generate_all_zmp3_testcase(date, ti=None):
    return generate_all_testcase(date, app=kiki_logs_constants.APP.APP_ZINGS)


@task.python
def generate_all_car_testcase(date, ti=None):
    return generate_all_testcase(date, app=kiki_logs_constants.APP.APP_CARS)


@task.python
def generate_all_zmp3_testcase_this_week(date, ti=None):
    yst_date_str = date
    yesterday_dt = datetime.datetime.strptime(
        yst_date_str, common_constants.DATE_FORMAT)
    weekday = yesterday_dt.weekday()
    date_list = []
    for delta in range(weekday + 1):
        date = yesterday_dt - datetime.timedelta(delta)
        date_str = date.strftime(common_constants.DATE_FORMAT)
        date_list.append(date_str)
    path = generate_testcase_all_a_week(
        date_list, yst_date_str, process_logs_utils.get_app_name(kiki_logs_constants.APP.APP_ZINGS))
    return path


@task.python
def generate_all_car_testcase_this_week(date, ti=None):
    yst_date_str = date
    yesterday_dt = datetime.datetime.strptime(
        yst_date_str, common_constants.DATE_FORMAT)
    weekday = yesterday_dt.weekday()
    date_list = []
    for delta in range(weekday + 1):
        date = yesterday_dt - datetime.timedelta(delta)
        date_str = date.strftime(common_constants.DATE_FORMAT)
        date_list.append(date_str)
    path = generate_testcase_all_a_week(
        date_list, yst_date_str, process_logs_utils.get_app_name(kiki_logs_constants.APP.APP_CARS))
    return path


@task.python
def monitoring_by_id_zmp3(date, ti=None):
    data = file_utils.load_daily_statistic_uar_data(
        date, common_constants.OUTPUT_PATH.ZING_TESTCASES_PATH)
    logger.info("Load data successfully!")

    whitelist_id = load_whitelist_id()
    logger.info("Load data whitelist id successfully!")

    if whitelist_id is not None:
        monitoring_by_id(data, date, whitelist_id)
    else:
        logger.error("Load data whitelist id failure!")
        raise Exception("Can not load data whitelist id!!!")


with DAG(
    dag_id="statistic_uar_by_query_dag",
    start_date=datetime.datetime(2022, 5, 25, tzinfo=common_constants.LOCAL_TIMEZONE),
    end_date=None,
    catchup=False,
    schedule_interval=None,
    tags=["tracking", "lyrics", "nearly_match"],
) as dag:
    """run trigger w/ config: {"date": "YYYY-mm-dd"}
    """
    date = dags_libs.determine_date()
    generate_all_zmp3 = generate_all_zmp3_testcase(date)
    monitor_by_id_zmp3 = monitoring_by_id_zmp3(date)
    generate_all_zmp3_this_week = generate_all_zmp3_testcase_this_week(date)
    generate_all_car = generate_all_car_testcase(date)
    generate_all_car_this_week = generate_all_car_testcase_this_week(date)
    [generate_all_zmp3, generate_all_car] << date
    [generate_all_zmp3_this_week, monitor_by_id_zmp3] << generate_all_zmp3
    generate_all_car_this_week << generate_all_car
