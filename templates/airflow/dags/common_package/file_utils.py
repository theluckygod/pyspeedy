import csv
import os

import common_package.constants as constants
import pandas as pd

LOGS_DATE_NAME = "logs_date_{}.csv"
TESTCASES_NAME = "testcases_all_nofilter_confuse_{}.csv"
TESTCASES_WEEKLY_NAME = "testcases_weekly_all_nofilter_confuse_{}.csv"


def get_logs_data_path(date, data_path=constants.OUTPUT_PATH.LOGS_PATH):
    return os.path.join(data_path, LOGS_DATE_NAME.format(date))


def get_testcases_statistic_uar_name(date):
    return TESTCASES_NAME.format(date)


def get_testcases_statistic_uar_weekly_name(date):
    return TESTCASES_WEEKLY_NAME.format(date)


def get_testcases_statistic_uar_path(date, data_path):
    return os.path.join(data_path, get_testcases_statistic_uar_name(date))


def get_testcases_statistic_uar_weekly_path(date, data_path):
    return os.path.join(data_path, TESTCASES_WEEKLY_NAME.format(date))


def is_logs_data_existing(date, data_path=constants.OUTPUT_PATH.LOGS_PATH):
    return os.path.isfile(get_logs_data_path(date, data_path))


def load_logs_data(date, data_path=constants.OUTPUT_PATH.LOGS_PATH, columns=None):
    file_path = get_logs_data_path(date, data_path)
    return load_data_from_file(file_path, columns=columns)


def write_logs_data(df, date, data_path=constants.OUTPUT_PATH.LOGS_PATH):
    if not os.path.isdir(data_path):
        os.makedirs(data_path)
    path = get_logs_data_path(date, data_path)
    df.to_csv(path, sep=",", encoding="utf-8", quoting=csv.QUOTE_ALL)
    return path


def write_data_to_dags_folder(df, file_name, data_path):
    if not os.path.isdir(data_path):
        os.makedirs(data_path)
    file_path = os.path.join(data_path, file_name)
    df.to_csv(file_path, index=False, sep=",", encoding="utf-8", quoting=csv.QUOTE_ALL)
    return file_path


def is_data_existing(file_name, data_path):
    return os.path.isfile(os.path.join(data_path, file_name))


def load_data_from_file(file_path, columns=None):
    return pd.read_csv(file_path, sep=",", encoding="utf-8", usecols=columns)


def load_data(file_name, data_path):
    return load_data_from_file(os.path.join(data_path, file_name))


def load_testcases(date, data_path, is_weekly=False):
    if is_weekly:
        file_path = get_testcases_statistic_uar_weekly_path(date, data_path)
    else:
        file_path = get_testcases_statistic_uar_path(date, data_path)
    return load_data_from_file(file_path)


def load_daily_statistic_uar_data(date, data_path):
    file_path = get_testcases_statistic_uar_path(date, data_path)
    return load_data_from_file(file_path)


def load_weekly_statistic_uar_data(date, data_path):
    file_path = get_testcases_statistic_uar_weekly_path(date, data_path)
    return load_data_from_file(file_path)


def is_testcases_data_existing(date, data_path):
    file_path = get_testcases_statistic_uar_weekly_path(date, data_path)
    return os.path.isfile(file_path)


def is_testcases_weekly_data_existing(date, data_path):
    file_path = get_testcases_statistic_uar_weekly_path(date, data_path)
    return os.path.isfile(file_path)
