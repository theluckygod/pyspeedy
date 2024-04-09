import os

import dateutil

LOCAL_TIMEZONE = dateutil.tz.gettz("Asia/Ho_Chi_Minh")
DATE_FORMAT = "%Y-%m-%d"
DAGS_PATH = "/home/lap14351/airflow/dags/"
DEPLOY_TYPE = "stg"


class DAG:
    CRAWL_DAILY_LOGS_DAG = "crawl_daily_logs"
    DAILY_LOGS_ANALYSIS = "daily_logs_analysis"
    STATISTIC_UAR_BY_QUERY = "statistic_uar_by_query"


class OUTPUT_PATH:
    LOGS_PATH = os.path.join(DAGS_PATH, DAG.CRAWL_DAILY_LOGS_DAG, "outputs")
    DAILY_LOGS_ANALYSIS_PATH = os.path.join(
        DAGS_PATH, DAG.DAILY_LOGS_ANALYSIS, "outputs"
    )
    ZING_TESTCASES_PATH = os.path.join(
        DAGS_PATH, DAG.STATISTIC_UAR_BY_QUERY, "zing_apps_outputs"
    )
    CAR_TESTCASES_PATH = os.path.join(
        DAGS_PATH, DAG.STATISTIC_UAR_BY_QUERY, "car_apps_outputs"
    )
