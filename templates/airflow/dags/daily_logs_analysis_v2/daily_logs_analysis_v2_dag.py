import os

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(CURRENT_FOLDER)

import datetime

import common_package.constants as constants
import common_package.dags_libs as dags_libs
from airflow import DAG
from airflow.decorators import task
from daily_logs_analysis_v2.analysis.intent_tracking import \
    IntentTrackingAnalysis
from daily_logs_analysis_v2.analysis.logics_tracking import \
    LogicsTrackingAnalysis
from daily_logs_analysis_v2.analysis.logmode_tracking import \
    LogmodeTrackingAnalysis
from daily_logs_analysis_v2.analysis.suggestion_tracking import \
    SuggestionTrackingAnalysis
from daily_logs_analysis_v2.analysis.user_acceptance_stats_v4 import \
    UserAcceptanceStatsV4Analysis
from loguru import logger


@task.python
def do_analysis_logics_tracking(date, ti=None, **kwargs):
    LogicsTrackingAnalysis.analysis(date=date, deploy_type="prod")
    
@task.python
def do_analysis_logmode_tracking(date, ti=None, **kwargs):
    LogmodeTrackingAnalysis.analysis(date=date, deploy_type="prod")
    
@task.python
def do_analysis_intent_tracking(date, ti=None, **kwargs):
    IntentTrackingAnalysis.analysis(date=date, deploy_type="prod")

@task.python
def do_analysis_user_acceptance_stats_v4(date, ti=None, **kwargs):
    UserAcceptanceStatsV4Analysis.analysis(date=date, deploy_type="prod")

@task.python
def do_analysis_suggestion_tracking(date, ti=None, **kwargs):
    SuggestionTrackingAnalysis.analysis(date=date, deploy_type="prod")

with DAG(
    dag_id="daily_logs_analysis_v2_dag",
    start_date=datetime.datetime(2023, 5, 1, tzinfo=constants.LOCAL_TIMEZONE),
    end_date=None,
    catchup=False,
    schedule_interval=None,
    tags=["analysis"],
) as dag:
    """run trigger w/ config: {"date": "YYYY-mm-dd"}
    """
    date = dags_libs.determine_date()
    # do_analysis_logics_tracking(date)
    do_analysis_logmode_tracking(date)
    do_analysis_intent_tracking(date)
    do_analysis_user_acceptance_stats_v4(date)
    do_analysis_suggestion_tracking(date)
