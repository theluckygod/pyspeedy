import os

CURRENT_FOLDER = os.path.dirname(os.path.abspath(__file__))
os.chdir(CURRENT_FOLDER)

import datetime

import common_package.constants as constants
from daily_logs_analysis_v2.analysis.intent_tracking import IntentTrackingAnalysis
from daily_logs_analysis_v2.analysis.logics_tracking import LogicsTrackingAnalysis
from daily_logs_analysis_v2.analysis.logmode_tracking import LogmodeTrackingAnalysis
from daily_logs_analysis_v2.analysis.suggestion_tracking import (
    SuggestionTrackingAnalysis,
)
from daily_logs_analysis_v2.analysis.user_acceptance_stats_v4 import (
    UserAcceptanceStatsV4Analysis,
)
from loguru import logger


def do_analysis_logics_tracking(date, ti=None, **kwargs):
    LogicsTrackingAnalysis.analysis(date=date, deploy_type="prod")


def do_analysis_logmode_tracking(date, ti=None, **kwargs):
    LogmodeTrackingAnalysis.analysis(date=date, deploy_type="prod")


def do_analysis_intent_tracking(date, ti=None, **kwargs):
    IntentTrackingAnalysis.analysis(date=date, deploy_type="prod")


def do_analysis_user_acceptance_stats_v4(date, ti=None, **kwargs):
    UserAcceptanceStatsV4Analysis.analysis(date=date, deploy_type="prod")


def do_analysis_suggestion_tracking(date, ti=None, **kwargs):
    SuggestionTrackingAnalysis.analysis(date=date, deploy_type="prod")


if __name__ == "__main__":

    import pandas as pd

    from_date = "2023-08-09"
    to_date = "2023-08-10"
    for date in pd.date_range(from_date, to_date):
        date = date.strftime("%Y-%m-%d")

        # do_analysis_logics_tracking(date)
        # do_analysis_logmode_tracking(date)
        do_analysis_intent_tracking(date)
        # do_analysis_user_acceptance_stats_v4(date)
        # do_analysis_suggestion_tracking(date)
