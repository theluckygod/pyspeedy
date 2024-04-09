from __future__ import annotations

from abc import ABC, abstractmethod

import common_package.constants as constants
import common_package.process_logs_utils as process_logs_utils
import pandas as pd
from beartype import beartype
from beartype.typing import Dict, List
from common_package.process_logs.commands import Command
from common_package.process_logs.process_logs_command import ProcessLogsCommand
from daily_logs_analysis_v2.analysis.analysis import Analysis
from daily_logs_analysis_v2.utils.utils import UpdateState


class MusicTrackingAnalysis(Analysis):

    @staticmethod
    def extract_info(df: pd.DataFrame, *args, **kwargs) -> dict:
        info = {}
        info["num"] = process_logs_utils.count_intent(df)
        info["num_responded"] = process_logs_utils.count_intent(df, is_dur=True)
        info["num_accepted"] = process_logs_utils.count_accepted_intent(df)
        return info

    @staticmethod
    def get_extracting_info_task(*args, **kwargs):
        return ProcessLogsCommand(__class__.extract_info, *args, **kwargs)

    @staticmethod
    @beartype
    def set_key(info: Dict, *args, **kwargs) -> dict:
        key_info = __class__.get_default_key(*args, **kwargs)
        info.update(key_info)
        return info

    @staticmethod
    @beartype
    def get_default_key(
        device_type: str,
        date: str,
        backend: str,
        logic: str,
        state: UpdateState,
        description: str = None,
    ) -> dict:
        key_info = {
            "device_type": device_type,
            "date": date,
            "backend": backend,
            "logic": logic,
            "state": state.value,
        }

        if description:
            key_info["description"] = description

        return key_info

    @staticmethod
    @beartype
    def get_set_key_task(*args, **kwargs):
        return ProcessLogsCommand(__class__.set_key, *args, **kwargs)

    @staticmethod
    @beartype
    def get_default_failure_task(*args, **kwargs):
        return Command(__class__.get_default_key, *args, **kwargs)

    @staticmethod
    @abstractmethod
    @beartype
    def analysis(date: str, deploy_type: str, **kwargs: Dict) -> None:
        pass
