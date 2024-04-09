from __future__ import annotations

from enum import Enum

import common_package.process_logs.utils as utils
import pandas as pd
from common_package.process_logs.post_processing_task import PostProcessingTask
from common_package.process_logs.process_logs_task import ProcessLogsTask


class UpdateState(Enum):
    """
    Enum class for the update state of the logic
    """

    OK = "OK"
    FAIL = "FAIL"


class LogsTrackingTask(PostProcessingTask, ProcessLogsTask):

    @staticmethod
    def create_dummy_task(result) -> LogsTrackingTask:
        """create_dummy_task creates a dummy task with first command is return result

        Args:
            result (any)

        Returns:
            LogicsTrackingTask
        """
        return LogsTrackingTask().add_command(utils.dummy_result_func, result)


class WrappedDataFrame(pd.DataFrame):
    def __init__(self, df: pd.DataFrame):
        super().__init__(df)

    def __eq__(self, other: WrappedDataFrame) -> bool:
        if isinstance(other, pd.DataFrame):
            other = WrappedDataFrame(other)

        if not isinstance(other, WrappedDataFrame):
            return False

        return self.keys().to_list() == other.keys().to_list() and self.equals(other)
