import common_package.kiki_logs_constants as kiki_logs_constants
import common_package.process_logs_utils as process_logs_utils
from common_package.process_logs.process_logs_task import ProcessLogsTask
from daily_logs_analysis_v2.filters.filter import Filter


class NearlyMatchFilter(Filter):
    LOGIC_NAME = "ask_song_nearly_match"

    def build_filting_task(self) -> ProcessLogsTask:
        return ProcessLogsTask().add_process_logs_command(
            process_logs_utils.filt_by_intents,
            intent=kiki_logs_constants.INTENT.NEARLY_MATCH_INTENT,
        )
