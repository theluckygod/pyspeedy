import common_package.kiki_logs_constants as kiki_logs_constants
import common_package.process_logs_utils as process_logs_utils
from common_package.process_logs.process_logs_task import ProcessLogsTask
from daily_logs_analysis_v2.filters.filter import Filter


class ZMP3MusicFilter(Filter):
    LOGIC_NAME = "zmp3_music"

    @staticmethod
    def build_filting_task():
        return ProcessLogsTask().add_process_logs_command(
            process_logs_utils.filt_by_intents,
            kiki_logs_constants.INTENT.ASK_ZMP3_MUSIC_INTENTS,
        )
