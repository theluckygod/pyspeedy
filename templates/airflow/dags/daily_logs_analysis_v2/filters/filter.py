from abc import ABC, abstractmethod

from beartype import beartype
from common_package.process_logs.commands import Command
from common_package.process_logs.process_logs_task import ProcessLogsTask


class Filter(ABC):
    LOGIC_NAME = "filter"
    
    def get_logic_name(self) -> str:
        """Return the logic name of the filter
        """
        return self.__class__.LOGIC_NAME
    
    @abstractmethod
    def build_filting_task(self) -> ProcessLogsTask:
        """Build a ProcessLogsTask that contains all the filting commands
        Example:
            from common_package.process_logs_utils import filt_by_intents, filt_by_logmode
            return ProcessLogsTask() \
                    .add_command(filt_by_intents, "music.ask_music.ask_play_lyrics") \
                    .add_command(filt_by_logmode, "lyrics_from_zmp3")
        """
        pass
    

    # @beartype
    # def build_task(self, read_csv: Command) -> ProcessLogsTask:
    #     return ProcessLogsTask(command=read_csv) \
    #             .append_task(self.build_filting_task())