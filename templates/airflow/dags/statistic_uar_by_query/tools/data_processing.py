import json

import common_package.kiki_logs_constants as kiki_logs_constants
import common_package.process_logs_utils as process_logs_utils
import pandas as pd
import statistic_uar_by_query.constants as constants
import statistic_uar_by_query.tools.get_score as get_score
import statistic_uar_by_query.tools.rule_based as rule_based


def extract_logmode_to_list(text, delimit=", "):
    if not text:
        return None
    return text.split(delimit)

class DataProcessingTools:
    def _get_action_field(row, _IS_USING_MP3_ACTION=True):
        acts = json.loads(row["actions"])
        acts = get_score.clean_action(acts)

        if not _IS_USING_MP3_ACTION:
            # act0
            act_text = rule_based.get_action_text(acts[0])
            act_text = act[len("Đang mở "):]
        else:
            # act_mp3
            act = get_score.extract_mp3_action(acts)
            act_text = get_score.extract_text_mp3_action(act)
        return act_text

    def _get_except_action_field(row):
        act = json.loads(row["actions"])
        if len(act) > 2 and "payload" in act[1] and "utter" in act[1]["payload"]:
            return act[1]["payload"]["utter"]
        return ""        

    def get_action(row):
        try:
            act_text = DataProcessingTools._get_action_field(row)
        except KeyError:
            act_text = DataProcessingTools._get_except_action_field(row)
        except TypeError:
            act_text = ""
            
        if not act_text and row["media_title"] and row["media_title"] != "NULL":
            act_text = row["media_title"]
            
        return act_text

    def _get_songID_action(row, _IS_USING_MP3_ACTION=True):
        acts = json.loads(row["actions"])
        acts = get_score.clean_action(acts)

        if not _IS_USING_MP3_ACTION:
            # act0
            act_text = rule_based.get_action_text(acts[0])
            act_text = act[len("Đang mở "):]
        else:
            # act_mp3
            act = get_score.extract_mp3_action(acts)
            if "type" in act["payload"] and "idEncode" in act["payload"]:
                act_text = act["payload"]["type"] + "_" + act["payload"]["idEncode"]
            else:
                raise KeyError
        return act_text

    def get_song_album_ID(row):
        try:
            act_text = DataProcessingTools._get_songID_action(row)
        except KeyError:
            act_text = ""
        except TypeError:
            act_text = ""
            
        if not act_text and row["media_id"]:
            act_text = "video" + "_" + row["media_id"]
            
        return act_text

    def get_duration(row):
        if not row["duration"]:
            raise TypeError
        return row["duration"]

    def get_question(row):
        if not row["question"]:
            raise TypeError
        return row["question"]

    def get_user_id(row):
        if row["common_user_id"] is None:
            raise TypeError
        return str(row["common_user_id"])

    def get_time(row):
        if row["created_time"] is None:
            raise TypeError
        return str(row["created_time"])

    def get_device(row):
        if row["device_type"] is None:
            raise TypeError
        return row["device_type"]

    def get_core_intent(row):
        if row["intent"] is None:
            raise TypeError
        return row["intent"].split(".")[-1]

    def get_logmode(row):
        if row["actions"] is None:
            raise TypeError
        return extract_logmode_to_list(process_logs_utils.extract_info_from_re(row["actions"], kiki_logs_constants.PATTERN.LOGMODE_PATTERN))

    def is_satisfied_uar_criteria(freq, uar, type):
        if freq >= constants._QUERIES_THRESH[type]:
            if uar >= constants._ACCEPTED_QUERIES_THRESH[type]: # >= 30% queries"s uar 
                label = True
            elif uar < constants._REJECTED_QUERIES_THRESH[type]: # < 5% queries"s uar 
                label = False
            else:
                label = "Confuse" #raise KeyError
        else:
            if freq * uar - constants._ACCEPTED_QUERIES_THRESH[type] * freq >= constants._ACCEPTED_SCORE_THRESH[type]:
                label = True
            elif freq * uar - constants._REJECTED_QUERIES_THRESH[type] * freq < constants._REJECTED_SCORE_THRESH[type]:
                label = False
            else:
                label = "Confuse" #raise KeyError
        return label

    def process_song_and_album(ques_df, uar, type):
        if len(pd.unique(ques_df["userid"])) <= constants._USER_DIF_THRESH[type]:
            return "Confuse"
        return DataProcessingTools.is_satisfied_uar_criteria(len(ques_df), uar, type)

    def process_song(ques_df, uar):
        return DataProcessingTools.process_song_and_album(ques_df, uar, "song")

    def process_album(ques_df, uar):
        return DataProcessingTools.process_song_and_album(ques_df, uar, "album")
    
    def process_video(ques_df, uar):
        return DataProcessingTools.process_song_and_album(ques_df, uar, "song")

    def get_uar(df):
        filted_df = df[df["is_accepted"].notnull()]
        return len(filted_df[filted_df["is_accepted"] == True]) / len(filted_df)

    def is_confirm_intent(intent):
        if intent in constants.CONFIRM_OPEN_MP3_INTENTS:
            return True
        return False