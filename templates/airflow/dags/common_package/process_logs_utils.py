import re

import common_package.kiki_logs_constants as kiki_logs_constants
import pandas as pd


def extract_info_from_re(text, pattern):
    if not isinstance(text, str):
        return None

    m = re.search(pattern, text)
    if m:
        found = m.group(1)
        return found
    return None


def get_app_name(app: kiki_logs_constants.APP):
    if app == kiki_logs_constants.APP.APP_ZINGS:
        return "zing_apps"
    elif app == kiki_logs_constants.APP.APP_CARS:
        return "car_apps"
    elif app == kiki_logs_constants.APP.ALL_APPS:
        return "apps"
    elif app == kiki_logs_constants.APP.APP_KIKIS:
        return "kiki_apps"
    else:
        raise ValueError(f"Unknown app_type {app}!!!")


def count_intent(df, intent=None, is_dur=False):
    if len(df) == 0:
        return 0
    interested_df = filt_by_intents(df, intent)
    if is_dur:
        interested_df = filt_by_duration(interested_df)

    return len(interested_df)


def count_intent_by_actions(df, trace, intent=None, is_dur=False):
    assert "duration" in df.columns, "duration is not in df"

    if len(df) == 0:
        return 0
    interested_df = filt_by_intents(df, intent)
    if is_dur:
        interested_df = filt_by_duration(interested_df)
    interested_df = filt_actions_by_trace(interested_df, trace)

    return len(interested_df)


def count_accepted_intent(df, intent=None):
    assert "is_accepted" in df.columns, "is_accepted is not in df"

    if len(df) == 0:
        return 0
    interested_df = filt_by_intents(df, intent)
    return len(interested_df[interested_df["is_accepted"] == True])


def count_short_songs(df, intent=None):
    import crawl_daily_logs.crawler as crawler

    assert "song_length" in df.columns, "song_length is not in df"
    assert "type" in df.columns, "type is not in df"

    if len(df) == 0:
        return 0
    interested_df = filt_by_intents(df, intent)
    interested_df = filt_by_duration(interested_df)

    return len(
        interested_df[
            (interested_df["type"] == "song")
            & (interested_df["song_length"].notnull())
            & (interested_df["song_length"] > 0)
            & (interested_df["song_length"] <= crawler.SONG_LENGTH_THRESHOLD)
        ]
    )


def get_uar(df, intent):
    interested_df = filt_by_intents(df, intent)
    interested_df = filt_by_duration(interested_df)
    count = len(interested_df)
    cnt_correct = len(interested_df[interested_df["is_accepted"] == True])

    return cnt_correct / count


def filt_by_intents(df, intent, is_not_contain=False):
    if intent is None:
        return df

    interested_df = df

    if isinstance(intent, str):
        selector = df["intent"] == intent
    elif isinstance(intent, list):
        selector = df["intent"].isin(intent)

    if is_not_contain:
        selector = ~selector
    return interested_df[selector]


def filt_by_slots_num(df, amount, type="=="):
    """filt intents by the number of slots

    Args:
        df (pd.DataFrame)
        amount (int): a number of slots
        type (str, optional): type of comparison. Defaults to "==".

    Returns:
        _type_: _description_
    """
    interested_df = df
    interested_df["slots_num"] = interested_df["slots"].str.count("SlotName")
    if isinstance(amount, int):
        if type == "==":
            interested_df = df[df["slots_num"] == amount]
        elif type == ">=":
            interested_df = df[df["slots_num"] >= amount]
        elif type == "<=":
            interested_df = df[df["slots_num"] <= amount]
        elif type == "<":
            interested_df = df[df["slots_num"] < amount]
        elif type == ">":
            interested_df = df[df["slots_num"] > amount]
        else:
            raise ValueError(f"Does not support {type}!")

    elif isinstance(amount, list):
        if type != "==":
            raise ValueError(f"Can not use type {type} with amount list")
        interested_df = df[df["slots_num"].isin(amount)]
    interested_df = interested_df.drop("slots_num", axis=1)
    return interested_df


def filt_by_slot(df, values, type="name", is_not_contain=False):
    if type == "name":
        type = '"SlotName"'
    elif type == "value":
        type = '"SlotValue"'
    elif type == "type":
        type = '"SlotType"'
    else:
        raise ValueError(f"Type must be in [name, value, type] not {type}")

    selector = None
    if isinstance(values, str):
        selector = df["slots"].str.contains(f'{type}:"{values}"', na=False)
    elif isinstance(values, list):
        selector = df["slots"].str.contains("|".join(values), na=False)

    if is_not_contain:
        selector = ~selector

    return df[selector]


def filt_by_slot_name(df, slot_name, is_not_contain=False):
    return filt_by_slot(df, slot_name, type="name", is_not_contain=is_not_contain)


def filt_by_slot_value(df, slot_name, is_not_contain=False):
    return filt_by_slot(df, slot_name, type="value", is_not_contain=is_not_contain)


def filt_by_slot_type(df, slot_name, is_not_contain=False):
    return filt_by_slot(df, slot_name, type="type", is_not_contain=is_not_contain)


def filt_slots_by_trace(df: pd.DataFrame, trace, is_not_contain=False):
    selector = None
    if isinstance(trace, str):
        selector = df["slots"].str.contains(trace, na=False)
    elif isinstance(trace, list):
        selector = df["slots"].str.contains("|".join(trace), na=False)

    if is_not_contain:
        selector = ~selector

    return df[selector]


def filt_by_duration(df):
    return df[df["duration"].notnull()]


def wrap_log_mode(log_mode):
    return rf"[ \[]{log_mode}[,\]]"


def filt_by_logmode(df: pd.DataFrame, log_mode, is_not_contain=False):
    if isinstance(log_mode, str):
        selector = df["actions"].str.contains(wrap_log_mode(log_mode), na=False)
    elif isinstance(log_mode, list) or isinstance(log_mode, tuple):
        wrap_log_modes = [wrap_log_mode(lm) for lm in log_mode]
        selector = df["actions"].str.contains("|".join(wrap_log_modes), na=False)
    else:
        raise ValueError(f"Unknown this type {type(log_mode)}!!!")
    if is_not_contain:
        selector = ~selector
    return df[selector]


def filt_by_backend(df: pd.DataFrame, backend: str):
    return df[df["backend"] == backend]


def filt_by_app_type(df: pd.DataFrame, app_types: list):
    """fil df by app type

    Args:
        df (pd.DataFrame)
        app_type (list): contains a list of app_type

    Returns:
        _type_: _description_
    """
    if app_types == kiki_logs_constants.APP.ALL_APPS:
        return df
    elif app_types == kiki_logs_constants.APP.APP_CARS:
        filter_df = df[
            df["device_type"].str.contains(
                kiki_logs_constants.APP.APP_CARS, na=False, regex=True
            )
        ]
        filter_df = filter_df[
            ~filter_df["device_type"].isin(kiki_logs_constants.APP.APP_TV)
        ]
        return filter_df
    else:
        return df[df["device_type"].isin(app_types)]


def filt_by_device_type(df: pd.DataFrame, device_type: str):
    """filt df by app type

    Args:
        df (pd.DataFrame)
        device_type (str): device type

    Returns:
        _type_: _description_
    """
    return df[df["device_type"] == device_type]


def filt_actions_by_trace(df: pd.DataFrame, trace, is_not_contain=False):
    if isinstance(trace, str):
        selector = df["actions"].str.lower().str.contains(trace, na=False)
    elif isinstance(trace, list):
        selector = df["actions"].str.lower().str.contains("|".join(trace), na=False)

    if is_not_contain:
        selector = ~selector

    return df[selector]


def filt_by_common_id(df: pd.DataFrame, common_id, is_not_contain=False):
    assert "common_user_id" in df
    if isinstance(common_id, list):
        for id in common_id:
            assert isinstance(id, str)
    else:
        assert isinstance(common_id, str)

    if isinstance(common_id, str):
        selector = df["common_user_id"] == common_id
    elif isinstance(common_id, list):
        selector = df["common_user_id"].isin(common_id)

    if is_not_contain:
        selector = ~selector
    return df[selector]
